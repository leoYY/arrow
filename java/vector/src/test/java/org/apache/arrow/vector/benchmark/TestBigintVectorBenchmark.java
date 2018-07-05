package org.apache.arrow.vector.benchmark;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class TestBigintVectorBenchmark {

    private static long[] smallLongArr = new long[1024];

    private static int[] readIndex = new int[1024];

    private static long[] midLongArr = new long[1024 * 1024];
    //private static long[] bigLongArr = new long[1024 * 1024 * 1024];

    private static BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);;
    private static BigIntVector smallVector;
    private static BigIntVector midVector;
    //private static BigIntVector bigVector;



    static {
        smallVector = new BigIntVector("small", allocator);
        smallVector.allocateNew(1024);
        midVector = new BigIntVector("mid", allocator);
        midVector.allocateNew(1024 * 1024);
        //bigVector = new BigIntVector("big", allocator);
        //bigVector.allocateNew(1024 * 1024 * 1024);
        Random r = new Random(System.currentTimeMillis());
        int l = midLongArr.length;
        for (int i = 0; i < l; ++i) {
            long rl = r.nextLong();
            if (i < smallLongArr.length) {
                int x = r.nextInt(1024);
                readIndex[i] = x > 0? x: -1 * x;
                smallLongArr[i] = rl;
                smallVector.set(i, rl);
            }
            if (i < midLongArr.length) {
                midLongArr[i] = rl;
                midVector.set(i, rl);
            }
            //bigLongArr[i] = rl;
            //bigVector.set(i, rl);

        }
    }

    @Benchmark
    public static void testSmallVectorRandomRead(Blackhole hole) {
        for (int i = 0; i < readIndex.length; ++i) {
            hole.consume(smallVector.get(readIndex[i]));
        }
    }

    @Benchmark
    public static void testSmallLongRandomRead(Blackhole hole) {
        for (int i = 0; i < readIndex.length; ++i) {
            hole.consume(smallLongArr[readIndex[i]]);
        }
    }

    @Benchmark
    public static void testMidVectorSequenceWrite(Blackhole hole) {
        int len = midVector.getValueCapacity();
        for (int i = 0; i < len; ++i) {
            midVector.set(i, i);
        }
        hole.consume(midVector);
    }

    @Benchmark
    public static void testMidLongSequanceWrite(Blackhole hole) {
        int len = midLongArr.length;
        for (int i = 0; i < len; ++i) {
            midLongArr[i] = i;
        }
        hole.consume(midLongArr);
    }

    @Benchmark
    public static void testMidVectorSequenceRead(Blackhole hole) {
        int len = midVector.getValueCapacity();
        for (int i = 0; i < len; ++i) {
            hole.consume(midVector.get(i));
        }
    }

    @Benchmark
    public static void testMidLongSequenceRead(Blackhole hole) {
        int len = midLongArr.length;
        for (int i = 0; i < len; ++i) {
            hole.consume(midLongArr[i]);
        }
    }

    @Benchmark
    public static void testSmallVectorSequenceRead(Blackhole hole) {
        int len = smallVector.getValueCapacity();
        for (int i = 0; i < len; ++i) {
            hole.consume(smallVector.get(i));
        }
    }

    @Benchmark
    public static void testSmallLongSequenceRead(Blackhole hole) {
        int len = smallLongArr.length;
        for (int i = 0; i < len; ++i) {
            hole.consume(smallLongArr[i]);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(TestBigintVectorBenchmark.class.getSimpleName())
                .forks(1)
                .warmupIterations(10)
                .measurementIterations(5)
                .build();
        new Runner(opt).run();
    }


}
