#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Common/ThreadPool.h>

#include <gtest/gtest.h>

#include <thread>
#include <vector>

using namespace DB;

namespace
{
using BitmapState = RoaringBitmapWithSmallSet<UInt32, 32>;

void fillBitmapState(BitmapState & state, UInt32 start, UInt32 count)
{
    for (UInt32 i = start; i < start + count; ++i)
        state.add(i);
}
}

/// Multiple threads call write() on the same COW-enabled bitmap state.
/// This directly exercises the CRoaring copy-constructor source-mutation path
/// that marks containers as SHARED_CONTAINER_TYPE.
TEST(BitmapCOWThreadSafety, ConcurrentWrite)
{
    BitmapState state;
    fillBitmapState(state, 0, 500'000);

    constexpr int num_threads = 8;
    constexpr int iterations_per_thread = 20;

    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([&state]()
        {
            for (int i = 0; i < iterations_per_thread; ++i)
            {
                WriteBufferFromOwnString buf;
                state.write(buf);
                ASSERT_GT(buf.str().size(), 0);
            }
        });
    }

    for (auto & t : threads)
        t.join();
}

/// Multiple threads call merge() using the same COW-enabled state as the RHS.
/// CRoaring's or_inplace can mutate the source when borrowing containers from a COW RHS.
TEST(BitmapCOWThreadSafety, ConcurrentMergeFromSameSource)
{
    BitmapState source;
    fillBitmapState(source, 0, 500'000);

    constexpr int num_threads = 8;

    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([&source]()
        {
            BitmapState local;
            fillBitmapState(local, 500'000, 100);
            local.merge(source);
            ASSERT_EQ(local.size(), 500'100);
        });
    }

    for (auto & t : threads)
        t.join();
}

/// Concurrent write + merge from the same state simultaneously.
TEST(BitmapCOWThreadSafety, ConcurrentWriteAndMerge)
{
    BitmapState state;
    fillBitmapState(state, 0, 500'000);

    constexpr int num_threads = 4;

    std::vector<std::thread> threads;
    threads.reserve(num_threads * 2);

    for (int t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([&state]()
        {
            for (int i = 0; i < 10; ++i)
            {
                WriteBufferFromOwnString buf;
                state.write(buf);
            }
        });

        threads.emplace_back([&state]()
        {
            BitmapState local;
            local.merge(state);
            ASSERT_EQ(local.size(), 500'000);
        });
    }

    for (auto & t : threads)
        t.join();
}

/// Verify serialization round-trip correctness after concurrent writes.
TEST(BitmapCOWThreadSafety, RoundTripAfterConcurrentWrite)
{
    BitmapState state;
    fillBitmapState(state, 0, 100'000);

    constexpr int num_threads = 4;
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([&state]()
        {
            WriteBufferFromOwnString buf;
            state.write(buf);
        });
    }

    for (auto & t : threads)
        t.join();

    WriteBufferFromOwnString serialized;
    state.write(serialized);

    BitmapState deserialized;
    ReadBufferFromString read_buf(serialized.str());
    deserialized.read(read_buf);

    ASSERT_EQ(deserialized.size(), 100'000);
}
