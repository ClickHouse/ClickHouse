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
using BitmapState32 = RoaringBitmapWithSmallSet<UInt32, 32>;
using BitmapState64 = RoaringBitmapWithSmallSet<UInt64, 32>;

template <typename State, typename Value>
void fillBitmapState(State & state, Value start, Value count)
{
    for (Value i = start; i < start + count; ++i)
        state.add(static_cast<Value>(i));
}
}

/// 32-bit (roaring::Roaring): Multiple threads call write() on the same COW-enabled state.
/// Exercises CRoaring copy-constructor source-mutation that marks containers SHARED_CONTAINER_TYPE.
TEST(BitmapCOWThreadSafety, ConcurrentWrite32)
{
    BitmapState32 state;
    fillBitmapState<BitmapState32, UInt32>(state, 0, 500'000);

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

/// 64-bit (roaring::Roaring64Map): Multiple threads call write() on the same COW-enabled state.
/// Roaring64Map uses std::map<uint32_t, Roaring> internally — its copy path iterates the map
/// and copies each inner Roaring, which has different thread-safety characteristics.
TEST(BitmapCOWThreadSafety, ConcurrentWrite64)
{
    BitmapState64 state;
    fillBitmapState<BitmapState64, UInt64>(state, 0, 500'000);

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

/// 32-bit: Multiple threads call merge() using the same COW-enabled state as the RHS.
/// CRoaring's or_inplace can mutate the source when borrowing containers from a COW RHS.
TEST(BitmapCOWThreadSafety, ConcurrentMergeFromSameSource32)
{
    BitmapState32 source;
    fillBitmapState<BitmapState32, UInt32>(source, 0, 500'000);

    constexpr int num_threads = 8;

    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([&source]()
        {
            BitmapState32 local;
            fillBitmapState<BitmapState32, UInt32>(local, 500'000, 100);
            local.merge(source);
            ASSERT_EQ(local.size(), 500'100);
        });
    }

    for (auto & t : threads)
        t.join();
}

/// 64-bit: Multiple threads call merge() using the same COW-enabled Roaring64Map state as the RHS.
TEST(BitmapCOWThreadSafety, ConcurrentMergeFromSameSource64)
{
    BitmapState64 source;
    fillBitmapState<BitmapState64, UInt64>(source, 0, 500'000);

    constexpr int num_threads = 8;

    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([&source]()
        {
            BitmapState64 local;
            fillBitmapState<BitmapState64, UInt64>(local, 500'000, 100);
            local.merge(source);
            ASSERT_EQ(local.size(), 500'100);
        });
    }

    for (auto & t : threads)
        t.join();
}

/// 32-bit: Concurrent write + merge from the same state simultaneously.
TEST(BitmapCOWThreadSafety, ConcurrentWriteAndMerge32)
{
    BitmapState32 state;
    fillBitmapState<BitmapState32, UInt32>(state, 0, 500'000);

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
            BitmapState32 local;
            local.merge(state);
            ASSERT_EQ(local.size(), 500'000);
        });
    }

    for (auto & t : threads)
        t.join();
}

/// 64-bit: Concurrent write + merge from the same Roaring64Map state simultaneously.
TEST(BitmapCOWThreadSafety, ConcurrentWriteAndMerge64)
{
    BitmapState64 state;
    fillBitmapState<BitmapState64, UInt64>(state, 0, 500'000);

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
            BitmapState64 local;
            local.merge(state);
            ASSERT_EQ(local.size(), 500'000);
        });
    }

    for (auto & t : threads)
        t.join();
}

/// 32-bit: Verify serialization round-trip correctness after concurrent writes.
TEST(BitmapCOWThreadSafety, RoundTripAfterConcurrentWrite32)
{
    BitmapState32 state;
    fillBitmapState<BitmapState32, UInt32>(state, 0, 100'000);

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

    BitmapState32 deserialized;
    ReadBufferFromString read_buf(serialized.str());
    deserialized.read(read_buf);

    ASSERT_EQ(deserialized.size(), 100'000);
}

/// 64-bit: Verify serialization round-trip correctness after concurrent writes.
TEST(BitmapCOWThreadSafety, RoundTripAfterConcurrentWrite64)
{
    BitmapState64 state;
    fillBitmapState<BitmapState64, UInt64>(state, 0, 100'000);

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

    BitmapState64 deserialized;
    ReadBufferFromString read_buf(serialized.str());
    deserialized.read(read_buf);

    ASSERT_EQ(deserialized.size(), 100'000);
}
