#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/PerCPU.h>
#include <Common/ProfileEvents.h>
#include <Common/ProfileEventsScope.h>
#include <Common/SharedMutex.h>
#include <Common/ThreadStatus.h>

#include <benchmark/benchmark.h>

namespace ProfileEvents
{
extern const Event CompressedWriteBufferCompressMicroseconds;
extern const Event PartsLocks;
extern const Event SharedPartsLocks;
}

namespace
{

/// Exercise reservations through the same four levels as query execution, with CPU sharding
/// enabled for both user and global counters. The benchmark process uses the default enabled mode.
struct ReservationChain
{
    ProfileEvents::Counters user{VariableContext::User, &ProfileEvents::global_counters};
    ProfileEvents::Counters process{VariableContext::Process, &user};
    ProfileEvents::Counters thread{VariableContext::Thread, &process};
    DB::ProfileEventsScope scope{&thread};

    ReservationChain()
    {
        /// Attaching a scope normally points it at the current worker; use our explicit hierarchy.
        thread.setParent(&process);
    }
};

void reportMetadata(benchmark::State & state)
{
    state.counters["cpu_rows"] = PerCPU::getNumCPUs();
    state.counters["levels"] = 4;
    state.SetItemsProcessed(state.iterations());
}

void BM_ProfileEventsWarmCompressionTimer(benchmark::State & state)
{
    DB::MainThreadStatus::getInstance();
    ReservationChain chain;
    const auto event = ProfileEvents::CompressedWriteBufferCompressMicroseconds;
    {
        DB::ProfileEventTimeIncrement<DB::Microseconds> timer(event);
    }
    /// Includes construction, clocks, reservation and cleanup, but no compression work.
    for (auto _ [[maybe_unused]] : state)
    {
        DB::ProfileEventTimeIncrement<DB::Microseconds> timer(event);
        benchmark::ClobberMemory();
    }
    if (chain.thread[event] != chain.process[event] || chain.thread[event] != chain.user[event])
        state.SkipWithError("timer publication did not reach every parent");
    reportMetadata(state);
}

void BM_ProfileEventsWarmPartsLock(benchmark::State & state)
{
    DB::MainThreadStatus::getInstance();
    ReservationChain chain;
    DB::SharedMutex mutex;
    {
        DB::DataPartsLock lock(mutex, nullptr);
    }
    /// Real uncontended exclusive lock and its three reservations; no table/list cleanup.
    for (auto _ [[maybe_unused]] : state)
    {
        DB::DataPartsLock lock(mutex, nullptr);
        benchmark::ClobberMemory();
    }
    const auto expected = static_cast<ProfileEvents::Count>(state.iterations()) + 1;
    if (chain.thread[ProfileEvents::PartsLocks] != expected || chain.process[ProfileEvents::PartsLocks] != expected
        || chain.user[ProfileEvents::PartsLocks] != expected)
        state.SkipWithError("parts-lock publication total differs from completed operations");
    reportMetadata(state);
}

void BM_ProfileEventsWarmSharedPartsLock(benchmark::State & state)
{
    DB::MainThreadStatus::getInstance();
    ReservationChain chain;
    DB::SharedMutex mutex;
    {
        DB::DataPartsSharedLock lock(mutex);
    }
    for (auto _ [[maybe_unused]] : state)
    {
        DB::DataPartsSharedLock lock(mutex);
        benchmark::ClobberMemory();
    }
    const auto expected = static_cast<ProfileEvents::Count>(state.iterations()) + 1;
    if (chain.thread[ProfileEvents::SharedPartsLocks] != expected || chain.process[ProfileEvents::SharedPartsLocks] != expected
        || chain.user[ProfileEvents::SharedPartsLocks] != expected)
        state.SkipWithError("shared parts-lock publication total differs from completed operations");
    reportMetadata(state);
}

}

BENCHMARK(BM_ProfileEventsWarmCompressionTimer)->Unit(benchmark::kNanosecond)->MinTime(0.5);
BENCHMARK(BM_ProfileEventsWarmPartsLock)->Unit(benchmark::kNanosecond)->MinTime(0.5);
BENCHMARK(BM_ProfileEventsWarmSharedPartsLock)->Unit(benchmark::kNanosecond)->MinTime(0.5);

BENCHMARK_MAIN();
