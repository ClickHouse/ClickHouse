/// Compares the background thread structures with jemalloc's (`background_thread_info_t`, `background_thread_stats_t`:
/// their sizes are observable through `stats.metadata`), the constants, and the interval computation of a work pass
/// (`backgroundWorkPass`) with a verbatim copy of jemalloc's `background_work_sleep_once` loop on random scripts:
/// the arenas worked on, the arenas queried and the resulting sleep interval must be identical.

#include <allocator/BackgroundThread.h>

#include "Test.h"

#include <cstddef>
#include <random>
#include <vector>

using namespace jemalloc;

extern "C"
{
struct RefScan
{
    const uint64_t * times;
    unsigned * worked;
    unsigned nworked;
    unsigned * queried;
    unsigned nqueried;
};

void ref_background_thread_layout(size_t out[16]);
uint64_t ref_background_work_sleep_ns(RefScan * scan, unsigned ind, unsigned narenas, size_t max_threads, bool slept_indefinitely);

/// The reference library pulls in the libunwind-based profiler backtrace, which is never called here.
int unw_backtrace(void **, int)
{
    return 0;
}
}

namespace
{

constexpr uint64_t MISSING = UINT64_MAX - 1;

struct Ops
{
    const std::vector<uint64_t> & times;
    std::vector<unsigned> worked;
    std::vector<unsigned> queried;

    const uint64_t * get(unsigned i) const { return times[i] == MISSING ? nullptr : &times[i]; }
    void doWork(const uint64_t * a) { worked.push_back(unsigned(a - times.data())); }
    uint64_t timeUntilDeferredWork(const uint64_t * a)
    {
        queried.push_back(unsigned(a - times.data()));
        return *a;
    }
};

}

TEST(BackgroundThreadOracle, Layout)
{
    size_t ref[16];
    ref_background_thread_layout(ref);
    CHECK_EQ(sizeof(BackgroundThreadInfo), ref[0]);
    CHECK_EQ(offsetof(BackgroundThreadInfo, thread), ref[1]);
    CHECK_EQ(offsetof(BackgroundThreadInfo, cond), ref[2]);
    CHECK_EQ(offsetof(BackgroundThreadInfo, mtx), ref[3]);
    CHECK_EQ(offsetof(BackgroundThreadInfo, state), ref[4]);
    CHECK_EQ(offsetof(BackgroundThreadInfo, indefinite_sleep), ref[5]);
    CHECK_EQ(offsetof(BackgroundThreadInfo, next_wakeup), ref[6]);
    CHECK_EQ(offsetof(BackgroundThreadInfo, npages_to_purge_new), ref[7]);
    CHECK_EQ(offsetof(BackgroundThreadInfo, tot_n_runs), ref[8]);
    CHECK_EQ(offsetof(BackgroundThreadInfo, tot_sleep_time), ref[9]);
    CHECK_EQ(sizeof(BackgroundThreadStats), ref[10]);
    CHECK_EQ(offsetof(BackgroundThreadStats, max_counter_per_bg_thd), ref[11]);
    CHECK_EQ(BACKGROUND_THREAD_MIN_INTERVAL_NS, uint64_t(ref[12]));
    CHECK_EQ(DEFAULT_NUM_BACKGROUND_THREAD, ref[13]);
    CHECK_EQ(MAX_BACKGROUND_THREAD_LIMIT, ref[14]);
    CHECK_EQ(size_t(BackgroundThreadState::Paused), ref[15]);
}

TEST(BackgroundThreadOracle, WorkPass)
{
    std::mt19937_64 rng(12345);
    /// Times around the interesting boundaries: 0, the minimal interval, the deferred maximum.
    const uint64_t special[] = {0, 1, BACKGROUND_THREAD_MIN_INTERVAL_NS - 1, BACKGROUND_THREAD_MIN_INTERVAL_NS,
                                BACKGROUND_THREAD_MIN_INTERVAL_NS + 1, BACKGROUND_THREAD_DEFERRED_MAX, MISSING};
    size_t ncases = 0;
    for (int iteration = 0; iteration < 200000; ++iteration)
    {
        unsigned narenas = unsigned(rng() % 40);
        size_t max_threads = 1 + rng() % 6;
        unsigned ind = unsigned(rng() % max_threads);
        bool slept_indefinitely = rng() % 2;
        std::vector<uint64_t> times(narenas);
        for (auto & t : times)
        {
            switch (rng() % 4)
            {
                case 0:
                    t = special[rng() % std::size(special)];
                    break;
                case 1:
                    t = rng() % (3 * BACKGROUND_THREAD_MIN_INTERVAL_NS);
                    break;
                case 2:
                    t = rng() % (100 * BACKGROUND_THREAD_MIN_INTERVAL_NS);
                    break;
                default:
                    t = (rng() % 8 == 0) ? BACKGROUND_THREAD_DEFERRED_MAX : rng() % UINT64_MAX;
                    break;
            }
        }

        Ops ops{times, {}, {}};
        uint64_t got = backgroundWorkPass(ind, narenas, max_threads, slept_indefinitely, ops);

        std::vector<unsigned> worked(narenas + 1);
        std::vector<unsigned> queried(narenas + 1);
        RefScan scan{times.data(), worked.data(), 0, queried.data(), 0};
        uint64_t expected = ref_background_work_sleep_ns(&scan, ind, narenas, max_threads, slept_indefinitely);
        worked.resize(scan.nworked);
        queried.resize(scan.nqueried);

        CHECK_EQ(got, expected);
        CHECK(ops.worked == worked);
        CHECK(ops.queried == queried);
        ++ncases;
    }
    CHECK_EQ(ncases, size_t(200000));
}
