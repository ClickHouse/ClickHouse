#include "config.h"

#if USE_JEMALLOC

#include <gtest/gtest.h>

#include <Common/Jemalloc.h>

using namespace DB;

/// Regression test for https://github.com/ClickHouse/ClickHouse/issues/102183.
/// On builds without `JEMALLOC_BACKGROUND_THREAD` (notably macOS),
/// `je_mallctl` for `background_thread` / `max_background_threads` returns
/// `ENOENT`. The fix in `Jemalloc::verifySetup` relies on `tryGetValue` to
/// detect that case and skip the comparison instead of reading uninitialized
/// memory.

/// `tryGetValue` must report failure for an unknown mallctl name and must not
/// touch the caller-provided output. We use a name that is guaranteed to be
/// absent in any jemalloc build, which exercises the same `ENOENT` path that
/// `background_thread` hits on Darwin.
TEST(Jemalloc, TryGetValueReportsFailureForUnknownMallctl)
{
    bool out = true;
    EXPECT_FALSE(Jemalloc::tryGetValue("this.mallctl.does.not.exist", out));
    EXPECT_TRUE(out);
}

/// `verifySetup` must not abort or warn when the only potential mismatch
/// comes from optional `background_thread` / `max_background_threads`
/// mallctls that are not compiled into the running jemalloc -- the macOS
/// scenario from #102183. Before the fix this aborted in debug builds and
/// emitted a spurious `Jemalloc settings mismatch` warning otherwise.
TEST(Jemalloc, VerifySetupNoAbortWhenBackgroundThreadMallctlMissing)
{
    bool current_background_thread = false;
    const bool background_thread_supported
        = Jemalloc::tryGetValue("background_thread", current_background_thread);

    /// Pass values that make every other check fall through: match the
    /// current global profiler state (or the default when `prof.*` is
    /// absent on builds without `JEMALLOC_PROF`), leave
    /// `collect_global_profile_samples` at its default, set
    /// `max_background_threads_num=0` to skip that check, and use
    /// `default_profiler_sampling_rate` to skip the `prof.lg_sample`
    /// check. Reading `prof.thread_active_init` via the strict
    /// `MibCache::getValue` would itself assert on builds without prof,
    /// which is exactly the scenario this test is meant to tolerate.
    bool current_thread_active_init = false;
    const bool enable_global_profiler
        = Jemalloc::getThreadProfileInitMib().tryGetValue(current_thread_active_init)
            ? current_thread_active_init
            : Jemalloc::default_enable_global_profiler;
    const bool enable_background_threads = background_thread_supported
        ? current_background_thread
        : Jemalloc::default_enable_background_threads;

    Jemalloc::verifySetup(
        enable_global_profiler,
        enable_background_threads,
        /*max_background_threads_num=*/0,
        /*collect_global_profile_samples_in_trace_log=*/false,
        Jemalloc::default_profiler_sampling_rate);
}

#endif
