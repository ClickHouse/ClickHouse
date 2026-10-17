#pragma once

/// Statistics output and the stats interval event (jemalloc: `stats.h`, `src/stats.c`).
///
/// The printer (`statsPrint`, jemalloc's `stats_print` with the `emitter`) is in Stats.cpp.
/// The boot, the interval event and the buffered entry point (`mallocStatsPrint`) are in StatsFrontend.cpp.

#include <allocator/Common.h>
#include <allocator/Format.h>

#include <cstdint>

namespace jemalloc
{

class ThreadState;

/// jemalloc: STATS_INTERVAL_ACCUM_LG_BATCH_SIZE, STATS_INTERVAL_ACCUM_BATCH_MAX
inline constexpr unsigned STATS_INTERVAL_ACCUMULATED_LOG2_BATCH_SIZE = 6;
inline constexpr uint64_t STATS_INTERVAL_ACCUMULATED_BATCH_MAX = 4 << 20;

/// Print the statistics through `write_callback` (null: the message callback), unbuffered.
/// jemalloc: stats_print
void statsPrint(WriteCallback * write_callback, void * callback_argument, const char * options_string);

/// Print the statistics through a 64 KiB internal buffer (allocated in arena 0 as internal metadata).
/// jemalloc: je_malloc_stats_print (the body)
void mallocStatsPrint(WriteCallback * write_callback, void * callback_argument, const char * options_string);

/// Returns true on error. jemalloc: stats_boot
bool statsBoot();

/// jemalloc: stats_prefork, stats_postfork_parent, stats_postfork_child
void statsPrefork(ThreadState * thread_state);
void statsPostforkParent(ThreadState * thread_state);
void statsPostforkChild(ThreadState * thread_state);

}
