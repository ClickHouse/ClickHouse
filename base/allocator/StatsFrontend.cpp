#include <allocator/Stats.h>

#include <allocator/BufferedWriter.h>
#include <allocator/Frontend.h>
#include <allocator/Options.h>
#include <allocator/ThreadEvent.h>
#include <allocator/ThreadState.h>

/// The parts of jemalloc's `stats.c` that are not the printer: the stats interval event and its boot, and the buffered
/// entry point of `je_malloc_stats_print`.

namespace jemalloc
{

/// The wait of the stats interval event. jemalloc: stats_interval_accum_batch
constinit uint64_t stats_interval_accumulated_batch = 0;

namespace
{

/// jemalloc: stats_interval_accumulated (static)
constinit CounterAccumulated stats_interval_accumulated;

/// The internal buffer of `mallocStatsPrint`: an internal allocation in arena 0.
/// jemalloc: buf_writer_init / buf_writer_terminate
void * statsBufferAllocate(ThreadState * thread_state, size_t size)
{
    return internalAllocateFull(
        thread_state, size, size_classes::sizeToIndex(size), false, nullptr, true, arenaGet(thread_state, 0, false), true);
}

void statsBufferDeallocate(ThreadState * thread_state, void * ptr)
{
    internalDeallocateFull(thread_state, ptr, nullptr, nullptr, true, true);
}

constexpr BufferAllocator stats_buffer_allocator{&statsBufferAllocate, &statsBufferDeallocate};

/// jemalloc: STATS_PRINT_BUFSIZE
constexpr size_t STATS_PRINT_BUF_SIZE = 65536;

}

/// jemalloc: je_malloc_stats_print
void mallocStatsPrint(WriteCallback * write_callback, void * callback_argument, const char * options_string)
{
    ThreadState * thread_state = ThreadState::threadStateFetch();

    /// jemalloc prints unbuffered with `config_debug`, which is never enabled in ClickHouse.
    BufferedWriter buf_writer;
    buf_writer.init(thread_state, write_callback, callback_argument, nullptr, STATS_PRINT_BUF_SIZE, &stats_buffer_allocator);
    statsPrint(&BufferedWriter::callback, &buf_writer, options_string);
    buf_writer.terminate(thread_state);
}

/// jemalloc: stats_interval_new_event_wait
uint64_t statsIntervalNewEventWait(ThreadState & /*tsd*/)
{
    return stats_interval_accumulated_batch;
}

/// jemalloc: stats_interval_postponed_event_wait
uint64_t statsIntervalPostponedEventWait(ThreadState & /*tsd*/)
{
    return THREAD_EVENT_MIN_START_WAIT;
}

/// jemalloc: stats_interval_event_handler
void statsIntervalEvent(ThreadState & thread_state)
{
    uint64_t last_event = threadAllocatedLastEventGet(thread_state);
    uint64_t last_sample_event = statsIntervalLastEventGet(thread_state);
    statsIntervalLastEventSet(thread_state, last_event);
    uint64_t elapsed = last_event - last_sample_event;

    ALLOCATOR_ASSERT(elapsed > 0 && elapsed != THREAD_EVENT_INVALID_ELAPSED);
    if (stats_interval_accumulated.accumulate(&thread_state, elapsed))
        mallocStatsPrint(nullptr, nullptr, options.stats_interval_options);
}

/// jemalloc: stats_boot
bool statsBoot()
{
    uint64_t stats_interval;
    if (options.stats_interval < 0)
    {
        ALLOCATOR_ASSERT(options.stats_interval == -1);
        stats_interval = 0;
        stats_interval_accumulated_batch = 0;
    }
    else
    {
        /// See comments in jemalloc's stats.h.
        stats_interval = (options.stats_interval > 0) ? uint64_t(options.stats_interval) : 1;
        uint64_t batch = stats_interval >> STATS_INTERVAL_ACCUMULATED_LOG2_BATCH_SIZE;
        if (batch > STATS_INTERVAL_ACCUMULATED_BATCH_MAX)
            batch = STATS_INTERVAL_ACCUMULATED_BATCH_MAX;
        else if (batch == 0)
            batch = 1;
        stats_interval_accumulated_batch = batch;
    }

    return stats_interval_accumulated.init(stats_interval);
}

/// jemalloc: stats_prefork
void statsPrefork(ThreadState * thread_state)
{
    stats_interval_accumulated.prefork(thread_state);
}

/// jemalloc: stats_postfork_parent
void statsPostforkParent(ThreadState * thread_state)
{
    stats_interval_accumulated.postforkParent(thread_state);
}

/// jemalloc: stats_postfork_child
void statsPostforkChild(ThreadState * thread_state)
{
    stats_interval_accumulated.postforkChild(thread_state);
}

}
