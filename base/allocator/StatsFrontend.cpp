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
constinit uint64_t stats_interval_accum_batch = 0;

namespace
{

/// jemalloc: stats_interval_accumulated (static)
constinit CounterAccum stats_interval_accumulated;

/// The internal buffer of `mallocStatsPrint`: an internal allocation in arena 0.
/// jemalloc: buf_writer_init / buf_writer_terminate
void * statsBufferAllocate(ThreadState * tsdn, size_t size)
{
    return iallocztm(tsdn, size, sz::sizeToIndex(size), false, nullptr, true, arenaGet(tsdn, 0, false), true);
}

void statsBufferDeallocate(ThreadState * tsdn, void * ptr)
{
    idalloctm(tsdn, ptr, nullptr, nullptr, true, true);
}

constexpr BufferAllocator stats_buffer_allocator{&statsBufferAllocate, &statsBufferDeallocate};

/// jemalloc: STATS_PRINT_BUFSIZE
constexpr size_t STATS_PRINT_BUFSIZE = 65536;

}

/// jemalloc: je_malloc_stats_print
void mallocStatsPrint(WriteCallback * write_cb, void * cbopaque, const char * opts)
{
    ThreadState * tsdn = ThreadState::tsdnFetch();

    /// jemalloc prints unbuffered with `config_debug`, which is never enabled in ClickHouse.
    BufferedWriter buf_writer;
    buf_writer.init(tsdn, write_cb, cbopaque, nullptr, STATS_PRINT_BUFSIZE, &stats_buffer_allocator);
    statsPrint(&BufferedWriter::callback, &buf_writer, opts);
    buf_writer.terminate(tsdn);
}

/// jemalloc: stats_interval_new_event_wait
uint64_t statsIntervalNewEventWait(ThreadState & /*tsd*/)
{
    return stats_interval_accum_batch;
}

/// jemalloc: stats_interval_postponed_event_wait
uint64_t statsIntervalPostponedEventWait(ThreadState & /*tsd*/)
{
    return TE_MIN_START_WAIT;
}

/// jemalloc: stats_interval_event_handler
void statsIntervalEvent(ThreadState & tsd)
{
    uint64_t last_event = threadAllocatedLastEventGet(tsd);
    uint64_t last_sample_event = statsIntervalLastEventGet(tsd);
    statsIntervalLastEventSet(tsd, last_event);
    uint64_t elapsed = last_event - last_sample_event;

    JE_ASSERT(elapsed > 0 && elapsed != TE_INVALID_ELAPSED);
    if (stats_interval_accumulated.accum(&tsd, elapsed))
        mallocStatsPrint(nullptr, nullptr, opt.stats_interval_opts);
}

/// jemalloc: stats_boot
bool statsBoot()
{
    uint64_t stats_interval;
    if (opt.stats_interval < 0)
    {
        JE_ASSERT(opt.stats_interval == -1);
        stats_interval = 0;
        stats_interval_accum_batch = 0;
    }
    else
    {
        /// See comments in jemalloc's stats.h.
        stats_interval = (opt.stats_interval > 0) ? uint64_t(opt.stats_interval) : 1;
        uint64_t batch = stats_interval >> STATS_INTERVAL_ACCUM_LG_BATCH_SIZE;
        if (batch > STATS_INTERVAL_ACCUM_BATCH_MAX)
            batch = STATS_INTERVAL_ACCUM_BATCH_MAX;
        else if (batch == 0)
            batch = 1;
        stats_interval_accum_batch = batch;
    }

    return stats_interval_accumulated.init(stats_interval);
}

/// jemalloc: stats_prefork
void statsPrefork(ThreadState * tsdn)
{
    stats_interval_accumulated.prefork(tsdn);
}

/// jemalloc: stats_postfork_parent
void statsPostforkParent(ThreadState * tsdn)
{
    stats_interval_accumulated.postforkParent(tsdn);
}

/// jemalloc: stats_postfork_child
void statsPostforkChild(ThreadState * tsdn)
{
    stats_interval_accumulated.postforkChild(tsdn);
}

}
