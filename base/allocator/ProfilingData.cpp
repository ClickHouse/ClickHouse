/// The core profiling data structures (jemalloc: `prof_data.c`).
///
/// Conceptually, profiling data can be imagined as a table with three columns: thread, stack trace, and current
/// allocation size (with `profiling_accumulated` there's one additional column which is the cumulative allocation size).
///
/// Implementation wise, each thread maintains a hash recording the stack trace to allocation size correspondences,
/// which are basically the individual rows in the table. In addition, two global "indices" are built to make data
/// aggregation efficient (for dumping): `backtrace_to_global_context` and `all_thread_data`, which are basically the "grouped by stack
/// trace" and
/// "grouped by thread" views of the same table, respectively. Note that the allocation size is only aggregated to the
/// two indices at dumping time, so as to optimize for performance.

#include <allocator/Profiling.h>

#include <allocator/Arenas.h>
#include <allocator/Bin.h>
#include <allocator/Format.h>
#include <allocator/Frontend.h>
#include <allocator/Hash.h>
#include <allocator/Options.h>

#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstdarg>
#include <cstring>
#include <unistd.h>

namespace jemalloc
{

/// --- Data ----------------------------------------------------------------------------------------------------------

constinit Mutex backtrace_to_global_context_mutex;
constinit Mutex all_thread_data_mutex;
constinit Mutex profiling_dump_mutex;

/// Table of mutexes that are shared among gctx's. These are leaf locks, so there is no problem with using them for
/// more than one gctx at the same time. The primary motivation for this sharing though is that gctx's are ephemeral,
/// and destroying mutexes causes complications for systems that allocate when creating/destroying mutexes.
constinit Mutex * global_context_locks = nullptr;

/// Table of mutexes that are shared among tdata's. No operations require holding multiple tdata locks, so there is no
/// problem with using them for more than one tdata at the same time, even though a gctx lock may be acquired while
/// holding a tdata lock.
constinit Mutex * thread_data_locks = nullptr;

constinit size_t profiling_unbiased_size[SIZE_CLASS_NUM_SIZES] = {};
constinit size_t profiling_shifted_unbiased_count[SIZE_CLASS_NUM_SIZES] = {};

namespace
{

/// Atomic counter. jemalloc: cum_gctxs
constinit std::atomic<unsigned> cumulative_global_contexts{0};

/// Global hash of (ProfilingBacktrace *) -> (ProfilingGlobalContext *). This is the master data structure that knows about all
/// backtraces currently captured. jemalloc: bt2gctx
constinit ProfilingCuckooHash backtrace_to_global_context{};

/// Tree of all extant ProfilingThreadData structures, regardless of state, {attached,detached,expired}. jemalloc: tdatas
constinit ProfilingThreadDataTree all_thread_data;

}

/// --- Comparators ---------------------------------------------------------------------------------------------------

/// jemalloc: prof_tctx_comp
int profilingThreadContextCompare(const ProfilingThreadContext * a, const ProfilingThreadContext * b)
{
    uint64_t a_thread_uid = a->thread_uid;
    uint64_t b_thread_uid = b->thread_uid;
    int result = (a_thread_uid > b_thread_uid) - (a_thread_uid < b_thread_uid);
    if (result == 0)
    {
        uint64_t a_thread_discriminator = a->thread_discriminator;
        uint64_t b_thread_discriminator = b->thread_discriminator;
        result = (a_thread_discriminator > b_thread_discriminator) - (a_thread_discriminator < b_thread_discriminator);
        if (result == 0)
        {
            uint64_t a_thread_context_uid = a->thread_context_uid;
            uint64_t b_thread_context_uid = b->thread_context_uid;
            result = (a_thread_context_uid > b_thread_context_uid) - (a_thread_context_uid < b_thread_context_uid);
        }
    }
    return result;
}

/// Note: `memcmp` on the raw bytes of the program counters (on little-endian machines this is not the numeric
/// order of the addresses); the order of the `@` blocks in heap dumps depends on it.
/// jemalloc: prof_gctx_comp
int profilingGlobalContextCompare(const ProfilingGlobalContext * a, const ProfilingGlobalContext * b)
{
    unsigned a_len = a->backtrace.len;
    unsigned b_len = b->backtrace.len;
    unsigned compare_len = (a_len < b_len) ? a_len : b_len;
    int result = memcmp(a->backtrace.vector, b->backtrace.vector, compare_len * sizeof(void *));
    if (result == 0)
        result = (a_len > b_len) - (a_len < b_len);
    return result;
}

/// jemalloc: prof_tdata_comp
int profilingThreadDataCompare(const ProfilingThreadData * a, const ProfilingThreadData * b)
{
    uint64_t a_uid = a->thread_uid;
    uint64_t b_uid = b->thread_uid;
    int result = ((a_uid > b_uid) - (a_uid < b_uid));
    if (result == 0)
    {
        uint64_t a_discriminator = a->thread_discriminator;
        uint64_t b_discriminator = b->thread_discriminator;
        result = ((a_discriminator > b_discriminator) - (a_discriminator < b_discriminator));
    }
    return result;
}

/// --- Internal allocations ------------------------------------------------------------------------------------------

void * profilingAllocArena0(ThreadState & thread_state, size_t size, bool init_if_missing)
{
    return internalAllocateFull(
        &thread_state,
        size,
        size_classes::sizeToIndex(size),
        false,
        nullptr,
        true,
        arenaGet(init_if_missing ? nullptr : &thread_state, 0, init_if_missing),
        true);
}

void * profilingAllocInternalArena(ThreadState & thread_state, size_t size)
{
    return internalAllocateFull(
        &thread_state, size, size_classes::sizeToIndex(size), false, nullptr, true, arenaChooseInternal(thread_state, nullptr), true);
}

void profilingInternalDeallocate(ThreadState * thread_state, void * ptr)
{
    internalDeallocateFull(thread_state, ptr, nullptr, nullptr, true, true);
}

void * ProfilingCuckooHashAllocator::allocate(ThreadState & thread_state, size_t usable_size, size_t alignment)
{
    return internalAllocateAlignedFull(
        &thread_state, usable_size, alignment, true, nullptr, true, arenaChooseInternal(thread_state, nullptr));
}

void ProfilingCuckooHashAllocator::deallocate(ThreadState & thread_state, void * ptr)
{
    internalDeallocateFull(&thread_state, ptr, nullptr, nullptr, true, true);
}

/// --- Global contexts -----------------------------------------------------------------------------------------------

namespace
{

/// NB: `fetch_add` returns the old value, so the first gctx gets the lock 1023, then 0, 1, ...
/// jemalloc: prof_gctx_mutex_choose
Mutex * profilingGlobalContextMutexChoose()
{
    unsigned num_global_contexts = cumulative_global_contexts.fetch_add(1, std::memory_order_relaxed);
    return &global_context_locks[(num_global_contexts - 1) % PROFILING_NUM_CONTEXT_LOCKS];
}

/// jemalloc: prof_tdata_mutex_choose
Mutex * profilingThreadDataMutexChoose(uint64_t thread_uid)
{
    return &thread_data_locks[thread_uid % PROFILING_NUM_THREAD_DATA_LOCKS];
}

/// jemalloc: prof_enter
void profilingEnter(ThreadState & thread_state, ProfilingThreadData * thread_data)
{
    ALLOCATOR_ASSERT(thread_data == profilingThreadDataGet(thread_state, false));

    if (thread_data != nullptr)
    {
        ALLOCATOR_ASSERT(!thread_data->enqueued);
        thread_data->enqueued = true;
    }

    backtrace_to_global_context_mutex.lock(&thread_state);
}

/// jemalloc: prof_leave
void profilingLeave(ThreadState & thread_state, ProfilingThreadData * thread_data)
{
    ALLOCATOR_ASSERT(thread_data == profilingThreadDataGet(thread_state, false));

    backtrace_to_global_context_mutex.unlock(&thread_state);

    if (thread_data != nullptr)
    {
        ALLOCATOR_ASSERT(thread_data->enqueued);
        thread_data->enqueued = false;
        bool interval_dump = thread_data->enqueued_interval_dump;
        thread_data->enqueued_interval_dump = false;
        bool growth_dump = thread_data->enqueued_growth_dump;
        thread_data->enqueued_growth_dump = false;

        if (interval_dump)
            profilingIntervalDump(&thread_state);
        if (growth_dump)
            profilingGrowthDump(&thread_state);
    }
}

/// jemalloc: prof_gctx_create
ProfilingGlobalContext * profilingGlobalContextCreate(ThreadState & thread_state, ProfilingBacktrace * backtrace)
{
    /// Create a single allocation that has space for vec of length bt->len.
    size_t size = offsetof(ProfilingGlobalContext, vector) + (backtrace->len * sizeof(void *));
    auto * global_context = static_cast<ProfilingGlobalContext *>(profilingAllocArena0(thread_state, size, true));
    if (global_context == nullptr)
        return nullptr;
    global_context->lock = profilingGlobalContextMutexChoose();
    /// Set nlimbo to 1, in order to avoid a race condition with `profilingThreadContextDestroy` / `profilingGlobalContextTryDestroy`.
    global_context->num_limbo = 1;
    global_context->thread_contexts.init();
    global_context->fragmentation_objects.init();
    /// Duplicate bt.
    memcpy(static_cast<void *>(global_context->vector), backtrace->vector, backtrace->len * sizeof(void *));
    global_context->backtrace.vector = global_context->vector;
    global_context->backtrace.len = backtrace->len;
    return global_context;
}

/// jemalloc: prof_gctx_try_destroy
void profilingGlobalContextTryDestroy(
    ThreadState & thread_state, ProfilingThreadData * thread_data_self, ProfilingGlobalContext * global_context)
{
    /// Check that gctx is still unused by any thread cache before destroying it. `profilingLookup` increments
    /// gctx->nlimbo in order to avoid a race condition with this function, as does `profilingThreadContextDestroy` in order to
    /// avoid a race between the main body of `profilingThreadContextDestroy` and entry into this function.
    profilingEnter(thread_state, thread_data_self);
    global_context->lock->lock(&thread_state);
    ALLOCATOR_ASSERT(global_context->num_limbo != 0);
    if (global_context->thread_contexts.empty() && global_context->num_limbo == 1)
    {
        /// No live tctx implies no live sampled allocation attributed to this gctx (each such allocation holds a
        /// curobjs count on its tctx until untracked).
        ALLOCATOR_ASSERT(global_context->fragmentation_objects.empty());
        /// Remove gctx from bt2gctx.
        if (backtrace_to_global_context.remove(thread_state, &global_context->backtrace, nullptr, nullptr))
            ALLOCATOR_NOT_REACHED();
        profilingLeave(thread_state, thread_data_self);
        /// Destroy gctx.
        global_context->lock->unlock(&thread_state);
        profilingInternalDeallocate(&thread_state, global_context);
    }
    else
    {
        /// Compensate for increment in `profilingThreadContextDestroy` or `profilingLookup`.
        --global_context->num_limbo;
        global_context->lock->unlock(&thread_state);
        profilingLeave(thread_state, thread_data_self);
    }
}

/// jemalloc: prof_gctx_should_destroy
bool profilingGlobalContextShouldDestroy(ProfilingGlobalContext * global_context)
{
    if (options.profiling_accumulated)
        return false;
    if (!global_context->thread_contexts.empty())
        return false;
    if (global_context->num_limbo != 0)
        return false;
    return true;
}

/// jemalloc: prof_lookup_global
bool profilingLookupGlobal(
    ThreadState & thread_state,
    ProfilingBacktrace * backtrace,
    ProfilingThreadData * thread_data,
    void ** backtrace_key_ptr,
    ProfilingGlobalContext ** global_context_ptr,
    bool * new_global_context_ptr)
{
    void * global_context_v = nullptr;
    void * backtrace_key_v = nullptr;
    ProfilingGlobalContext * created_global_context;
    bool new_global_context;

    profilingEnter(thread_state, thread_data);
    if (backtrace_to_global_context.search(backtrace, &backtrace_key_v, &global_context_v))
    {
        /// bt has never been seen before. Insert it.
        profilingLeave(thread_state, thread_data);
        created_global_context = profilingGlobalContextCreate(thread_state, backtrace);
        if (created_global_context == nullptr)
            return true;
        profilingEnter(thread_state, thread_data);
        if (backtrace_to_global_context.search(backtrace, &backtrace_key_v, &global_context_v))
        {
            global_context_v = created_global_context;
            backtrace_key_v = &created_global_context->backtrace;
            if (backtrace_to_global_context.insert(thread_state, backtrace_key_v, global_context_v))
            {
                /// OOM.
                profilingLeave(thread_state, thread_data);
                profilingInternalDeallocate(&thread_state, global_context_v);
                return true;
            }
            new_global_context = true;
        }
        else
        {
            new_global_context = false;
        }
    }
    else
    {
        created_global_context = nullptr;
        new_global_context = false;
    }

    auto * global_context = static_cast<ProfilingGlobalContext *>(global_context_v);
    if (!new_global_context)
    {
        /// Increment nlimbo, in order to avoid a race condition with `profilingThreadContextDestroy` / `profilingGlobalContextTryDestroy`.
        global_context->lock->lock(&thread_state);
        ++global_context->num_limbo;
        global_context->lock->unlock(&thread_state);
        new_global_context = false;

        if (created_global_context != nullptr)
        {
            /// Lost race to insert.
            profilingInternalDeallocate(&thread_state, created_global_context);
        }
    }
    profilingLeave(thread_state, thread_data);

    *backtrace_key_ptr = backtrace_key_v;
    *global_context_ptr = global_context;
    *new_global_context_ptr = new_global_context;
    return false;
}

}

/// jemalloc: prof_data_init
bool profilingDataInit(ThreadState & thread_state)
{
    all_thread_data.init();
    return backtrace_to_global_context.init(
        thread_state, PROFILING_CUCKOO_HASH_MIN_ITEMS, profilingBacktraceHash, profilingBacktraceKeyCompare);
}

/// Track/untrack a live sampled allocation on its gctx's `fragmentation_objects` list, so that heap dumps can enumerate live
/// sampled allocations (fragmentation profiling). Called with no locks held: track from `profilingMallocSampleObject`
/// while `thread_context->prepared` still pins the tctx; untrack from the `profilingInfoGetAndResetRecent` sever point, which on
/// every deallocation path precedes the curobjs decrement in `profilingFreeSampledObject` -- so in both cases the tctx
/// (hence gctx) is guaranteed alive.
/// jemalloc: prof_frag_track
void profilingFragmentationTrack(ThreadState & thread_state, Extent * extent, ProfilingThreadContext * thread_context)
{
    ProfilingGlobalContext * global_context = thread_context->global_context;

    MutexLock lock(&thread_state, *global_context->lock);
    ALLOCATOR_ASSERT(!extent->profilingFragmentationTracked());
    global_context->fragmentation_objects.append(extent);
    extent->setProfilingFragmentationTracked(true);
}

/// jemalloc: prof_frag_untrack
void profilingFragmentationUntrack(ThreadState & thread_state, Extent * extent, ProfilingThreadContext * thread_context)
{
    ProfilingGlobalContext * global_context = thread_context->global_context;

    MutexLock lock(&thread_state, *global_context->lock);
    /// Not necessarily tracked: if an in-place reallocation severed the allocation but then failed (OOM), the
    /// allocation stays live yet untracked, and its eventual deallocation severs it a second time.
    if (extent->profilingFragmentationTracked())
    {
        global_context->fragmentation_objects.remove(extent);
        extent->setProfilingFragmentationTracked(false);
    }
}

/// jemalloc: prof_lookup
ProfilingThreadContext * profilingLookup(ThreadState & thread_state, ProfilingBacktrace * backtrace)
{
    ProfilingThreadData * thread_data = profilingThreadDataGet(thread_state, false);
    ALLOCATOR_ASSERT(thread_data != nullptr);

    void * result_v = nullptr;
    thread_data->lock->lock(&thread_state);
    bool not_found = thread_data->backtrace_to_thread_context.search(backtrace, nullptr, &result_v);
    auto * result = static_cast<ProfilingThreadContext *>(result_v);
    if (!not_found) /// Note double negative!
        result->prepared = true;
    thread_data->lock->unlock(&thread_state);
    if (not_found)
    {
        void * backtrace_key;
        ProfilingGlobalContext * global_context;
        bool new_global_context;

        /// This thread's cache lacks bt. Look for it in the global cache.
        if (profilingLookupGlobal(thread_state, backtrace, thread_data, &backtrace_key, &global_context, &new_global_context))
            return nullptr;

        /// Link a ProfilingThreadContext into gctx for this thread.
        result = static_cast<ProfilingThreadContext *>(profilingAllocInternalArena(thread_state, sizeof(ProfilingThreadContext)));
        if (result == nullptr)
        {
            if (new_global_context)
                profilingGlobalContextTryDestroy(thread_state, thread_data, global_context);
            return nullptr;
        }
        result->thread_data = thread_data;
        result->thread_uid = thread_data->thread_uid;
        result->thread_discriminator = thread_data->thread_discriminator;
        result->recent_count = 0;
        memset(&result->counts, 0, sizeof(ProfilingCounters));
        result->global_context = global_context;
        result->thread_context_uid = thread_data->thread_context_uid_next++;
        result->prepared = true;
        result->state = profiling_thread_context_state_initializing;
        thread_data->lock->lock(&thread_state);
        bool error = thread_data->backtrace_to_thread_context.insert(thread_state, backtrace_key, result);
        thread_data->lock->unlock(&thread_state);
        if (error)
        {
            if (new_global_context)
                profilingGlobalContextTryDestroy(thread_state, thread_data, global_context);
            profilingInternalDeallocate(&thread_state, result);
            return nullptr;
        }
        global_context->lock->lock(&thread_state);
        result->state = profiling_thread_context_state_nominal;
        global_context->thread_contexts.insert(result);
        --global_context->num_limbo;
        global_context->lock->unlock(&thread_state);
    }

    return result;
}

/// Used in unit tests. jemalloc: prof_tdata_count
size_t profilingThreadDataCount()
{
    size_t thread_data_count = 0;
    ThreadState * thread_state = ThreadState::threadStateFetch();
    MutexLock lock(thread_state, all_thread_data_mutex);
    all_thread_data.iterate(
        nullptr,
        [&](ProfilingThreadData *) -> ProfilingThreadData *
        {
            ++thread_data_count;
            return nullptr;
        });
    return thread_data_count;
}

/// Used in unit tests. jemalloc: prof_bt_count
size_t profilingBacktraceCount()
{
    ThreadState & thread_state = ThreadState::fetch();
    ProfilingThreadData * thread_data = profilingThreadDataGet(thread_state, false);
    if (thread_data == nullptr)
        return 0;

    MutexLock lock(&thread_state, backtrace_to_global_context_mutex);
    return backtrace_to_global_context.count();
}

namespace
{

/// jemalloc: prof_thread_name_write_tdata
void profilingThreadNameWriteThreadData(ProfilingThreadData * thread_data, const char * thread_name)
{
    strncpy(thread_data->thread_name, thread_name, PROFILING_THREAD_NAME_MAX_LEN);
    thread_data->thread_name[PROFILING_THREAD_NAME_MAX_LEN - 1] = '\0';
}

}

/// jemalloc: prof_thread_name_set_impl
int profilingThreadNameSetImpl(ThreadState & thread_state, const char * thread_name)
{
    ALLOCATOR_ASSERT(thread_state.reentrancyLevel() == 0);
    ALLOCATOR_ASSERT(thread_name != nullptr);

    for (unsigned i = 0; thread_name[i] != '\0'; ++i)
    {
        char c = thread_name[i];
        if (!isgraph(c) && !isblank(c))
            return EINVAL;
    }

    ProfilingThreadData * thread_data = profilingThreadDataGet(thread_state, true);
    if (thread_data == nullptr)
        return ENOMEM;

    profilingThreadNameWriteThreadData(thread_data, thread_name);

    return 0;
}

/// --- Unbiasing -----------------------------------------------------------------------------------------------------

namespace
{

/// jemalloc: prof_dump_printf
ALLOCATOR_FORMAT_PRINTF(3, 4)
void profilingDumpPrintf(WriteCallback * profiling_dump_write, void * callback_argument, const char * format_string, ...)
{
    va_list args;
    char buf[PROFILING_PRINTF_BUF_SIZE];

    va_start(args, format_string);
    formatV(buf, sizeof(buf), format_string, args);
    va_end(args);
    profiling_dump_write(callback_argument, buf);
}

/// Casting a double to a uint64_t may not necessarily be in range; this can be UB. UINT64_MAX + 1 is exactly
/// representable as a double; writing this as !(a < b) instead of (a >= b) means that we're NaN-safe.
/// jemalloc: prof_double_uint64_cast
uint64_t profilingDoubleUint64Cast(double d)
{
    double rounded = round(d);
    if (!(rounded < static_cast<double>(UINT64_MAX)))
        return UINT64_MAX;
    return static_cast<uint64_t>(rounded);
}

}

/// jemalloc: prof_unbias_map_init
void profilingUnbiasMapInit()
{
    for (SizeClassIdx i = 0; i < SIZE_CLASS_NUM_SIZES; ++i)
    {
        /// With large size classes disabled, the unbiased calculation here is not as accurate as it was because usize
        /// now changes in a finer grain while the unbiased_sz is still calculated using the old way.
        double size = static_cast<double>(size_classes::indexToSizeUnsafe(i));
        double rate = static_cast<double>(size_t(1) << log2_profiling_sample);
        double division_value = 1.0 - exp(-size / rate);
        double unbiased_size = size / division_value;
        /// The "true" right value for the unbiased count is 1.0/(1 - exp(-sz/rate)). The counts are kept as integers;
        /// to limit the rounding error, they are multiplied by the size of the smallest allocation.
        double count_shift = static_cast<double>(size_t(1) << SIZE_CLASS_LOG2_TINY_MIN);
        double shifted_unbiased_count = count_shift / division_value;
        profiling_unbiased_size[i] = static_cast<size_t>(round(unbiased_size));
        profiling_shifted_unbiased_count[i] = static_cast<size_t>(round(shifted_unbiased_count));
    }
}

namespace
{

/// jeprof unbiases the count and aggregate size as
///     c_out = c_in * 1/(1-exp(-s_in/c_in/R)
///     s_out = s_in * 1/(1-exp(-s_in/c_in/R)
/// Here we solve for the values of c_in and s_in that give the c_out and s_out computed internally: with x = s_in /
/// c_in, y = s_in, x = s_out / c_out, and the other values fall out from that.
/// jemalloc: prof_do_unbias
void profilingDoUnbias(uint64_t c_out_shifted_i, uint64_t s_out_i, uint64_t * result_c_in, uint64_t * result_s_in)
{
    if (c_out_shifted_i == 0 || s_out_i == 0)
    {
        *result_c_in = 0;
        *result_s_in = 0;
        return;
    }
    /// c_out is taken in a shifted form (see `profilingUnbiasMapInit`).
    double c_out = static_cast<double>(c_out_shifted_i) / static_cast<double>(size_t(1) << SIZE_CLASS_LOG2_TINY_MIN);
    double s_out = static_cast<double>(s_out_i);
    double R = static_cast<double>(size_t(1) << log2_profiling_sample);

    double x = s_out / c_out;
    double y = s_out * (1.0 - exp(-x / R));

    double c_in = y / x;
    double s_in = y;

    *result_c_in = profilingDoubleUint64Cast(c_in);
    *result_s_in = profilingDoubleUint64Cast(s_in);
}

/// jemalloc: prof_dump_print_cnts
void profilingDumpPrintCounts(WriteCallback * profiling_dump_write, void * callback_argument, const ProfilingCounters * counts)
{
    uint64_t current_objects;
    uint64_t current_bytes;
    uint64_t accumulated_objects;
    uint64_t accumulated_bytes;
    if (options.profiling_unbias)
    {
        profilingDoUnbias(counts->current_objects_shifted_unbiased, counts->current_bytes_unbiased, &current_objects, &current_bytes);
        profilingDoUnbias(
            counts->accumulated_objects_shifted_unbiased, counts->accumulated_bytes_unbiased, &accumulated_objects, &accumulated_bytes);
    }
    else
    {
        current_objects = counts->current_objects;
        current_bytes = counts->current_bytes;
        accumulated_objects = counts->accumulated_objects;
        accumulated_bytes = counts->accumulated_bytes;
    }
    profilingDumpPrintf(
        profiling_dump_write,
        callback_argument,
        "%llu: %llu [%llu: %llu]",
        static_cast<unsigned long long>(current_objects),
        static_cast<unsigned long long>(current_bytes),
        static_cast<unsigned long long>(accumulated_objects),
        static_cast<unsigned long long>(accumulated_bytes));
}

/// --- Dump aggregation ------------------------------------------------------------------------------------------------

/// Adds the cur* counters of `src` to `dst` (and the accum* counters if `opt.prof_accum`).
void profilingCountsMerge(ProfilingCounters & dst, const ProfilingCounters & src)
{
    dst.current_objects += src.current_objects;
    dst.current_objects_shifted_unbiased += src.current_objects_shifted_unbiased;
    dst.current_bytes += src.current_bytes;
    dst.current_bytes_unbiased += src.current_bytes_unbiased;
    if (options.profiling_accumulated)
    {
        dst.accumulated_objects += src.accumulated_objects;
        dst.accumulated_objects_shifted_unbiased += src.accumulated_objects_shifted_unbiased;
        dst.accumulated_bytes += src.accumulated_bytes;
        dst.accumulated_bytes_unbiased += src.accumulated_bytes_unbiased;
    }
}

/// jemalloc: prof_tctx_merge_tdata
void profilingThreadContextMergeThreadData(
    ThreadState * thread_state, ProfilingThreadContext * thread_context, ProfilingThreadData * thread_data)
{
    thread_context->thread_data->lock->assertOwner(thread_state);

    thread_context->global_context->lock->lock(thread_state);

    switch (thread_context->state)
    {
        case profiling_thread_context_state_initializing: thread_context->global_context->lock->unlock(thread_state); return;
        case profiling_thread_context_state_nominal:
            thread_context->state = profiling_thread_context_state_dumping;
            thread_context->global_context->lock->unlock(thread_state);

            memcpy(&thread_context->dump_counts, &thread_context->counts, sizeof(ProfilingCounters));

            profilingCountsMerge(thread_data->count_summed, thread_context->dump_counts);
            break;
        case profiling_thread_context_state_dumping:
        case profiling_thread_context_state_purgatory: ALLOCATOR_NOT_REACHED();
    }
}

/// jemalloc: prof_tctx_merge_gctx
void profilingThreadContextMergeGlobalContext(
    ThreadState * thread_state, ProfilingThreadContext * thread_context, ProfilingGlobalContext * global_context)
{
    global_context->lock->assertOwner(thread_state);
    profilingCountsMerge(global_context->count_summed, thread_context->dump_counts);
}

/// jemalloc: prof_dump_iter_arg_t
struct ProfilingDumpIterateArg
{
    ThreadState * thread_state;
    WriteCallback * profiling_dump_write;
    void * callback_argument;
    /// Dump-time timestamp for computing live sampled allocation ages.
    Nanoseconds now;
};

/// jemalloc: prof_tctx_dump_iter
void profilingThreadContextDumpIterate(ProfilingDumpIterateArg * arg, ProfilingThreadContext * thread_context)
{
    thread_context->global_context->lock->assertOwner(arg->thread_state);

    switch (thread_context->state)
    {
        case profiling_thread_context_state_initializing:
        case profiling_thread_context_state_nominal:
            /// Not captured by this dump.
            break;
        case profiling_thread_context_state_dumping:
        case profiling_thread_context_state_purgatory:
            profilingDumpPrintf(
                arg->profiling_dump_write,
                arg->callback_argument,
                "  t%llu: ",
                static_cast<unsigned long long>(thread_context->thread_uid));
            profilingDumpPrintCounts(arg->profiling_dump_write, arg->callback_argument, &thread_context->dump_counts);
            arg->profiling_dump_write(arg->callback_argument, "\n");
            break;
    }
}

/// jemalloc: prof_tctx_finish_iter
ProfilingThreadContext * profilingThreadContextFinishIterate(ThreadState * thread_state, ProfilingThreadContext * thread_context)
{
    thread_context->global_context->lock->assertOwner(thread_state);

    switch (thread_context->state)
    {
        case profiling_thread_context_state_nominal:
            /// New since dumping started; ignore.
            break;
        case profiling_thread_context_state_dumping: thread_context->state = profiling_thread_context_state_nominal; break;
        case profiling_thread_context_state_purgatory: return thread_context;
        case profiling_thread_context_state_initializing: ALLOCATOR_NOT_REACHED();
    }
    return nullptr;
}

/// jemalloc: prof_dump_gctx_prep
void profilingDumpGlobalContextPrepare(
    ThreadState * thread_state, ProfilingGlobalContext * global_context, ProfilingGlobalContextTree * global_contexts)
{
    MutexLock lock(thread_state, *global_context->lock);

    /// Increment nlimbo so that gctx won't go away before dump. Additionally, link gctx into the dump list so that it
    /// is included in the second pass of the dump.
    ++global_context->num_limbo;
    global_contexts->insert(global_context);

    memset(&global_context->count_summed, 0, sizeof(ProfilingCounters));
}

/// jemalloc: prof_gctx_merge_iter
void profilingGlobalContextMergeIterate(
    ThreadState * thread_state, ProfilingGlobalContext * global_context, size_t * leak_num_global_contexts)
{
    MutexLock lock(thread_state, *global_context->lock);
    global_context->thread_contexts.iterate(
        nullptr,
        [&](ProfilingThreadContext * thread_context) -> ProfilingThreadContext *
        {
            /// jemalloc: prof_tctx_merge_iter
            thread_context->global_context->lock->assertOwner(thread_state);
            switch (thread_context->state)
            {
                case profiling_thread_context_state_nominal:
                    /// New since dumping started; ignore.
                    break;
                case profiling_thread_context_state_dumping:
                case profiling_thread_context_state_purgatory:
                    profilingThreadContextMergeGlobalContext(thread_state, thread_context, thread_context->global_context);
                    break;
                case profiling_thread_context_state_initializing: ALLOCATOR_NOT_REACHED();
            }
            return nullptr;
        });
    if (global_context->count_summed.current_objects != 0)
        ++*leak_num_global_contexts;
}

/// jemalloc: prof_gctx_finish
void profilingGlobalContextFinish(ThreadState & thread_state, ProfilingGlobalContextTree * global_contexts)
{
    ProfilingThreadData * thread_data = profilingThreadDataGet(thread_state, false);
    ProfilingGlobalContext * global_context;

    /// Standard tree iteration won't work here, because as soon as we decrement gctx->nlimbo and unlock gctx, another
    /// thread can concurrently destroy it, which will corrupt the tree. Therefore, tear down the tree one node at a
    /// time during iteration.
    while ((global_context = global_contexts->first()) != nullptr)
    {
        global_contexts->remove(global_context);
        global_context->lock->lock(&thread_state);
        {
            ProfilingThreadContext * next = nullptr;
            do
            {
                ProfilingThreadContext * to_destroy = global_context->thread_contexts.iterate(
                    next,
                    [&](ProfilingThreadContext * thread_context) -> ProfilingThreadContext *
                    { return profilingThreadContextFinishIterate(&thread_state, thread_context); });
                if (to_destroy != nullptr)
                {
                    next = global_context->thread_contexts.next(to_destroy);
                    global_context->thread_contexts.remove(to_destroy);
                    profilingInternalDeallocate(&thread_state, to_destroy);
                }
                else
                {
                    next = nullptr;
                }
            } while (next != nullptr);
        }
        --global_context->num_limbo;
        if (profilingGlobalContextShouldDestroy(global_context))
        {
            ++global_context->num_limbo;
            global_context->lock->unlock(&thread_state);
            profilingGlobalContextTryDestroy(thread_state, thread_data, global_context);
        }
        else
        {
            global_context->lock->unlock(&thread_state);
        }
    }
}

/// jemalloc: prof_tdata_merge_iter
void profilingThreadDataMergeIterate(ThreadState * thread_state, ProfilingThreadData * thread_data, ProfilingCounters * count_all)
{
    MutexLock lock(thread_state, *thread_data->lock);
    if (!thread_data->expired)
    {
        thread_data->dumping = true;
        memset(&thread_data->count_summed, 0, sizeof(ProfilingCounters));
        void * thread_context_v = nullptr;
        for (size_t table_idx = 0; !thread_data->backtrace_to_thread_context.iterate(&table_idx, nullptr, &thread_context_v);)
            profilingThreadContextMergeThreadData(thread_state, static_cast<ProfilingThreadContext *>(thread_context_v), thread_data);

        profilingCountsMerge(*count_all, thread_data->count_summed);
    }
    else
    {
        thread_data->dumping = false;
    }
}

/// jemalloc: prof_tdata_dump_iter
void profilingThreadDataDumpIterate(ProfilingDumpIterateArg * arg, ProfilingThreadData * thread_data)
{
    if (!thread_data->dumping)
        return;

    profilingDumpPrintf(
        arg->profiling_dump_write, arg->callback_argument, "  t%llu: ", static_cast<unsigned long long>(thread_data->thread_uid));
    profilingDumpPrintCounts(arg->profiling_dump_write, arg->callback_argument, &thread_data->count_summed);
    if (!profilingThreadNameEmpty(thread_data))
    {
        arg->profiling_dump_write(arg->callback_argument, " ");
        arg->profiling_dump_write(arg->callback_argument, thread_data->thread_name);
    }
    arg->profiling_dump_write(arg->callback_argument, "\n");
}

/// jemalloc: prof_dump_header
void profilingDumpHeader(ProfilingDumpIterateArg * arg, const ProfilingCounters * count_all)
{
    profilingDumpPrintf(
        arg->profiling_dump_write,
        arg->callback_argument,
        "heap_v2/%llu\n  t*: ",
        static_cast<unsigned long long>(uint64_t(1U) << log2_profiling_sample));
    profilingDumpPrintCounts(arg->profiling_dump_write, arg->callback_argument, count_all);
    arg->profiling_dump_write(arg->callback_argument, "\n");

    MutexLock lock(arg->thread_state, all_thread_data_mutex);
    all_thread_data.iterate(
        nullptr,
        [&](ProfilingThreadData * thread_data) -> ProfilingThreadData *
        {
            profilingThreadDataDumpIterate(arg, thread_data);
            return nullptr;
        });
}

/// jemalloc: prof_dump_gctx
void profilingDumpGlobalContext(
    ProfilingDumpIterateArg * arg, ProfilingGlobalContext * global_context, const ProfilingBacktrace * backtrace)
{
    global_context->lock->assertOwner(arg->thread_state);

    /// Avoid dumping such gctx's that have no useful data.
    if ((!options.profiling_accumulated && global_context->count_summed.current_objects == 0)
        || (options.profiling_accumulated && global_context->count_summed.accumulated_objects == 0))
    {
        ALLOCATOR_ASSERT(global_context->count_summed.current_objects == 0);
        ALLOCATOR_ASSERT(global_context->count_summed.current_bytes == 0);
        /// The asserts on the unbiased cur counters would not be correct (races with `prof.reset`).
        ALLOCATOR_ASSERT(global_context->count_summed.accumulated_objects == 0);
        ALLOCATOR_ASSERT(global_context->count_summed.accumulated_objects_shifted_unbiased == 0);
        ALLOCATOR_ASSERT(global_context->count_summed.accumulated_bytes == 0);
        ALLOCATOR_ASSERT(global_context->count_summed.accumulated_bytes_unbiased == 0);
        return;
    }

    arg->profiling_dump_write(arg->callback_argument, "@");
    for (unsigned i = 0; i < backtrace->len; ++i)
        profilingDumpPrintf(
            arg->profiling_dump_write,
            arg->callback_argument,
            " %#lx",
            static_cast<unsigned long>(reinterpret_cast<uintptr_t>(backtrace->vector[i])));

    arg->profiling_dump_write(arg->callback_argument, "\n  t*: ");
    profilingDumpPrintCounts(arg->profiling_dump_write, arg->callback_argument, &global_context->count_summed);
    arg->profiling_dump_write(arg->callback_argument, "\n");

    global_context->thread_contexts.iterate(
        nullptr,
        [&](ProfilingThreadContext * thread_context) -> ProfilingThreadContext *
        {
            profilingThreadContextDumpIterate(arg, thread_context);
            return nullptr;
        });

    /// One record per live sampled allocation attributed to this gctx, for fragmentation profiling (ignored by
    /// jeprof):
    ///     f: <age_ns> <request_size> <usize> <szind> <arena_ind> <thr_uid>
    /// An allocation on frag_objs cannot be concurrently deallocated (its sever point takes gctx->lock), and its tctx
    /// is pinned by the allocation's curobjs count, so all reads below are stable.
    for (Extent * extent = global_context->fragmentation_objects.first(); extent != nullptr;
         extent = global_context->fragmentation_objects.next(extent))
    {
        const Nanoseconds * alloc_time = extent->profilingAllocTime();
        Nanoseconds age = Nanoseconds::zero();
        if (arg->now.compare(*alloc_time) > 0)
        {
            age.copy(arg->now);
            age.subtract(*alloc_time);
        }
        else
        {
            /// Sampled after this dump's timestamp was taken (or within the prof clock resolution).
            age = Nanoseconds::zero();
        }
        ProfilingThreadContext * thread_context = extent->profilingThreadContext();
        ALLOCATOR_ASSERT(profilingThreadContextIsValid(thread_context));
        profilingDumpPrintf(
            arg->profiling_dump_write,
            arg->callback_argument,
            "  f: %llu %zu %zu %u %u %llu\n",
            static_cast<unsigned long long>(age.ns()),
            extent->profilingAllocSize(),
            extent->usableSize(),
            static_cast<unsigned>(extent->sizeClassIdx()),
            extent->arenaIdx(),
            static_cast<unsigned long long>(thread_context->thread_uid));
    }
}

/// Scaling is equivalent AdjustSamples() in jeprof, but the result may differ slightly from what jeprof reports,
/// because here we scale the summary values, whereas jeprof scales each context individually and reports the sums of
/// the scaled values.
/// jemalloc: prof_leakcheck
void profilingLeakCheck(const ProfilingCounters * count_all, size_t leak_num_global_contexts)
{
    if (count_all->current_bytes != 0)
    {
        double sample_period = static_cast<double>(uint64_t(1) << log2_profiling_sample);
        double ratio = ((static_cast<double>(count_all->current_bytes)) / static_cast<double>(count_all->current_objects)) / sample_period;
        double scale_factor = 1.0 / (1.0 - exp(-ratio));
        auto current_bytes = static_cast<uint64_t>(round((static_cast<double>(count_all->current_bytes)) * scale_factor));
        auto current_objects = static_cast<uint64_t>(round((static_cast<double>(count_all->current_objects)) * scale_factor));

        printMessage(
            "<jemalloc>: Leak approximation summary: ~%llu byte%s, ~%llu object%s, >= %zu context%s\n",
            static_cast<unsigned long long>(current_bytes),
            (current_bytes != 1) ? "s" : "",
            static_cast<unsigned long long>(current_objects),
            (current_objects != 1) ? "s" : "",
            leak_num_global_contexts,
            (leak_num_global_contexts != 1) ? "s" : "");
        printMessage("<jemalloc>: Run jeprof on dump output for leak detail\n");
        if (options.profiling_leak_error)
        {
            printMessage("<jemalloc>: Exiting with error code because memory leaks were detected\n");
            /// Use `_exit` with underscore to avoid calling `atexit` and entering endless cycle.
            _exit(1);
        }
    }
}

/// Per-(arena, bin) slab utilization snapshot, for fragmentation profiling (ignored by jeprof):
///     frag_util: <arena_ind> <binind> <reg_size> <slab_size> <nregs> <n_shards> <curslabs> <curregs> <nonfull_slabs>
/// Wasted memory per line is curslabs * slab_size - curregs * reg_size.
/// jemalloc: prof_dump_frag_util
void profilingDumpFragmentationUtilization(ProfilingDumpIterateArg * arg)
{
    if constexpr (!config::stats)
    {
        /// curslabs/curregs/nonfull_slabs are not maintained.
        return;
    }
    for (unsigned i = 0; i < numArenasTotalGet(); ++i)
    {
        Arena * arena = arenaGet(arg->thread_state, i, false);
        if (arena == nullptr)
            continue;
        for (SizeClassIdx j = 0; j < SIZE_CLASS_NUM_BINS; ++j)
        {
            const BinInfo * info = &bin_infos[j];
            size_t current_slabs = 0;
            size_t current_regions = 0;
            size_t non_full_slabs = 0;
            for (unsigned k = 0; k < info->num_shards; ++k)
            {
                Bin * bin = arenaGetBin(arena, j, k);
                MutexLock lock(arg->thread_state, bin->lock);
                current_slabs += bin->stats.current_slabs;
                current_regions += bin->stats.current_regions;
                non_full_slabs += bin->stats.non_full_slabs;
            }
            if (current_slabs == 0)
                continue;
            profilingDumpPrintf(
                arg->profiling_dump_write,
                arg->callback_argument,
                "frag_util: %u %u %zu %zu %u %u %zu %zu %zu\n",
                i,
                static_cast<unsigned>(j),
                info->region_size,
                info->slab_size,
                info->num_regions,
                info->num_shards,
                current_slabs,
                current_regions,
                non_full_slabs);
        }
    }
}

/// jemalloc: prof_dump_prep
void profilingDumpPrepare(
    ThreadState & thread_state,
    ProfilingThreadData * thread_data,
    ProfilingCounters * count_all,
    size_t * leak_num_global_contexts,
    ProfilingGlobalContextTree * global_contexts)
{
    profilingEnter(thread_state, thread_data);

    /// Put gctx's in limbo and clear their counters in preparation for summing.
    global_contexts->init();
    void * global_context_v = nullptr;
    for (size_t table_idx = 0; !backtrace_to_global_context.iterate(&table_idx, nullptr, &global_context_v);)
        profilingDumpGlobalContextPrepare(&thread_state, static_cast<ProfilingGlobalContext *>(global_context_v), global_contexts);

    /// Iterate over tdatas, and for the non-expired ones snapshot their tctx stats and merge them into the associated
    /// gctx's.
    memset(count_all, 0, sizeof(ProfilingCounters));
    {
        MutexLock lock(&thread_state, all_thread_data_mutex);
        all_thread_data.iterate(
            nullptr,
            [&](ProfilingThreadData * td) -> ProfilingThreadData *
            {
                profilingThreadDataMergeIterate(&thread_state, td, count_all);
                return nullptr;
            });
    }

    /// Merge tctx stats into gctx's.
    *leak_num_global_contexts = 0;
    global_contexts->iterate(
        nullptr,
        [&](ProfilingGlobalContext * global_context) -> ProfilingGlobalContext *
        {
            profilingGlobalContextMergeIterate(&thread_state, global_context, leak_num_global_contexts);
            return nullptr;
        });

    profilingLeave(thread_state, thread_data);
}

}

/// jemalloc: prof_dump_impl
void profilingDumpImpl(
    ThreadState & thread_state,
    WriteCallback * profiling_dump_write,
    void * callback_argument,
    ProfilingThreadData * thread_data,
    bool leak_check)
{
    profiling_dump_mutex.assertOwner(&thread_state);
    ProfilingCounters count_all;
    size_t leak_num_global_contexts;
    ProfilingGlobalContextTree global_contexts;
    profilingDumpPrepare(thread_state, thread_data, &count_all, &leak_num_global_contexts, &global_contexts);
    ProfilingDumpIterateArg profiling_dump_iterate_arg = {&thread_state, profiling_dump_write, callback_argument, Nanoseconds::zero()};
    profiling_dump_iterate_arg.now.profilingInitUpdate();
    profilingDumpHeader(&profiling_dump_iterate_arg, &count_all);
    global_contexts.iterate(
        nullptr,
        [&](ProfilingGlobalContext * global_context) -> ProfilingGlobalContext *
        {
            /// jemalloc: prof_gctx_dump_iter
            MutexLock lock(&thread_state, *global_context->lock);
            profilingDumpGlobalContext(&profiling_dump_iterate_arg, global_context, &global_context->backtrace);
            return nullptr;
        });
    profilingDumpFragmentationUtilization(&profiling_dump_iterate_arg);
    profilingGlobalContextFinish(thread_state, &global_contexts);
    if (leak_check)
        profilingLeakCheck(&count_all, leak_num_global_contexts);
}

/// jemalloc: prof_bt_hash
void profilingBacktraceHash(const void * key, size_t result_hash[2])
{
    const auto * backtrace = static_cast<const ProfilingBacktrace *>(key);
    hash::hash(backtrace->vector, backtrace->len * sizeof(void *), 0x94122f33U, result_hash);
}

/// jemalloc: prof_bt_keycomp
bool profilingBacktraceKeyCompare(const void * k1, const void * k2)
{
    const auto * backtrace1 = static_cast<const ProfilingBacktrace *>(k1);
    const auto * backtrace2 = static_cast<const ProfilingBacktrace *>(k2);

    if (backtrace1->len != backtrace2->len)
        return false;
    return memcmp(backtrace1->vector, backtrace2->vector, backtrace1->len * sizeof(void *)) == 0;
}

/// --- Thread data ---------------------------------------------------------------------------------------------------

/// jemalloc: prof_tdata_init_impl
ProfilingThreadData * profilingThreadDataInitImpl(
    ThreadState & thread_state, uint64_t thread_uid, uint64_t thread_discriminator, const char * thread_name, bool active)
{
    ALLOCATOR_ASSERT(thread_state.reentrancyLevel() == 0);

    /// Initialize an empty cache for this thread.
    size_t thread_data_size = alignmentCeiling(sizeof(ProfilingThreadData), QUANTUM);
    size_t total_size = thread_data_size + sizeof(void *) * options.profiling_backtrace_max;
    auto * thread_data = static_cast<ProfilingThreadData *>(profilingAllocArena0(thread_state, total_size, true));
    if (thread_data == nullptr)
        return nullptr;

    thread_data->vector = reinterpret_cast<void **>(reinterpret_cast<uint8_t *>(thread_data) + thread_data_size);
    thread_data->lock = profilingThreadDataMutexChoose(thread_uid);
    thread_data->thread_uid = thread_uid;
    thread_data->thread_discriminator = thread_discriminator;
    thread_data->attached = true;
    thread_data->expired = false;
    thread_data->thread_context_uid_next = 0;
    if (thread_name == nullptr)
        profilingThreadNameClear(thread_data);
    else
        profilingThreadNameWriteThreadData(thread_data, thread_name);
    profilingThreadNameAssert(thread_data);

    if (thread_data->backtrace_to_thread_context.init(
            thread_state, PROFILING_CUCKOO_HASH_MIN_ITEMS, profilingBacktraceHash, profilingBacktraceKeyCompare))
    {
        profilingInternalDeallocate(&thread_state, thread_data);
        return nullptr;
    }

    thread_data->enqueued = false;
    thread_data->enqueued_interval_dump = false;
    thread_data->enqueued_growth_dump = false;

    thread_data->dumping = false;
    thread_data->active = active;

    MutexLock lock(&thread_state, all_thread_data_mutex);
    all_thread_data.insert(thread_data);

    return thread_data;
}

namespace
{

/// jemalloc: prof_tdata_should_destroy_unlocked
bool profilingThreadDataShouldDestroyUnlocked(ProfilingThreadData * thread_data, bool even_if_attached)
{
    if (thread_data->attached && !even_if_attached)
        return false;
    if (thread_data->backtrace_to_thread_context.count() != 0)
        return false;
    return true;
}

/// jemalloc: prof_tdata_should_destroy
bool profilingThreadDataShouldDestroy(ThreadState * thread_state, ProfilingThreadData * thread_data, bool even_if_attached)
{
    thread_data->lock->assertOwner(thread_state);
    return profilingThreadDataShouldDestroyUnlocked(thread_data, even_if_attached);
}

/// jemalloc: prof_tdata_destroy_locked
void profilingThreadDataDestroyLocked(ThreadState & thread_state, ProfilingThreadData * thread_data, bool even_if_attached)
{
    all_thread_data_mutex.assertOwner(&thread_state);
    thread_data->lock->assertNotOwner(&thread_state);

    all_thread_data.remove(thread_data);
    ALLOCATOR_ASSERT(profilingThreadDataShouldDestroyUnlocked(thread_data, even_if_attached));
    (void)even_if_attached;

    thread_data->backtrace_to_thread_context.destroy(thread_state);
    profilingInternalDeallocate(&thread_state, thread_data);
}

/// jemalloc: prof_tdata_destroy
void profilingThreadDataDestroy(ThreadState & thread_state, ProfilingThreadData * thread_data, bool even_if_attached)
{
    MutexLock lock(&thread_state, all_thread_data_mutex);
    profilingThreadDataDestroyLocked(thread_state, thread_data, even_if_attached);
}

/// jemalloc: prof_tdata_expire
bool profilingThreadDataExpire(ThreadState * thread_state, ProfilingThreadData * thread_data)
{
    bool destroy_thread_data;

    MutexLock lock(thread_state, *thread_data->lock);
    if (!thread_data->expired)
    {
        thread_data->expired = true;
        destroy_thread_data = profilingThreadDataShouldDestroy(thread_state, thread_data, false);
    }
    else
    {
        destroy_thread_data = false;
    }

    return destroy_thread_data;
}

}

/// jemalloc: prof_tdata_detach
void profilingThreadDataDetach(ThreadState & thread_state, ProfilingThreadData * thread_data)
{
    bool destroy_thread_data;

    thread_data->lock->lock(&thread_state);
    if (thread_data->attached)
    {
        destroy_thread_data = profilingThreadDataShouldDestroy(&thread_state, thread_data, true);
        /// Only detach if !destroy_tdata, because detaching would allow another thread to win the race to destroy
        /// tdata.
        if (!destroy_thread_data)
            thread_data->attached = false;
        thread_state.profiling_thread_data = nullptr;
    }
    else
    {
        destroy_thread_data = false;
    }
    thread_data->lock->unlock(&thread_state);
    if (destroy_thread_data)
        profilingThreadDataDestroy(thread_state, thread_data, true);
}

/// jemalloc: prof_reset
void profilingReset(ThreadState & thread_state, size_t log2_sample)
{
    ALLOCATOR_ASSERT(log2_sample < (sizeof(uint64_t) << 3));

    MutexLock dump_lock(&thread_state, profiling_dump_mutex);
    MutexLock all_thread_data_lock(&thread_state, all_thread_data_mutex);

    log2_profiling_sample = log2_sample;
    profilingUnbiasMapInit();

    ProfilingThreadData * next = nullptr;
    do
    {
        /// jemalloc: prof_tdata_reset_iter
        ProfilingThreadData * to_destroy = all_thread_data.iterate(
            next,
            [&](ProfilingThreadData * thread_data) -> ProfilingThreadData *
            { return profilingThreadDataExpire(&thread_state, thread_data) ? thread_data : nullptr; });
        if (to_destroy != nullptr)
        {
            next = all_thread_data.next(to_destroy);
            profilingThreadDataDestroyLocked(thread_state, to_destroy, false);
        }
        else
        {
            next = nullptr;
        }
    } while (next != nullptr);
}

/// --- Thread contexts -----------------------------------------------------------------------------------------------

namespace
{

/// jemalloc: prof_tctx_should_destroy
bool profilingThreadContextShouldDestroy(ThreadState & thread_state, ProfilingThreadContext * thread_context)
{
    thread_context->thread_data->lock->assertOwner(&thread_state);

    if (options.profiling_accumulated)
        return false;
    if (thread_context->counts.current_objects != 0)
        return false;
    if (thread_context->prepared)
        return false;
    if (thread_context->recent_count != 0)
        return false;
    return true;
}

/// jemalloc: prof_tctx_destroy
void profilingThreadContextDestroy(ThreadState & thread_state, ProfilingThreadContext * thread_context)
{
    thread_context->thread_data->lock->assertOwner(&thread_state);

    ALLOCATOR_ASSERT(thread_context->counts.current_objects == 0);
    ALLOCATOR_ASSERT(thread_context->counts.current_bytes == 0);
    /// The asserts on the unbiased cur counters are not correct (races with `prof.reset`).
    ALLOCATOR_ASSERT(!options.profiling_accumulated);
    ALLOCATOR_ASSERT(thread_context->counts.accumulated_objects == 0);
    ALLOCATOR_ASSERT(thread_context->counts.accumulated_bytes == 0);
    /// These ones are, since accumbyte counts never go down.
    ALLOCATOR_ASSERT(thread_context->counts.accumulated_objects_shifted_unbiased == 0);
    ALLOCATOR_ASSERT(thread_context->counts.accumulated_bytes_unbiased == 0);

    ProfilingGlobalContext * global_context = thread_context->global_context;

    {
        ProfilingThreadData * thread_data = thread_context->thread_data;
        thread_context->thread_data = nullptr;
        thread_data->backtrace_to_thread_context.remove(thread_state, &global_context->backtrace, nullptr, nullptr);
        bool destroy_thread_data = profilingThreadDataShouldDestroy(&thread_state, thread_data, false);
        thread_data->lock->unlock(&thread_state);
        if (destroy_thread_data)
            profilingThreadDataDestroy(thread_state, thread_data, false);
    }

    bool destroy_thread_context;
    bool destroy_global_context;

    global_context->lock->lock(&thread_state);
    switch (thread_context->state)
    {
        case profiling_thread_context_state_nominal:
            global_context->thread_contexts.remove(thread_context);
            destroy_thread_context = true;
            if (profilingGlobalContextShouldDestroy(global_context))
            {
                /// Increment gctx->nlimbo in order to keep another thread from winning the race to destroy gctx while
                /// this one has gctx->lock dropped.
                ++global_context->num_limbo;
                destroy_global_context = true;
            }
            else
            {
                destroy_global_context = false;
            }
            break;
        case profiling_thread_context_state_dumping:
            /// A dumping thread needs tctx to remain valid until dumping has finished. Change state such that the
            /// dumping thread will complete destruction during a late dump iteration phase.
            thread_context->state = profiling_thread_context_state_purgatory;
            destroy_thread_context = false;
            destroy_global_context = false;
            break;
        case profiling_thread_context_state_initializing:
        case profiling_thread_context_state_purgatory:
        default:
            ALLOCATOR_NOT_REACHED();
            destroy_thread_context = false;
            destroy_global_context = false;
    }
    global_context->lock->unlock(&thread_state);
    if (destroy_global_context)
        profilingGlobalContextTryDestroy(thread_state, profilingThreadDataGet(thread_state, false), global_context);
    if (destroy_thread_context)
        profilingInternalDeallocate(&thread_state, thread_context);
}

}

/// jemalloc: prof_tctx_try_destroy
void profilingThreadContextTryDestroy(ThreadState & thread_state, ProfilingThreadContext * thread_context)
{
    thread_context->thread_data->lock->assertOwner(&thread_state);
    if (profilingThreadContextShouldDestroy(thread_state, thread_context))
    {
        /// tctx->tdata->lock will be released in `profilingThreadContextDestroy`.
        profilingThreadContextDestroy(thread_state, thread_context);
    }
    else
    {
        thread_context->thread_data->lock->unlock(&thread_state);
    }
}

}
