/// The record of the last `profiling_recent_alloc_max` sampled allocations (`experimental.prof_recent.*`)
/// (jemalloc: `prof_recent.c`).

#include <allocator/Profiling.h>

#include <allocator/BufferedWriter.h>
#include <allocator/Emitter.h>
#include <allocator/Format.h>
#include <allocator/Frontend.h>
#include <allocator/Options.h>

namespace jemalloc
{

/// Protects the fields below. jemalloc: prof_recent_alloc_mtx
constinit Mutex profiling_recent_alloc_mutex;
/// Protects dumping. jemalloc: prof_recent_dump_mtx
constinit Mutex profiling_recent_dump_mutex;

namespace
{

/// jemalloc: prof_recent_alloc_max
constinit std::atomic<ssize_t> profiling_recent_alloc_max{0};
/// jemalloc: prof_recent_alloc_count
constinit ssize_t profiling_recent_alloc_count = 0;
/// jemalloc: prof_recent_alloc_list
constinit ProfilingRecentList profiling_recent_alloc_list;

/// jemalloc: prof_recent_alloc_max_init
void profilingRecentAllocMaxInit()
{
    profiling_recent_alloc_max.store(options.profiling_recent_alloc_max, std::memory_order_relaxed);
}

/// jemalloc: prof_recent_alloc_max_get_no_lock
inline ssize_t profilingRecentAllocMaxGetNoLock()
{
    return profiling_recent_alloc_max.load(std::memory_order_relaxed);
}

/// jemalloc: prof_recent_alloc_max_get
inline ssize_t profilingRecentAllocMaxGet(ThreadState & thread_state)
{
    profiling_recent_alloc_mutex.assertOwner(&thread_state);
    return profilingRecentAllocMaxGetNoLock();
}

/// jemalloc: prof_recent_alloc_max_update
inline ssize_t profilingRecentAllocMaxUpdate(ThreadState & thread_state, ssize_t max)
{
    profiling_recent_alloc_mutex.assertOwner(&thread_state);
    ssize_t old_max = profilingRecentAllocMaxGet(thread_state);
    profiling_recent_alloc_max.store(max, std::memory_order_relaxed);
    return old_max;
}

/// jemalloc: prof_recent_allocate_node
ProfilingRecent * profilingRecentAllocateNode(ThreadState & thread_state)
{
    return static_cast<ProfilingRecent *>(profilingAllocArena0(thread_state, sizeof(ProfilingRecent), false));
}

/// jemalloc: prof_recent_free_node
void profilingRecentFreeNode(ThreadState & thread_state, ProfilingRecent * node)
{
    ALLOCATOR_ASSERT(node != nullptr);
    ALLOCATOR_ASSERT(allocationSize(&thread_state, node) == size_classes::sizeToUsableSize(sizeof(ProfilingRecent)));
    profilingInternalDeallocate(&thread_state, node);
}

/// jemalloc: increment_recent_count
inline void incrementRecentCount(ThreadState & thread_state, ProfilingThreadContext * thread_context)
{
    thread_context->thread_data->lock->assertOwner(&thread_state);
    ++thread_context->recent_count;
    ALLOCATOR_ASSERT(thread_context->recent_count > 0);
}

}

/// jemalloc: prof_recent_alloc_prepare
bool profilingRecentAllocPrepare(ThreadState & thread_state, ProfilingThreadContext * thread_context)
{
    ALLOCATOR_ASSERT(options.profiling && profiling_booted);
    thread_context->thread_data->lock->assertOwner(&thread_state);
    profiling_recent_alloc_mutex.assertNotOwner(&thread_state);

    /// Check whether last-N mode is turned on without trying to acquire the lock, so as to optimize for the following
    /// two scenarios: (1) Last-N mode is switched off; (2) Dumping, during which last-N mode is temporarily turned off
    /// so as not to block sampled allocations.
    if (profilingRecentAllocMaxGetNoLock() == 0)
        return false;

    /// Increment recent_count to hold the tctx so that it won't be gone even after tctx->tdata->lock is released.
    /// This acts as a "placeholder"; the real recording of the allocation requires a lock on
    /// `profiling_recent_alloc_mutex` and is done in `profilingRecentAlloc` (when tctx->tdata->lock has been released).
    incrementRecentCount(thread_state, thread_context);
    return true;
}

namespace
{

/// jemalloc: decrement_recent_count
void decrementRecentCount(ThreadState & thread_state, ProfilingThreadContext * thread_context)
{
    profiling_recent_alloc_mutex.assertNotOwner(&thread_state);
    ALLOCATOR_ASSERT(thread_context != nullptr);
    thread_context->thread_data->lock->lock(&thread_state);
    ALLOCATOR_ASSERT(thread_context->recent_count > 0);
    --thread_context->recent_count;
    profilingThreadContextTryDestroy(thread_state, thread_context);
}

/// jemalloc: prof_recent_alloc_edata_get_no_lock
inline Extent * profilingRecentAllocExtentGetNoLock(const ProfilingRecent * n)
{
    return n->alloc_extent.load(std::memory_order_acquire);
}

/// jemalloc: prof_recent_alloc_edata_get
inline Extent * profilingRecentAllocExtentGet(ThreadState & thread_state, const ProfilingRecent * n)
{
    profiling_recent_alloc_mutex.assertOwner(&thread_state);
    return profilingRecentAllocExtentGetNoLock(n);
}

/// jemalloc: prof_recent_alloc_edata_set
void profilingRecentAllocExtentSet(ThreadState & thread_state, ProfilingRecent * n, Extent * extent)
{
    profiling_recent_alloc_mutex.assertOwner(&thread_state);
    n->alloc_extent.store(extent, std::memory_order_release);
}

/// jemalloc: edata_prof_recent_alloc_get_no_lock
inline ProfilingRecent * extentProfilingRecentAllocGetNoLock(const Extent * extent)
{
    return extent->profilingRecentAllocGetDontCallDirectly();
}

/// jemalloc: edata_prof_recent_alloc_get
inline ProfilingRecent * extentProfilingRecentAllocGet(ThreadState & thread_state, const Extent * extent)
{
    profiling_recent_alloc_mutex.assertOwner(&thread_state);
    ProfilingRecent * recent_alloc = extentProfilingRecentAllocGetNoLock(extent);
    ALLOCATOR_ASSERT(recent_alloc == nullptr || profilingRecentAllocExtentGet(thread_state, recent_alloc) == extent);
    return recent_alloc;
}

/// jemalloc: edata_prof_recent_alloc_update_internal
ProfilingRecent * extentProfilingRecentAllocUpdateInternal(ThreadState & thread_state, Extent * extent, ProfilingRecent * recent_alloc)
{
    profiling_recent_alloc_mutex.assertOwner(&thread_state);
    ProfilingRecent * old_recent_alloc = extentProfilingRecentAllocGet(thread_state, extent);
    extent->setProfilingRecentAllocDontCallDirectly(recent_alloc);
    return old_recent_alloc;
}

/// jemalloc: edata_prof_recent_alloc_set
void extentProfilingRecentAllocSet(ThreadState & thread_state, Extent * extent, ProfilingRecent * recent_alloc)
{
    profiling_recent_alloc_mutex.assertOwner(&thread_state);
    ALLOCATOR_ASSERT(recent_alloc != nullptr);
    [[maybe_unused]] ProfilingRecent * old_recent_alloc = extentProfilingRecentAllocUpdateInternal(thread_state, extent, recent_alloc);
    ALLOCATOR_ASSERT(old_recent_alloc == nullptr);
    profilingRecentAllocExtentSet(thread_state, recent_alloc, extent);
}

/// jemalloc: edata_prof_recent_alloc_reset
void extentProfilingRecentAllocReset(ThreadState & thread_state, Extent * extent, ProfilingRecent * recent_alloc)
{
    profiling_recent_alloc_mutex.assertOwner(&thread_state);
    ALLOCATOR_ASSERT(recent_alloc != nullptr);
    [[maybe_unused]] ProfilingRecent * old_recent_alloc = extentProfilingRecentAllocUpdateInternal(thread_state, extent, nullptr);
    ALLOCATOR_ASSERT(old_recent_alloc == recent_alloc);
    ALLOCATOR_ASSERT(extent == profilingRecentAllocExtentGet(thread_state, recent_alloc));
    profilingRecentAllocExtentSet(thread_state, recent_alloc, nullptr);
}

}

/// This function should be called right before an allocation is released, so that the associated recent allocation
/// record can contain the following information: (1) The allocation is released; (2) The time of the deallocation;
/// and (3) The tctx associated with the deallocation.
/// jemalloc: prof_recent_alloc_reset
void profilingRecentAllocReset(ThreadState & thread_state, Extent * extent)
{
    /// Check whether the recent allocation record still exists without trying to acquire the lock.
    if (extentProfilingRecentAllocGetNoLock(extent) == nullptr)
        return;

    ProfilingThreadContext * deallocation_thread_context = profilingThreadContextCreate(thread_state);
    /// In case dalloc_tctx is null, e.g. due to OOM, we will not record the deallocation time / tctx, which is
    /// handled later, after we check again when holding the lock.

    if (deallocation_thread_context != nullptr)
    {
        deallocation_thread_context->thread_data->lock->lock(&thread_state);
        incrementRecentCount(thread_state, deallocation_thread_context);
        deallocation_thread_context->prepared = false;
        deallocation_thread_context->thread_data->lock->unlock(&thread_state);
    }

    profiling_recent_alloc_mutex.lock(&thread_state);
    /// Check again after acquiring the lock.
    ProfilingRecent * recent = extentProfilingRecentAllocGet(thread_state, extent);
    if (recent != nullptr)
    {
        ALLOCATOR_ASSERT(recent->deallocation_time.ns() == 0);
        ALLOCATOR_ASSERT(recent->deallocation_thread_context == nullptr);
        if (deallocation_thread_context != nullptr)
        {
            recent->deallocation_time.profilingUpdate();
            recent->deallocation_thread_context = deallocation_thread_context;
            deallocation_thread_context = nullptr;
        }
        extentProfilingRecentAllocReset(thread_state, extent, recent);
    }
    profiling_recent_alloc_mutex.unlock(&thread_state);

    if (deallocation_thread_context != nullptr)
    {
        /// We lost the race - the allocation record was just gone.
        decrementRecentCount(thread_state, deallocation_thread_context);
    }
}

namespace
{

/// jemalloc: prof_recent_alloc_evict_edata
void profilingRecentAllocEvictExtent(ThreadState & thread_state, ProfilingRecent * recent_alloc)
{
    profiling_recent_alloc_mutex.assertOwner(&thread_state);
    Extent * extent = profilingRecentAllocExtentGet(thread_state, recent_alloc);
    if (extent != nullptr)
        extentProfilingRecentAllocReset(thread_state, extent, recent_alloc);
}

/// jemalloc: prof_recent_alloc_is_empty
bool profilingRecentAllocIsEmpty(ThreadState & thread_state)
{
    profiling_recent_alloc_mutex.assertOwner(&thread_state);
    if (profiling_recent_alloc_list.empty())
    {
        ALLOCATOR_ASSERT(profiling_recent_alloc_count == 0);
        return true;
    }
    ALLOCATOR_ASSERT(profiling_recent_alloc_count > 0);
    return false;
}

/// jemalloc: prof_recent_alloc_assert_count
void profilingRecentAllocAssertCount(ThreadState & thread_state)
{
    profiling_recent_alloc_mutex.assertOwner(&thread_state);
    if constexpr (!config::debug)
        return;
    ssize_t count = 0;
    profiling_recent_alloc_list.forEach([&](ProfilingRecent *) { ++count; });
    ALLOCATOR_ASSERT(count == profiling_recent_alloc_count);
    ALLOCATOR_ASSERT(profilingRecentAllocMaxGet(thread_state) == -1 || count <= profilingRecentAllocMaxGet(thread_state));
    (void)count;
}

}

/// jemalloc: prof_recent_alloc
void profilingRecentAlloc(ThreadState & thread_state, Extent * extent, size_t size, size_t usable_size)
{
    ALLOCATOR_ASSERT(extent != nullptr);
    ProfilingThreadContext * thread_context = extent->profilingThreadContext();

    thread_context->thread_data->lock->assertNotOwner(&thread_state);
    profiling_recent_alloc_mutex.lock(&thread_state);
    profilingRecentAllocAssertCount(thread_state);

    /// Reserve a new ProfilingRecent node if needed. If needed, we release the `profiling_recent_alloc_mutex` lock and allocate.
    /// Then, rather than immediately checking for OOM, we regain the lock and try to make use of the reserve node if
    /// needed (see the comment in jemalloc's `prof_recent_alloc` for the six scenarios).
    ProfilingRecent * reserve = nullptr;
    ProfilingThreadContext * old_alloc_thread_context;
    ProfilingThreadContext * old_deallocation_thread_context;
    ProfilingRecent * tail;
    if (profilingRecentAllocMaxGet(thread_state) == -1 || profiling_recent_alloc_count < profilingRecentAllocMaxGet(thread_state))
    {
        ALLOCATOR_ASSERT(profilingRecentAllocMaxGet(thread_state) != 0);
        profiling_recent_alloc_mutex.unlock(&thread_state);
        reserve = profilingRecentAllocateNode(thread_state);
        profiling_recent_alloc_mutex.lock(&thread_state);
        profilingRecentAllocAssertCount(thread_state);
    }

    if (profilingRecentAllocMaxGet(thread_state) == 0)
    {
        ALLOCATOR_ASSERT(profilingRecentAllocIsEmpty(thread_state));
        goto label_rollback;
    }

    if (profiling_recent_alloc_count == profilingRecentAllocMaxGet(thread_state))
    {
        /// If upper limit is reached, rotate the head.
        ALLOCATOR_ASSERT(profilingRecentAllocMaxGet(thread_state) != -1);
        ALLOCATOR_ASSERT(!profilingRecentAllocIsEmpty(thread_state));
        ProfilingRecent * head = profiling_recent_alloc_list.first();
        old_alloc_thread_context = head->alloc_thread_context;
        ALLOCATOR_ASSERT(old_alloc_thread_context != nullptr);
        old_deallocation_thread_context = head->deallocation_thread_context;
        profilingRecentAllocEvictExtent(thread_state, head);
        profiling_recent_alloc_list.rotate();
    }
    else
    {
        /// Otherwise make use of the new node.
        ALLOCATOR_ASSERT(
            profilingRecentAllocMaxGet(thread_state) == -1 || profiling_recent_alloc_count < profilingRecentAllocMaxGet(thread_state));
        if (reserve == nullptr)
            goto label_rollback;
        ProfilingRecentList::elementInit(reserve);
        profiling_recent_alloc_list.tailInsert(reserve);
        reserve = nullptr;
        old_alloc_thread_context = nullptr;
        old_deallocation_thread_context = nullptr;
        ++profiling_recent_alloc_count;
    }

    /// Fill content into the tail node.
    tail = profiling_recent_alloc_list.last();
    ALLOCATOR_ASSERT(tail != nullptr);
    tail->size = size;
    tail->usable_size = usable_size;
    tail->alloc_time.copy(*extent->profilingAllocTime());
    tail->alloc_thread_context = thread_context;
    tail->deallocation_time.initZero();
    tail->deallocation_thread_context = nullptr;
    extentProfilingRecentAllocSet(thread_state, extent, tail);

    ALLOCATOR_ASSERT(!profilingRecentAllocIsEmpty(thread_state));
    profilingRecentAllocAssertCount(thread_state);
    profiling_recent_alloc_mutex.unlock(&thread_state);

    if (reserve != nullptr)
        profilingRecentFreeNode(thread_state, reserve);

    /// Asynchronously handle the tctx of the old node, so that there's no simultaneous holdings of
    /// `profiling_recent_alloc_mutex` and tdata->lock. In the worst case this may delay the tctx release but it's better
    /// than holding `profiling_recent_alloc_mutex` for longer.
    if (old_alloc_thread_context != nullptr)
        decrementRecentCount(thread_state, old_alloc_thread_context);
    if (old_deallocation_thread_context != nullptr)
        decrementRecentCount(thread_state, old_deallocation_thread_context);
    return;

label_rollback:
    ALLOCATOR_ASSERT(extentProfilingRecentAllocGet(thread_state, extent) == nullptr);
    profilingRecentAllocAssertCount(thread_state);
    profiling_recent_alloc_mutex.unlock(&thread_state);
    if (reserve != nullptr)
        profilingRecentFreeNode(thread_state, reserve);
    decrementRecentCount(thread_state, thread_context);
}

/// jemalloc: prof_recent_alloc_max_ctl_read
ssize_t profilingRecentAllocMaxMallctlRead()
{
    /// Don't bother to acquire the lock.
    return profilingRecentAllocMaxGetNoLock();
}

namespace
{

/// jemalloc: prof_recent_alloc_restore_locked
void profilingRecentAllocRestoreLocked(ThreadState & thread_state, ProfilingRecentList * to_delete)
{
    profiling_recent_alloc_mutex.assertOwner(&thread_state);
    ssize_t max = profilingRecentAllocMaxGet(thread_state);
    if (max == -1 || profiling_recent_alloc_count <= max)
    {
        /// Easy case - no need to alter the list.
        to_delete->init();
        profilingRecentAllocAssertCount(thread_state);
        return;
    }

    ProfilingRecent * node = nullptr;
    for (node = profiling_recent_alloc_list.first(); node != nullptr; node = profiling_recent_alloc_list.next(node))
    {
        if (profiling_recent_alloc_count == max)
            break;
        profilingRecentAllocEvictExtent(thread_state, node);
        --profiling_recent_alloc_count;
    }
    ALLOCATOR_ASSERT(profiling_recent_alloc_count == max);

    to_delete->moveFrom(profiling_recent_alloc_list);
    if (max == 0)
    {
        ALLOCATOR_ASSERT(node == nullptr);
    }
    else
    {
        ALLOCATOR_ASSERT(node != nullptr);
        to_delete->split(node, profiling_recent_alloc_list);
    }
    ALLOCATOR_ASSERT(!to_delete->empty());
    profilingRecentAllocAssertCount(thread_state);
}

/// jemalloc: prof_recent_alloc_async_cleanup
void profilingRecentAllocAsyncCleanup(ThreadState & thread_state, ProfilingRecentList * to_delete)
{
    profiling_recent_dump_mutex.assertNotOwner(&thread_state);
    profiling_recent_alloc_mutex.assertNotOwner(&thread_state);
    while (!to_delete->empty())
    {
        ProfilingRecent * node = to_delete->first();
        to_delete->remove(node);
        decrementRecentCount(thread_state, node->alloc_thread_context);
        if (node->deallocation_thread_context != nullptr)
            decrementRecentCount(thread_state, node->deallocation_thread_context);
        profilingRecentFreeNode(thread_state, node);
    }
}

}

/// jemalloc: prof_recent_alloc_max_ctl_write
ssize_t profilingRecentAllocMaxMallctlWrite(ThreadState & thread_state, ssize_t max)
{
    ALLOCATOR_ASSERT(max >= -1);
    profiling_recent_alloc_mutex.lock(&thread_state);
    profilingRecentAllocAssertCount(thread_state);
    const ssize_t old_max = profilingRecentAllocMaxUpdate(thread_state, max);
    ProfilingRecentList to_delete;
    profilingRecentAllocRestoreLocked(thread_state, &to_delete);
    profiling_recent_alloc_mutex.unlock(&thread_state);
    profilingRecentAllocAsyncCleanup(thread_state, &to_delete);
    return old_max;
}

namespace
{

/// jemalloc: prof_recent_alloc_dump_bt
void profilingRecentAllocDumpBacktrace(Emitter & emitter, ProfilingThreadContext * thread_context)
{
    char backtrace_buf[2 * sizeof(intptr_t) + 3];
    const char * s = backtrace_buf;
    ALLOCATOR_ASSERT(thread_context != nullptr);
    ProfilingBacktrace * backtrace = &thread_context->global_context->backtrace;
    for (size_t i = 0; i < backtrace->len; ++i)
    {
        format(backtrace_buf, sizeof(backtrace_buf), "%p", backtrace->vector[i]);
        emitter.jsonValue(EmitterType::String, &s);
    }
}

/// jemalloc: prof_recent_alloc_dump_node
void profilingRecentAllocDumpNode(Emitter & emitter, ProfilingRecent * node)
{
    emitter.jsonObjectBegin();

    emitter.jsonKeyValue("size", EmitterType::Size, &node->size);
    emitter.jsonKeyValue("usize", EmitterType::Size, &node->usable_size);
    bool released = profilingRecentAllocExtentGetNoLock(node) == nullptr;
    emitter.jsonKeyValue("released", EmitterType::Bool, &released);

    emitter.jsonKeyValue("alloc_thread_uid", EmitterType::Uint64, &node->alloc_thread_context->thread_uid);
    ProfilingThreadData * alloc_thread_data = node->alloc_thread_context->thread_data;
    ALLOCATOR_ASSERT(alloc_thread_data != nullptr);
    if (!profilingThreadNameEmpty(alloc_thread_data))
    {
        const char * thread_name = alloc_thread_data->thread_name;
        emitter.jsonKeyValue("alloc_thread_name", EmitterType::String, &thread_name);
    }
    uint64_t alloc_time_ns = node->alloc_time.ns();
    emitter.jsonKeyValue("alloc_time", EmitterType::Uint64, &alloc_time_ns);
    emitter.jsonArrayKeyValueBegin("alloc_trace");
    profilingRecentAllocDumpBacktrace(emitter, node->alloc_thread_context);
    emitter.jsonArrayEnd();

    if (released && node->deallocation_thread_context != nullptr)
    {
        emitter.jsonKeyValue("dalloc_thread_uid", EmitterType::Uint64, &node->deallocation_thread_context->thread_uid);
        ProfilingThreadData * thread_data_to_deallocate = node->deallocation_thread_context->thread_data;
        ALLOCATOR_ASSERT(thread_data_to_deallocate != nullptr);
        if (!profilingThreadNameEmpty(thread_data_to_deallocate))
        {
            const char * thread_name = thread_data_to_deallocate->thread_name;
            emitter.jsonKeyValue("dalloc_thread_name", EmitterType::String, &thread_name);
        }
        ALLOCATOR_ASSERT(node->deallocation_time.ns() != 0);
        uint64_t deallocation_time_ns = node->deallocation_time.ns();
        emitter.jsonKeyValue("dalloc_time", EmitterType::Uint64, &deallocation_time_ns);
        emitter.jsonArrayKeyValueBegin("dalloc_trace");
        profilingRecentAllocDumpBacktrace(emitter, node->deallocation_thread_context);
        emitter.jsonArrayEnd();
    }

    emitter.jsonObjectEnd();
}

/// jemalloc: PROF_RECENT_PRINT_BUFSIZE
constexpr size_t PROFILING_RECENT_PRINT_BUF_SIZE = 65536;

/// jemalloc: buf_writer_allocate_internal_buf / buf_writer_free_internal_buf (arena 0, internal)
void * profilingRecentBufferAllocate(ThreadState * thread_state, size_t size)
{
    return profilingAllocArena0(*thread_state, size, false);
}

void profilingRecentBufferDeallocate(ThreadState * thread_state, void * ptr)
{
    profilingInternalDeallocate(thread_state, ptr);
}

constexpr BufferAllocator profiling_recent_buffer_allocator = {profilingRecentBufferAllocate, profilingRecentBufferDeallocate};

}

/// jemalloc: prof_recent_alloc_dump
ALLOCATOR_NOINLINE void profilingRecentAllocDump(ThreadState & thread_state, WriteCallback * write_callback, void * callback_argument)
{
    profiling_recent_dump_mutex.lock(&thread_state);
    BufferedWriter buf_writer;
    buf_writer.init(
        &thread_state, write_callback, callback_argument, nullptr, PROFILING_RECENT_PRINT_BUF_SIZE, &profiling_recent_buffer_allocator);
    Emitter emitter(EmitterOutput::JSONCompact, BufferedWriter::callback, &buf_writer);
    ProfilingRecentList temp_list;

    profiling_recent_alloc_mutex.lock(&thread_state);
    profilingRecentAllocAssertCount(thread_state);
    ssize_t dump_max = profilingRecentAllocMaxGet(thread_state);
    temp_list.moveFrom(profiling_recent_alloc_list);
    ssize_t dump_count = profiling_recent_alloc_count;
    profiling_recent_alloc_count = 0;
    profilingRecentAllocAssertCount(thread_state);
    profiling_recent_alloc_mutex.unlock(&thread_state);

    emitter.begin();
    uint64_t sample_interval = uint64_t(1U) << log2_profiling_sample;
    emitter.jsonKeyValue("sample_interval", EmitterType::Uint64, &sample_interval);
    emitter.jsonKeyValue("recent_alloc_max", EmitterType::Ssize, &dump_max);
    emitter.jsonArrayKeyValueBegin("recent_alloc");
    temp_list.forEach([&](ProfilingRecent * node) { profilingRecentAllocDumpNode(emitter, node); });
    emitter.jsonArrayEnd();
    emitter.end();

    profiling_recent_alloc_mutex.lock(&thread_state);
    profilingRecentAllocAssertCount(thread_state);
    temp_list.concat(profiling_recent_alloc_list);
    profiling_recent_alloc_list.moveFrom(temp_list);
    profiling_recent_alloc_count += dump_count;
    profilingRecentAllocRestoreLocked(thread_state, &temp_list);
    profiling_recent_alloc_mutex.unlock(&thread_state);

    buf_writer.terminate(&thread_state);
    profiling_recent_dump_mutex.unlock(&thread_state);

    profilingRecentAllocAsyncCleanup(thread_state, &temp_list);
}

/// jemalloc: prof_recent_init
bool profilingRecentInit()
{
    profilingRecentAllocMaxInit();

    if (profiling_recent_alloc_mutex.init("prof_recent_alloc", MutexRank::PROFILING_RECENT_ALLOC))
        return true;

    if (profiling_recent_dump_mutex.init("prof_recent_dump", MutexRank::PROFILING_RECENT_DUMP))
        return true;

    profiling_recent_alloc_list.init();

    return false;
}

}
