/// The record of the last `prof_recent_alloc_max` sampled allocations (`experimental.prof_recent.*`)
/// (jemalloc: `prof_recent.c`).

#include <allocator/Prof.h>

#include <allocator/BufferedWriter.h>
#include <allocator/Emitter.h>
#include <allocator/Format.h>
#include <allocator/Frontend.h>
#include <allocator/Options.h>

namespace jemalloc
{

/// Protects the fields below. jemalloc: prof_recent_alloc_mtx
constinit Mutex prof_recent_alloc_mtx;
/// Protects dumping. jemalloc: prof_recent_dump_mtx
constinit Mutex prof_recent_dump_mtx;

namespace
{

/// jemalloc: prof_recent_alloc_max
constinit std::atomic<ssize_t> prof_recent_alloc_max{0};
/// jemalloc: prof_recent_alloc_count
constinit ssize_t prof_recent_alloc_count = 0;
/// jemalloc: prof_recent_alloc_list
constinit ProfRecentList prof_recent_alloc_list;

/// jemalloc: prof_recent_alloc_max_init
void profRecentAllocMaxInit()
{
    prof_recent_alloc_max.store(opt.prof_recent_alloc_max, std::memory_order_relaxed);
}

/// jemalloc: prof_recent_alloc_max_get_no_lock
inline ssize_t profRecentAllocMaxGetNoLock()
{
    return prof_recent_alloc_max.load(std::memory_order_relaxed);
}

/// jemalloc: prof_recent_alloc_max_get
inline ssize_t profRecentAllocMaxGet(ThreadState & tsd)
{
    prof_recent_alloc_mtx.assertOwner(&tsd);
    return profRecentAllocMaxGetNoLock();
}

/// jemalloc: prof_recent_alloc_max_update
inline ssize_t profRecentAllocMaxUpdate(ThreadState & tsd, ssize_t max)
{
    prof_recent_alloc_mtx.assertOwner(&tsd);
    ssize_t old_max = profRecentAllocMaxGet(tsd);
    prof_recent_alloc_max.store(max, std::memory_order_relaxed);
    return old_max;
}

/// jemalloc: prof_recent_allocate_node
ProfRecent * profRecentAllocateNode(ThreadState & tsd)
{
    return static_cast<ProfRecent *>(profAllocArena0(tsd, sizeof(ProfRecent), false));
}

/// jemalloc: prof_recent_free_node
void profRecentFreeNode(ThreadState & tsd, ProfRecent * node)
{
    JE_ASSERT(node != nullptr);
    JE_ASSERT(isalloc(&tsd, node) == sz::s2u(sizeof(ProfRecent)));
    profIdalloc(&tsd, node);
}

/// jemalloc: increment_recent_count
inline void incrementRecentCount(ThreadState & tsd, ProfThreadContext * tctx)
{
    tctx->tdata->lock->assertOwner(&tsd);
    ++tctx->recent_count;
    JE_ASSERT(tctx->recent_count > 0);
}

}

/// jemalloc: prof_recent_alloc_prepare
bool profRecentAllocPrepare(ThreadState & tsd, ProfThreadContext * tctx)
{
    JE_ASSERT(opt.prof && prof_booted);
    tctx->tdata->lock->assertOwner(&tsd);
    prof_recent_alloc_mtx.assertNotOwner(&tsd);

    /// Check whether last-N mode is turned on without trying to acquire the lock, so as to optimize for the following
    /// two scenarios: (1) Last-N mode is switched off; (2) Dumping, during which last-N mode is temporarily turned off
    /// so as not to block sampled allocations.
    if (profRecentAllocMaxGetNoLock() == 0)
        return false;

    /// Increment recent_count to hold the tctx so that it won't be gone even after tctx->tdata->lock is released.
    /// This acts as a "placeholder"; the real recording of the allocation requires a lock on
    /// `prof_recent_alloc_mtx` and is done in `profRecentAlloc` (when tctx->tdata->lock has been released).
    incrementRecentCount(tsd, tctx);
    return true;
}

namespace
{

/// jemalloc: decrement_recent_count
void decrementRecentCount(ThreadState & tsd, ProfThreadContext * tctx)
{
    prof_recent_alloc_mtx.assertNotOwner(&tsd);
    JE_ASSERT(tctx != nullptr);
    tctx->tdata->lock->lock(&tsd);
    JE_ASSERT(tctx->recent_count > 0);
    --tctx->recent_count;
    profTctxTryDestroy(tsd, tctx);
}

/// jemalloc: prof_recent_alloc_edata_get_no_lock
inline Extent * profRecentAllocEdataGetNoLock(const ProfRecent * n)
{
    return n->alloc_edata.load(std::memory_order_acquire);
}

/// jemalloc: prof_recent_alloc_edata_get
inline Extent * profRecentAllocEdataGet(ThreadState & tsd, const ProfRecent * n)
{
    prof_recent_alloc_mtx.assertOwner(&tsd);
    return profRecentAllocEdataGetNoLock(n);
}

/// jemalloc: prof_recent_alloc_edata_set
void profRecentAllocEdataSet(ThreadState & tsd, ProfRecent * n, Extent * edata)
{
    prof_recent_alloc_mtx.assertOwner(&tsd);
    n->alloc_edata.store(edata, std::memory_order_release);
}

/// jemalloc: edata_prof_recent_alloc_get_no_lock
inline ProfRecent * edataProfRecentAllocGetNoLock(const Extent * edata)
{
    return edata->profRecentAllocGetDontCallDirectly();
}

/// jemalloc: edata_prof_recent_alloc_get
inline ProfRecent * edataProfRecentAllocGet(ThreadState & tsd, const Extent * edata)
{
    prof_recent_alloc_mtx.assertOwner(&tsd);
    ProfRecent * recent_alloc = edataProfRecentAllocGetNoLock(edata);
    JE_ASSERT(recent_alloc == nullptr || profRecentAllocEdataGet(tsd, recent_alloc) == edata);
    return recent_alloc;
}

/// jemalloc: edata_prof_recent_alloc_update_internal
ProfRecent * edataProfRecentAllocUpdateInternal(ThreadState & tsd, Extent * edata, ProfRecent * recent_alloc)
{
    prof_recent_alloc_mtx.assertOwner(&tsd);
    ProfRecent * old_recent_alloc = edataProfRecentAllocGet(tsd, edata);
    edata->setProfRecentAllocDontCallDirectly(recent_alloc);
    return old_recent_alloc;
}

/// jemalloc: edata_prof_recent_alloc_set
void edataProfRecentAllocSet(ThreadState & tsd, Extent * edata, ProfRecent * recent_alloc)
{
    prof_recent_alloc_mtx.assertOwner(&tsd);
    JE_ASSERT(recent_alloc != nullptr);
    [[maybe_unused]] ProfRecent * old_recent_alloc = edataProfRecentAllocUpdateInternal(tsd, edata, recent_alloc);
    JE_ASSERT(old_recent_alloc == nullptr);
    profRecentAllocEdataSet(tsd, recent_alloc, edata);
}

/// jemalloc: edata_prof_recent_alloc_reset
void edataProfRecentAllocReset(ThreadState & tsd, Extent * edata, ProfRecent * recent_alloc)
{
    prof_recent_alloc_mtx.assertOwner(&tsd);
    JE_ASSERT(recent_alloc != nullptr);
    [[maybe_unused]] ProfRecent * old_recent_alloc = edataProfRecentAllocUpdateInternal(tsd, edata, nullptr);
    JE_ASSERT(old_recent_alloc == recent_alloc);
    JE_ASSERT(edata == profRecentAllocEdataGet(tsd, recent_alloc));
    profRecentAllocEdataSet(tsd, recent_alloc, nullptr);
}

}

/// This function should be called right before an allocation is released, so that the associated recent allocation
/// record can contain the following information: (1) The allocation is released; (2) The time of the deallocation;
/// and (3) The tctx associated with the deallocation.
/// jemalloc: prof_recent_alloc_reset
void profRecentAllocReset(ThreadState & tsd, Extent * edata)
{
    /// Check whether the recent allocation record still exists without trying to acquire the lock.
    if (edataProfRecentAllocGetNoLock(edata) == nullptr)
        return;

    ProfThreadContext * dalloc_tctx = profTctxCreate(tsd);
    /// In case dalloc_tctx is null, e.g. due to OOM, we will not record the deallocation time / tctx, which is
    /// handled later, after we check again when holding the lock.

    if (dalloc_tctx != nullptr)
    {
        dalloc_tctx->tdata->lock->lock(&tsd);
        incrementRecentCount(tsd, dalloc_tctx);
        dalloc_tctx->prepared = false;
        dalloc_tctx->tdata->lock->unlock(&tsd);
    }

    prof_recent_alloc_mtx.lock(&tsd);
    /// Check again after acquiring the lock.
    ProfRecent * recent = edataProfRecentAllocGet(tsd, edata);
    if (recent != nullptr)
    {
        JE_ASSERT(recent->dalloc_time.ns() == 0);
        JE_ASSERT(recent->dalloc_tctx == nullptr);
        if (dalloc_tctx != nullptr)
        {
            recent->dalloc_time.profUpdate();
            recent->dalloc_tctx = dalloc_tctx;
            dalloc_tctx = nullptr;
        }
        edataProfRecentAllocReset(tsd, edata, recent);
    }
    prof_recent_alloc_mtx.unlock(&tsd);

    if (dalloc_tctx != nullptr)
    {
        /// We lost the race - the allocation record was just gone.
        decrementRecentCount(tsd, dalloc_tctx);
    }
}

namespace
{

/// jemalloc: prof_recent_alloc_evict_edata
void profRecentAllocEvictEdata(ThreadState & tsd, ProfRecent * recent_alloc)
{
    prof_recent_alloc_mtx.assertOwner(&tsd);
    Extent * edata = profRecentAllocEdataGet(tsd, recent_alloc);
    if (edata != nullptr)
        edataProfRecentAllocReset(tsd, edata, recent_alloc);
}

/// jemalloc: prof_recent_alloc_is_empty
bool profRecentAllocIsEmpty(ThreadState & tsd)
{
    prof_recent_alloc_mtx.assertOwner(&tsd);
    if (prof_recent_alloc_list.empty())
    {
        JE_ASSERT(prof_recent_alloc_count == 0);
        return true;
    }
    JE_ASSERT(prof_recent_alloc_count > 0);
    return false;
}

/// jemalloc: prof_recent_alloc_assert_count
void profRecentAllocAssertCount(ThreadState & tsd)
{
    prof_recent_alloc_mtx.assertOwner(&tsd);
    if constexpr (!config::debug)
        return;
    ssize_t count = 0;
    prof_recent_alloc_list.forEach([&](ProfRecent *) { ++count; });
    JE_ASSERT(count == prof_recent_alloc_count);
    JE_ASSERT(profRecentAllocMaxGet(tsd) == -1 || count <= profRecentAllocMaxGet(tsd));
    (void)count;
}

}

/// jemalloc: prof_recent_alloc
void profRecentAlloc(ThreadState & tsd, Extent * edata, size_t size, size_t usize)
{
    JE_ASSERT(edata != nullptr);
    ProfThreadContext * tctx = edata->profTctx();

    tctx->tdata->lock->assertNotOwner(&tsd);
    prof_recent_alloc_mtx.lock(&tsd);
    profRecentAllocAssertCount(tsd);

    /// Reserve a new ProfRecent node if needed. If needed, we release the `prof_recent_alloc_mtx` lock and allocate.
    /// Then, rather than immediately checking for OOM, we regain the lock and try to make use of the reserve node if
    /// needed (see the comment in jemalloc's `prof_recent_alloc` for the six scenarios).
    ProfRecent * reserve = nullptr;
    ProfThreadContext * old_alloc_tctx;
    ProfThreadContext * old_dalloc_tctx;
    ProfRecent * tail;
    if (profRecentAllocMaxGet(tsd) == -1 || prof_recent_alloc_count < profRecentAllocMaxGet(tsd))
    {
        JE_ASSERT(profRecentAllocMaxGet(tsd) != 0);
        prof_recent_alloc_mtx.unlock(&tsd);
        reserve = profRecentAllocateNode(tsd);
        prof_recent_alloc_mtx.lock(&tsd);
        profRecentAllocAssertCount(tsd);
    }

    if (profRecentAllocMaxGet(tsd) == 0)
    {
        JE_ASSERT(profRecentAllocIsEmpty(tsd));
        goto label_rollback;
    }

    if (prof_recent_alloc_count == profRecentAllocMaxGet(tsd))
    {
        /// If upper limit is reached, rotate the head.
        JE_ASSERT(profRecentAllocMaxGet(tsd) != -1);
        JE_ASSERT(!profRecentAllocIsEmpty(tsd));
        ProfRecent * head = prof_recent_alloc_list.first();
        old_alloc_tctx = head->alloc_tctx;
        JE_ASSERT(old_alloc_tctx != nullptr);
        old_dalloc_tctx = head->dalloc_tctx;
        profRecentAllocEvictEdata(tsd, head);
        prof_recent_alloc_list.rotate();
    }
    else
    {
        /// Otherwise make use of the new node.
        JE_ASSERT(profRecentAllocMaxGet(tsd) == -1 || prof_recent_alloc_count < profRecentAllocMaxGet(tsd));
        if (reserve == nullptr)
            goto label_rollback;
        ProfRecentList::elementInit(reserve);
        prof_recent_alloc_list.tailInsert(reserve);
        reserve = nullptr;
        old_alloc_tctx = nullptr;
        old_dalloc_tctx = nullptr;
        ++prof_recent_alloc_count;
    }

    /// Fill content into the tail node.
    tail = prof_recent_alloc_list.last();
    JE_ASSERT(tail != nullptr);
    tail->size = size;
    tail->usize = usize;
    tail->alloc_time.copy(*edata->profAllocTime());
    tail->alloc_tctx = tctx;
    tail->dalloc_time.initZero();
    tail->dalloc_tctx = nullptr;
    edataProfRecentAllocSet(tsd, edata, tail);

    JE_ASSERT(!profRecentAllocIsEmpty(tsd));
    profRecentAllocAssertCount(tsd);
    prof_recent_alloc_mtx.unlock(&tsd);

    if (reserve != nullptr)
        profRecentFreeNode(tsd, reserve);

    /// Asynchronously handle the tctx of the old node, so that there's no simultaneous holdings of
    /// `prof_recent_alloc_mtx` and tdata->lock. In the worst case this may delay the tctx release but it's better
    /// than holding `prof_recent_alloc_mtx` for longer.
    if (old_alloc_tctx != nullptr)
        decrementRecentCount(tsd, old_alloc_tctx);
    if (old_dalloc_tctx != nullptr)
        decrementRecentCount(tsd, old_dalloc_tctx);
    return;

label_rollback:
    JE_ASSERT(edataProfRecentAllocGet(tsd, edata) == nullptr);
    profRecentAllocAssertCount(tsd);
    prof_recent_alloc_mtx.unlock(&tsd);
    if (reserve != nullptr)
        profRecentFreeNode(tsd, reserve);
    decrementRecentCount(tsd, tctx);
}

/// jemalloc: prof_recent_alloc_max_ctl_read
ssize_t profRecentAllocMaxCtlRead()
{
    /// Don't bother to acquire the lock.
    return profRecentAllocMaxGetNoLock();
}

namespace
{

/// jemalloc: prof_recent_alloc_restore_locked
void profRecentAllocRestoreLocked(ThreadState & tsd, ProfRecentList * to_delete)
{
    prof_recent_alloc_mtx.assertOwner(&tsd);
    ssize_t max = profRecentAllocMaxGet(tsd);
    if (max == -1 || prof_recent_alloc_count <= max)
    {
        /// Easy case - no need to alter the list.
        to_delete->init();
        profRecentAllocAssertCount(tsd);
        return;
    }

    ProfRecent * node = nullptr;
    for (node = prof_recent_alloc_list.first(); node != nullptr; node = prof_recent_alloc_list.next(node))
    {
        if (prof_recent_alloc_count == max)
            break;
        profRecentAllocEvictEdata(tsd, node);
        --prof_recent_alloc_count;
    }
    JE_ASSERT(prof_recent_alloc_count == max);

    to_delete->moveFrom(prof_recent_alloc_list);
    if (max == 0)
    {
        JE_ASSERT(node == nullptr);
    }
    else
    {
        JE_ASSERT(node != nullptr);
        to_delete->split(node, prof_recent_alloc_list);
    }
    JE_ASSERT(!to_delete->empty());
    profRecentAllocAssertCount(tsd);
}

/// jemalloc: prof_recent_alloc_async_cleanup
void profRecentAllocAsyncCleanup(ThreadState & tsd, ProfRecentList * to_delete)
{
    prof_recent_dump_mtx.assertNotOwner(&tsd);
    prof_recent_alloc_mtx.assertNotOwner(&tsd);
    while (!to_delete->empty())
    {
        ProfRecent * node = to_delete->first();
        to_delete->remove(node);
        decrementRecentCount(tsd, node->alloc_tctx);
        if (node->dalloc_tctx != nullptr)
            decrementRecentCount(tsd, node->dalloc_tctx);
        profRecentFreeNode(tsd, node);
    }
}

}

/// jemalloc: prof_recent_alloc_max_ctl_write
ssize_t profRecentAllocMaxCtlWrite(ThreadState & tsd, ssize_t max)
{
    JE_ASSERT(max >= -1);
    prof_recent_alloc_mtx.lock(&tsd);
    profRecentAllocAssertCount(tsd);
    const ssize_t old_max = profRecentAllocMaxUpdate(tsd, max);
    ProfRecentList to_delete;
    profRecentAllocRestoreLocked(tsd, &to_delete);
    prof_recent_alloc_mtx.unlock(&tsd);
    profRecentAllocAsyncCleanup(tsd, &to_delete);
    return old_max;
}

namespace
{

/// jemalloc: prof_recent_alloc_dump_bt
void profRecentAllocDumpBt(Emitter & emitter, ProfThreadContext * tctx)
{
    char bt_buf[2 * sizeof(intptr_t) + 3];
    const char * s = bt_buf;
    JE_ASSERT(tctx != nullptr);
    ProfBacktrace * bt = &tctx->gctx->bt;
    for (size_t i = 0; i < bt->len; ++i)
    {
        format(bt_buf, sizeof(bt_buf), "%p", bt->vec[i]);
        emitter.jsonValue(EmitterType::String, &s);
    }
}

/// jemalloc: prof_recent_alloc_dump_node
void profRecentAllocDumpNode(Emitter & emitter, ProfRecent * node)
{
    emitter.jsonObjectBegin();

    emitter.jsonKv("size", EmitterType::Size, &node->size);
    emitter.jsonKv("usize", EmitterType::Size, &node->usize);
    bool released = profRecentAllocEdataGetNoLock(node) == nullptr;
    emitter.jsonKv("released", EmitterType::Bool, &released);

    emitter.jsonKv("alloc_thread_uid", EmitterType::Uint64, &node->alloc_tctx->thr_uid);
    ProfThreadData * alloc_tdata = node->alloc_tctx->tdata;
    JE_ASSERT(alloc_tdata != nullptr);
    if (!profThreadNameEmpty(alloc_tdata))
    {
        const char * thread_name = alloc_tdata->thread_name;
        emitter.jsonKv("alloc_thread_name", EmitterType::String, &thread_name);
    }
    uint64_t alloc_time_ns = node->alloc_time.ns();
    emitter.jsonKv("alloc_time", EmitterType::Uint64, &alloc_time_ns);
    emitter.jsonArrayKvBegin("alloc_trace");
    profRecentAllocDumpBt(emitter, node->alloc_tctx);
    emitter.jsonArrayEnd();

    if (released && node->dalloc_tctx != nullptr)
    {
        emitter.jsonKv("dalloc_thread_uid", EmitterType::Uint64, &node->dalloc_tctx->thr_uid);
        ProfThreadData * dalloc_tdata = node->dalloc_tctx->tdata;
        JE_ASSERT(dalloc_tdata != nullptr);
        if (!profThreadNameEmpty(dalloc_tdata))
        {
            const char * thread_name = dalloc_tdata->thread_name;
            emitter.jsonKv("dalloc_thread_name", EmitterType::String, &thread_name);
        }
        JE_ASSERT(node->dalloc_time.ns() != 0);
        uint64_t dalloc_time_ns = node->dalloc_time.ns();
        emitter.jsonKv("dalloc_time", EmitterType::Uint64, &dalloc_time_ns);
        emitter.jsonArrayKvBegin("dalloc_trace");
        profRecentAllocDumpBt(emitter, node->dalloc_tctx);
        emitter.jsonArrayEnd();
    }

    emitter.jsonObjectEnd();
}

/// jemalloc: PROF_RECENT_PRINT_BUFSIZE
constexpr size_t PROF_RECENT_PRINT_BUFSIZE = 65536;

/// jemalloc: buf_writer_allocate_internal_buf / buf_writer_free_internal_buf (arena 0, internal)
void * profRecentBufferAllocate(ThreadState * tsdn, size_t size)
{
    return profAllocArena0(*tsdn, size, false);
}

void profRecentBufferDeallocate(ThreadState * tsdn, void * ptr)
{
    profIdalloc(tsdn, ptr);
}

constexpr BufferAllocator prof_recent_buffer_allocator = {profRecentBufferAllocate, profRecentBufferDeallocate};

}

/// jemalloc: prof_recent_alloc_dump
JE_NOINLINE void profRecentAllocDump(ThreadState & tsd, WriteCallback * write_cb, void * cbopaque)
{
    prof_recent_dump_mtx.lock(&tsd);
    BufferedWriter buf_writer;
    buf_writer.init(&tsd, write_cb, cbopaque, nullptr, PROF_RECENT_PRINT_BUFSIZE, &prof_recent_buffer_allocator);
    Emitter emitter(EmitterOutput::JSONCompact, BufferedWriter::callback, &buf_writer);
    ProfRecentList temp_list;

    prof_recent_alloc_mtx.lock(&tsd);
    profRecentAllocAssertCount(tsd);
    ssize_t dump_max = profRecentAllocMaxGet(tsd);
    temp_list.moveFrom(prof_recent_alloc_list);
    ssize_t dump_count = prof_recent_alloc_count;
    prof_recent_alloc_count = 0;
    profRecentAllocAssertCount(tsd);
    prof_recent_alloc_mtx.unlock(&tsd);

    emitter.begin();
    uint64_t sample_interval = uint64_t(1U) << lg_prof_sample;
    emitter.jsonKv("sample_interval", EmitterType::Uint64, &sample_interval);
    emitter.jsonKv("recent_alloc_max", EmitterType::Ssize, &dump_max);
    emitter.jsonArrayKvBegin("recent_alloc");
    temp_list.forEach([&](ProfRecent * node) { profRecentAllocDumpNode(emitter, node); });
    emitter.jsonArrayEnd();
    emitter.end();

    prof_recent_alloc_mtx.lock(&tsd);
    profRecentAllocAssertCount(tsd);
    temp_list.concat(prof_recent_alloc_list);
    prof_recent_alloc_list.moveFrom(temp_list);
    prof_recent_alloc_count += dump_count;
    profRecentAllocRestoreLocked(tsd, &temp_list);
    prof_recent_alloc_mtx.unlock(&tsd);

    buf_writer.terminate(&tsd);
    prof_recent_dump_mtx.unlock(&tsd);

    profRecentAllocAsyncCleanup(tsd, &temp_list);
}

/// jemalloc: prof_recent_init
bool profRecentInit()
{
    profRecentAllocMaxInit();

    if (prof_recent_alloc_mtx.init("prof_recent_alloc", MutexRank::PROF_RECENT_ALLOC))
        return true;

    if (prof_recent_dump_mtx.init("prof_recent_dump", MutexRank::PROF_RECENT_DUMP))
        return true;

    prof_recent_alloc_list.init();

    return false;
}

}
