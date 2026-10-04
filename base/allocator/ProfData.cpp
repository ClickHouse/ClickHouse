/// The core profiling data structures (jemalloc: `prof_data.c`).
///
/// Conceptually, profiling data can be imagined as a table with three columns: thread, stack trace, and current
/// allocation size (with `prof_accum` there's one additional column which is the cumulative allocation size).
///
/// Implementation wise, each thread maintains a hash recording the stack trace to allocation size correspondences,
/// which are basically the individual rows in the table. In addition, two global "indices" are built to make data
/// aggregation efficient (for dumping): `bt2gctx` and `tdatas`, which are basically the "grouped by stack trace" and
/// "grouped by thread" views of the same table, respectively. Note that the allocation size is only aggregated to the
/// two indices at dumping time, so as to optimize for performance.

#include <allocator/Prof.h>

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

constinit Mutex bt2gctx_mtx;
constinit Mutex tdatas_mtx;
constinit Mutex prof_dump_mtx;

/// Table of mutexes that are shared among gctx's. These are leaf locks, so there is no problem with using them for
/// more than one gctx at the same time. The primary motivation for this sharing though is that gctx's are ephemeral,
/// and destroying mutexes causes complications for systems that allocate when creating/destroying mutexes.
constinit Mutex * gctx_locks = nullptr;

/// Table of mutexes that are shared among tdata's. No operations require holding multiple tdata locks, so there is no
/// problem with using them for more than one tdata at the same time, even though a gctx lock may be acquired while
/// holding a tdata lock.
constinit Mutex * tdata_locks = nullptr;

constinit size_t prof_unbiased_sz[SC_NSIZES] = {};
constinit size_t prof_shifted_unbiased_cnt[SC_NSIZES] = {};

namespace
{

/// Atomic counter. jemalloc: cum_gctxs
constinit std::atomic<unsigned> cum_gctxs{0};

/// Global hash of (ProfBacktrace *) -> (ProfGlobalContext *). This is the master data structure that knows about all
/// backtraces currently captured. jemalloc: bt2gctx
constinit ProfCuckooHash bt2gctx{};

/// Tree of all extant ProfThreadData structures, regardless of state, {attached,detached,expired}. jemalloc: tdatas
constinit ProfTdataTree tdatas;

}

/// --- Comparators ---------------------------------------------------------------------------------------------------

/// jemalloc: prof_tctx_comp
int profTctxCompare(const ProfThreadContext * a, const ProfThreadContext * b)
{
    uint64_t a_thr_uid = a->thr_uid;
    uint64_t b_thr_uid = b->thr_uid;
    int ret = (a_thr_uid > b_thr_uid) - (a_thr_uid < b_thr_uid);
    if (ret == 0)
    {
        uint64_t a_thr_discrim = a->thr_discrim;
        uint64_t b_thr_discrim = b->thr_discrim;
        ret = (a_thr_discrim > b_thr_discrim) - (a_thr_discrim < b_thr_discrim);
        if (ret == 0)
        {
            uint64_t a_tctx_uid = a->tctx_uid;
            uint64_t b_tctx_uid = b->tctx_uid;
            ret = (a_tctx_uid > b_tctx_uid) - (a_tctx_uid < b_tctx_uid);
        }
    }
    return ret;
}

/// Note: `memcmp` on the raw bytes of the program counters (on little-endian machines this is not the numeric
/// order of the addresses); the order of the `@` blocks in heap dumps depends on it.
/// jemalloc: prof_gctx_comp
int profGctxCompare(const ProfGlobalContext * a, const ProfGlobalContext * b)
{
    unsigned a_len = a->bt.len;
    unsigned b_len = b->bt.len;
    unsigned comp_len = (a_len < b_len) ? a_len : b_len;
    int ret = memcmp(a->bt.vec, b->bt.vec, comp_len * sizeof(void *));
    if (ret == 0)
        ret = (a_len > b_len) - (a_len < b_len);
    return ret;
}

/// jemalloc: prof_tdata_comp
int profTdataCompare(const ProfThreadData * a, const ProfThreadData * b)
{
    uint64_t a_uid = a->thr_uid;
    uint64_t b_uid = b->thr_uid;
    int ret = ((a_uid > b_uid) - (a_uid < b_uid));
    if (ret == 0)
    {
        uint64_t a_discrim = a->thr_discrim;
        uint64_t b_discrim = b->thr_discrim;
        ret = ((a_discrim > b_discrim) - (a_discrim < b_discrim));
    }
    return ret;
}

/// --- Internal allocations ------------------------------------------------------------------------------------------

void * profAllocArena0(ThreadState & tsd, size_t size, bool init_if_missing)
{
    return iallocztm(
        &tsd, size, sz::sizeToIndex(size), false, nullptr, true, arenaGet(init_if_missing ? nullptr : &tsd, 0, init_if_missing), true);
}

void * profAllocIchoose(ThreadState & tsd, size_t size)
{
    return iallocztm(&tsd, size, sz::sizeToIndex(size), false, nullptr, true, arenaIchoose(tsd, nullptr), true);
}

void profIdalloc(ThreadState * tsdn, void * ptr)
{
    idalloctm(tsdn, ptr, nullptr, nullptr, true, true);
}

void * ProfCuckooHashAllocator::allocate(ThreadState & tsd, size_t usize, size_t alignment)
{
    return ipallocztm(&tsd, usize, alignment, true, nullptr, true, arenaIchoose(tsd, nullptr));
}

void ProfCuckooHashAllocator::deallocate(ThreadState & tsd, void * ptr)
{
    idalloctm(&tsd, ptr, nullptr, nullptr, true, true);
}

/// --- Global contexts -----------------------------------------------------------------------------------------------

namespace
{

/// NB: `fetch_add` returns the old value, so the first gctx gets the lock 1023, then 0, 1, ...
/// jemalloc: prof_gctx_mutex_choose
Mutex * profGctxMutexChoose()
{
    unsigned ngctxs = cum_gctxs.fetch_add(1, std::memory_order_relaxed);
    return &gctx_locks[(ngctxs - 1) % PROF_NCTX_LOCKS];
}

/// jemalloc: prof_tdata_mutex_choose
Mutex * profTdataMutexChoose(uint64_t thr_uid)
{
    return &tdata_locks[thr_uid % PROF_NTDATA_LOCKS];
}

/// jemalloc: prof_enter
void profEnter(ThreadState & tsd, ProfThreadData * tdata)
{
    JE_ASSERT(tdata == profTdataGet(tsd, false));

    if (tdata != nullptr)
    {
        JE_ASSERT(!tdata->enq);
        tdata->enq = true;
    }

    bt2gctx_mtx.lock(&tsd);
}

/// jemalloc: prof_leave
void profLeave(ThreadState & tsd, ProfThreadData * tdata)
{
    JE_ASSERT(tdata == profTdataGet(tsd, false));

    bt2gctx_mtx.unlock(&tsd);

    if (tdata != nullptr)
    {
        JE_ASSERT(tdata->enq);
        tdata->enq = false;
        bool idump = tdata->enq_idump;
        tdata->enq_idump = false;
        bool gdump = tdata->enq_gdump;
        tdata->enq_gdump = false;

        if (idump)
            profIdump(&tsd);
        if (gdump)
            profGdump(&tsd);
    }
}

/// jemalloc: prof_gctx_create
ProfGlobalContext * profGctxCreate(ThreadState & tsd, ProfBacktrace * bt)
{
    /// Create a single allocation that has space for vec of length bt->len.
    size_t size = offsetof(ProfGlobalContext, vec) + (bt->len * sizeof(void *));
    auto * gctx = static_cast<ProfGlobalContext *>(profAllocArena0(tsd, size, true));
    if (gctx == nullptr)
        return nullptr;
    gctx->lock = profGctxMutexChoose();
    /// Set nlimbo to 1, in order to avoid a race condition with `profTctxDestroy` / `profGctxTryDestroy`.
    gctx->nlimbo = 1;
    gctx->tctxs.init();
    gctx->frag_objs.init();
    /// Duplicate bt.
    memcpy(static_cast<void *>(gctx->vec), bt->vec, bt->len * sizeof(void *));
    gctx->bt.vec = gctx->vec;
    gctx->bt.len = bt->len;
    return gctx;
}

/// jemalloc: prof_gctx_try_destroy
void profGctxTryDestroy(ThreadState & tsd, ProfThreadData * tdata_self, ProfGlobalContext * gctx)
{
    /// Check that gctx is still unused by any thread cache before destroying it. `profLookup` increments
    /// gctx->nlimbo in order to avoid a race condition with this function, as does `profTctxDestroy` in order to
    /// avoid a race between the main body of `profTctxDestroy` and entry into this function.
    profEnter(tsd, tdata_self);
    gctx->lock->lock(&tsd);
    JE_ASSERT(gctx->nlimbo != 0);
    if (gctx->tctxs.empty() && gctx->nlimbo == 1)
    {
        /// No live tctx implies no live sampled allocation attributed to this gctx (each such allocation holds a
        /// curobjs count on its tctx until untracked).
        JE_ASSERT(gctx->frag_objs.empty());
        /// Remove gctx from bt2gctx.
        if (bt2gctx.remove(tsd, &gctx->bt, nullptr, nullptr))
            JE_NOT_REACHED();
        profLeave(tsd, tdata_self);
        /// Destroy gctx.
        gctx->lock->unlock(&tsd);
        profIdalloc(&tsd, gctx);
    }
    else
    {
        /// Compensate for increment in `profTctxDestroy` or `profLookup`.
        --gctx->nlimbo;
        gctx->lock->unlock(&tsd);
        profLeave(tsd, tdata_self);
    }
}

/// jemalloc: prof_gctx_should_destroy
bool profGctxShouldDestroy(ProfGlobalContext * gctx)
{
    if (opt.prof_accum)
        return false;
    if (!gctx->tctxs.empty())
        return false;
    if (gctx->nlimbo != 0)
        return false;
    return true;
}

/// jemalloc: prof_lookup_global
bool profLookupGlobal(
    ThreadState & tsd, ProfBacktrace * bt, ProfThreadData * tdata, void ** p_btkey, ProfGlobalContext ** p_gctx, bool * p_new_gctx)
{
    void * gctx_v = nullptr;
    void * btkey_v = nullptr;
    ProfGlobalContext * tgctx;
    bool new_gctx;

    profEnter(tsd, tdata);
    if (bt2gctx.search(bt, &btkey_v, &gctx_v))
    {
        /// bt has never been seen before. Insert it.
        profLeave(tsd, tdata);
        tgctx = profGctxCreate(tsd, bt);
        if (tgctx == nullptr)
            return true;
        profEnter(tsd, tdata);
        if (bt2gctx.search(bt, &btkey_v, &gctx_v))
        {
            gctx_v = tgctx;
            btkey_v = &tgctx->bt;
            if (bt2gctx.insert(tsd, btkey_v, gctx_v))
            {
                /// OOM.
                profLeave(tsd, tdata);
                profIdalloc(&tsd, gctx_v);
                return true;
            }
            new_gctx = true;
        }
        else
        {
            new_gctx = false;
        }
    }
    else
    {
        tgctx = nullptr;
        new_gctx = false;
    }

    auto * gctx = static_cast<ProfGlobalContext *>(gctx_v);
    if (!new_gctx)
    {
        /// Increment nlimbo, in order to avoid a race condition with `profTctxDestroy` / `profGctxTryDestroy`.
        gctx->lock->lock(&tsd);
        ++gctx->nlimbo;
        gctx->lock->unlock(&tsd);
        new_gctx = false;

        if (tgctx != nullptr)
        {
            /// Lost race to insert.
            profIdalloc(&tsd, tgctx);
        }
    }
    profLeave(tsd, tdata);

    *p_btkey = btkey_v;
    *p_gctx = gctx;
    *p_new_gctx = new_gctx;
    return false;
}

}

/// jemalloc: prof_data_init
bool profDataInit(ThreadState & tsd)
{
    tdatas.init();
    return bt2gctx.init(tsd, PROF_CKH_MINITEMS, profBtHash, profBtKeycomp);
}

/// Track/untrack a live sampled allocation on its gctx's `frag_objs` list, so that heap dumps can enumerate live
/// sampled allocations (fragmentation profiling). Called with no locks held: track from `profMallocSampleObject`
/// while `tctx->prepared` still pins the tctx; untrack from the `profInfoGetAndResetRecent` sever point, which on
/// every deallocation path precedes the curobjs decrement in `profFreeSampledObject` -- so in both cases the tctx
/// (hence gctx) is guaranteed alive.
/// jemalloc: prof_frag_track
void profFragTrack(ThreadState & tsd, Extent * edata, ProfThreadContext * tctx)
{
    ProfGlobalContext * gctx = tctx->gctx;

    MutexLock lock(&tsd, *gctx->lock);
    JE_ASSERT(!edata->profFragTracked());
    gctx->frag_objs.append(edata);
    edata->setProfFragTracked(true);
}

/// jemalloc: prof_frag_untrack
void profFragUntrack(ThreadState & tsd, Extent * edata, ProfThreadContext * tctx)
{
    ProfGlobalContext * gctx = tctx->gctx;

    MutexLock lock(&tsd, *gctx->lock);
    /// Not necessarily tracked: if an in-place reallocation severed the allocation but then failed (OOM), the
    /// allocation stays live yet untracked, and its eventual deallocation severs it a second time.
    if (edata->profFragTracked())
    {
        gctx->frag_objs.remove(edata);
        edata->setProfFragTracked(false);
    }
}

/// jemalloc: prof_lookup
ProfThreadContext * profLookup(ThreadState & tsd, ProfBacktrace * bt)
{
    ProfThreadData * tdata = profTdataGet(tsd, false);
    JE_ASSERT(tdata != nullptr);

    void * ret_v = nullptr;
    tdata->lock->lock(&tsd);
    bool not_found = tdata->bt2tctx.search(bt, nullptr, &ret_v);
    auto * ret = static_cast<ProfThreadContext *>(ret_v);
    if (!not_found) /// Note double negative!
        ret->prepared = true;
    tdata->lock->unlock(&tsd);
    if (not_found)
    {
        void * btkey;
        ProfGlobalContext * gctx;
        bool new_gctx;

        /// This thread's cache lacks bt. Look for it in the global cache.
        if (profLookupGlobal(tsd, bt, tdata, &btkey, &gctx, &new_gctx))
            return nullptr;

        /// Link a ProfThreadContext into gctx for this thread.
        ret = static_cast<ProfThreadContext *>(profAllocIchoose(tsd, sizeof(ProfThreadContext)));
        if (ret == nullptr)
        {
            if (new_gctx)
                profGctxTryDestroy(tsd, tdata, gctx);
            return nullptr;
        }
        ret->tdata = tdata;
        ret->thr_uid = tdata->thr_uid;
        ret->thr_discrim = tdata->thr_discrim;
        ret->recent_count = 0;
        memset(&ret->cnts, 0, sizeof(ProfCounters));
        ret->gctx = gctx;
        ret->tctx_uid = tdata->tctx_uid_next++;
        ret->prepared = true;
        ret->state = prof_tctx_state_initializing;
        tdata->lock->lock(&tsd);
        bool error = tdata->bt2tctx.insert(tsd, btkey, ret);
        tdata->lock->unlock(&tsd);
        if (error)
        {
            if (new_gctx)
                profGctxTryDestroy(tsd, tdata, gctx);
            profIdalloc(&tsd, ret);
            return nullptr;
        }
        gctx->lock->lock(&tsd);
        ret->state = prof_tctx_state_nominal;
        gctx->tctxs.insert(ret);
        --gctx->nlimbo;
        gctx->lock->unlock(&tsd);
    }

    return ret;
}

/// Used in unit tests. jemalloc: prof_tdata_count
size_t profTdataCount()
{
    size_t tdata_count = 0;
    ThreadState * tsdn = ThreadState::tsdnFetch();
    MutexLock lock(tsdn, tdatas_mtx);
    tdatas.iter(
        nullptr,
        [&](ProfThreadData *) -> ProfThreadData *
        {
            ++tdata_count;
            return nullptr;
        });
    return tdata_count;
}

/// Used in unit tests. jemalloc: prof_bt_count
size_t profBtCount()
{
    ThreadState & tsd = ThreadState::fetch();
    ProfThreadData * tdata = profTdataGet(tsd, false);
    if (tdata == nullptr)
        return 0;

    MutexLock lock(&tsd, bt2gctx_mtx);
    return bt2gctx.count();
}

namespace
{

/// jemalloc: prof_thread_name_write_tdata
void profThreadNameWriteTdata(ProfThreadData * tdata, const char * thread_name)
{
    strncpy(tdata->thread_name, thread_name, PROF_THREAD_NAME_MAX_LEN);
    tdata->thread_name[PROF_THREAD_NAME_MAX_LEN - 1] = '\0';
}

}

/// jemalloc: prof_thread_name_set_impl
int profThreadNameSetImpl(ThreadState & tsd, const char * thread_name)
{
    JE_ASSERT(tsd.reentrancyLevel() == 0);
    JE_ASSERT(thread_name != nullptr);

    for (unsigned i = 0; thread_name[i] != '\0'; ++i)
    {
        char c = thread_name[i];
        if (!isgraph(c) && !isblank(c))
            return EINVAL;
    }

    ProfThreadData * tdata = profTdataGet(tsd, true);
    if (tdata == nullptr)
        return ENOMEM;

    profThreadNameWriteTdata(tdata, thread_name);

    return 0;
}

/// --- Unbiasing -----------------------------------------------------------------------------------------------------

namespace
{

/// jemalloc: prof_dump_printf
JE_FORMAT_PRINTF(3, 4)
void profDumpPrintf(WriteCallback * prof_dump_write, void * cbopaque, const char * fmt, ...)
{
    va_list ap;
    char buf[PROF_PRINTF_BUFSIZE];

    va_start(ap, fmt);
    formatV(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    prof_dump_write(cbopaque, buf);
}

/// Casting a double to a uint64_t may not necessarily be in range; this can be UB. UINT64_MAX + 1 is exactly
/// representable as a double; writing this as !(a < b) instead of (a >= b) means that we're NaN-safe.
/// jemalloc: prof_double_uint64_cast
uint64_t profDoubleUint64Cast(double d)
{
    double rounded = round(d);
    if (!(rounded < static_cast<double>(UINT64_MAX)))
        return UINT64_MAX;
    return static_cast<uint64_t>(rounded);
}

}

/// jemalloc: prof_unbias_map_init
void profUnbiasMapInit()
{
    for (szind_t i = 0; i < SC_NSIZES; ++i)
    {
        /// With large size classes disabled, the unbiased calculation here is not as accurate as it was because usize
        /// now changes in a finer grain while the unbiased_sz is still calculated using the old way.
        double sz = static_cast<double>(sz::indexToSizeUnsafe(i));
        double rate = static_cast<double>(size_t(1) << lg_prof_sample);
        double div_val = 1.0 - exp(-sz / rate);
        double unbiased_sz = sz / div_val;
        /// The "true" right value for the unbiased count is 1.0/(1 - exp(-sz/rate)). The counts are kept as integers;
        /// to limit the rounding error, they are multiplied by the size of the smallest allocation.
        double cnt_shift = static_cast<double>(size_t(1) << SC_LG_TINY_MIN);
        double shifted_unbiased_cnt = cnt_shift / div_val;
        prof_unbiased_sz[i] = static_cast<size_t>(round(unbiased_sz));
        prof_shifted_unbiased_cnt[i] = static_cast<size_t>(round(shifted_unbiased_cnt));
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
void profDoUnbias(uint64_t c_out_shifted_i, uint64_t s_out_i, uint64_t * r_c_in, uint64_t * r_s_in)
{
    if (c_out_shifted_i == 0 || s_out_i == 0)
    {
        *r_c_in = 0;
        *r_s_in = 0;
        return;
    }
    /// c_out is taken in a shifted form (see `profUnbiasMapInit`).
    double c_out = static_cast<double>(c_out_shifted_i) / static_cast<double>(size_t(1) << SC_LG_TINY_MIN);
    double s_out = static_cast<double>(s_out_i);
    double R = static_cast<double>(size_t(1) << lg_prof_sample);

    double x = s_out / c_out;
    double y = s_out * (1.0 - exp(-x / R));

    double c_in = y / x;
    double s_in = y;

    *r_c_in = profDoubleUint64Cast(c_in);
    *r_s_in = profDoubleUint64Cast(s_in);
}

/// jemalloc: prof_dump_print_cnts
void profDumpPrintCnts(WriteCallback * prof_dump_write, void * cbopaque, const ProfCounters * cnts)
{
    uint64_t curobjs;
    uint64_t curbytes;
    uint64_t accumobjs;
    uint64_t accumbytes;
    if (opt.prof_unbias)
    {
        profDoUnbias(cnts->curobjs_shifted_unbiased, cnts->curbytes_unbiased, &curobjs, &curbytes);
        profDoUnbias(cnts->accumobjs_shifted_unbiased, cnts->accumbytes_unbiased, &accumobjs, &accumbytes);
    }
    else
    {
        curobjs = cnts->curobjs;
        curbytes = cnts->curbytes;
        accumobjs = cnts->accumobjs;
        accumbytes = cnts->accumbytes;
    }
    profDumpPrintf(
        prof_dump_write,
        cbopaque,
        "%llu: %llu [%llu: %llu]",
        static_cast<unsigned long long>(curobjs),
        static_cast<unsigned long long>(curbytes),
        static_cast<unsigned long long>(accumobjs),
        static_cast<unsigned long long>(accumbytes));
}

/// --- Dump aggregation ------------------------------------------------------------------------------------------------

/// Adds the cur* counters of `src` to `dst` (and the accum* counters if `opt.prof_accum`).
void profCntsMerge(ProfCounters & dst, const ProfCounters & src)
{
    dst.curobjs += src.curobjs;
    dst.curobjs_shifted_unbiased += src.curobjs_shifted_unbiased;
    dst.curbytes += src.curbytes;
    dst.curbytes_unbiased += src.curbytes_unbiased;
    if (opt.prof_accum)
    {
        dst.accumobjs += src.accumobjs;
        dst.accumobjs_shifted_unbiased += src.accumobjs_shifted_unbiased;
        dst.accumbytes += src.accumbytes;
        dst.accumbytes_unbiased += src.accumbytes_unbiased;
    }
}

/// jemalloc: prof_tctx_merge_tdata
void profTctxMergeTdata(ThreadState * tsdn, ProfThreadContext * tctx, ProfThreadData * tdata)
{
    tctx->tdata->lock->assertOwner(tsdn);

    tctx->gctx->lock->lock(tsdn);

    switch (tctx->state)
    {
        case prof_tctx_state_initializing:
            tctx->gctx->lock->unlock(tsdn);
            return;
        case prof_tctx_state_nominal:
            tctx->state = prof_tctx_state_dumping;
            tctx->gctx->lock->unlock(tsdn);

            memcpy(&tctx->dump_cnts, &tctx->cnts, sizeof(ProfCounters));

            profCntsMerge(tdata->cnt_summed, tctx->dump_cnts);
            break;
        case prof_tctx_state_dumping:
        case prof_tctx_state_purgatory:
            JE_NOT_REACHED();
    }
}

/// jemalloc: prof_tctx_merge_gctx
void profTctxMergeGctx(ThreadState * tsdn, ProfThreadContext * tctx, ProfGlobalContext * gctx)
{
    gctx->lock->assertOwner(tsdn);
    profCntsMerge(gctx->cnt_summed, tctx->dump_cnts);
}

/// jemalloc: prof_dump_iter_arg_t
struct ProfDumpIterArg
{
    ThreadState * tsdn;
    WriteCallback * prof_dump_write;
    void * cbopaque;
    /// Dump-time timestamp for computing live sampled allocation ages.
    NsTime now;
};

/// jemalloc: prof_tctx_dump_iter
void profTctxDumpIter(ProfDumpIterArg * arg, ProfThreadContext * tctx)
{
    tctx->gctx->lock->assertOwner(arg->tsdn);

    switch (tctx->state)
    {
        case prof_tctx_state_initializing:
        case prof_tctx_state_nominal:
            /// Not captured by this dump.
            break;
        case prof_tctx_state_dumping:
        case prof_tctx_state_purgatory:
            profDumpPrintf(arg->prof_dump_write, arg->cbopaque, "  t%llu: ", static_cast<unsigned long long>(tctx->thr_uid));
            profDumpPrintCnts(arg->prof_dump_write, arg->cbopaque, &tctx->dump_cnts);
            arg->prof_dump_write(arg->cbopaque, "\n");
            break;
    }
}

/// jemalloc: prof_tctx_finish_iter
ProfThreadContext * profTctxFinishIter(ThreadState * tsdn, ProfThreadContext * tctx)
{
    tctx->gctx->lock->assertOwner(tsdn);

    switch (tctx->state)
    {
        case prof_tctx_state_nominal:
            /// New since dumping started; ignore.
            break;
        case prof_tctx_state_dumping:
            tctx->state = prof_tctx_state_nominal;
            break;
        case prof_tctx_state_purgatory:
            return tctx;
        case prof_tctx_state_initializing:
            JE_NOT_REACHED();
    }
    return nullptr;
}

/// jemalloc: prof_dump_gctx_prep
void profDumpGctxPrep(ThreadState * tsdn, ProfGlobalContext * gctx, ProfGctxTree * gctxs)
{
    MutexLock lock(tsdn, *gctx->lock);

    /// Increment nlimbo so that gctx won't go away before dump. Additionally, link gctx into the dump list so that it
    /// is included in the second pass of the dump.
    ++gctx->nlimbo;
    gctxs->insert(gctx);

    memset(&gctx->cnt_summed, 0, sizeof(ProfCounters));
}

/// jemalloc: prof_gctx_merge_iter
void profGctxMergeIter(ThreadState * tsdn, ProfGlobalContext * gctx, size_t * leak_ngctx)
{
    MutexLock lock(tsdn, *gctx->lock);
    gctx->tctxs.iter(
        nullptr,
        [&](ProfThreadContext * tctx) -> ProfThreadContext *
        {
            /// jemalloc: prof_tctx_merge_iter
            tctx->gctx->lock->assertOwner(tsdn);
            switch (tctx->state)
            {
                case prof_tctx_state_nominal:
                    /// New since dumping started; ignore.
                    break;
                case prof_tctx_state_dumping:
                case prof_tctx_state_purgatory:
                    profTctxMergeGctx(tsdn, tctx, tctx->gctx);
                    break;
                case prof_tctx_state_initializing:
                    JE_NOT_REACHED();
            }
            return nullptr;
        });
    if (gctx->cnt_summed.curobjs != 0)
        ++*leak_ngctx;
}

/// jemalloc: prof_gctx_finish
void profGctxFinish(ThreadState & tsd, ProfGctxTree * gctxs)
{
    ProfThreadData * tdata = profTdataGet(tsd, false);
    ProfGlobalContext * gctx;

    /// Standard tree iteration won't work here, because as soon as we decrement gctx->nlimbo and unlock gctx, another
    /// thread can concurrently destroy it, which will corrupt the tree. Therefore, tear down the tree one node at a
    /// time during iteration.
    while ((gctx = gctxs->first()) != nullptr)
    {
        gctxs->remove(gctx);
        gctx->lock->lock(&tsd);
        {
            ProfThreadContext * next = nullptr;
            do
            {
                ProfThreadContext * to_destroy = gctx->tctxs.iter(
                    next, [&](ProfThreadContext * tctx) -> ProfThreadContext * { return profTctxFinishIter(&tsd, tctx); });
                if (to_destroy != nullptr)
                {
                    next = gctx->tctxs.next(to_destroy);
                    gctx->tctxs.remove(to_destroy);
                    profIdalloc(&tsd, to_destroy);
                }
                else
                {
                    next = nullptr;
                }
            } while (next != nullptr);
        }
        --gctx->nlimbo;
        if (profGctxShouldDestroy(gctx))
        {
            ++gctx->nlimbo;
            gctx->lock->unlock(&tsd);
            profGctxTryDestroy(tsd, tdata, gctx);
        }
        else
        {
            gctx->lock->unlock(&tsd);
        }
    }
}

/// jemalloc: prof_tdata_merge_iter
void profTdataMergeIter(ThreadState * tsdn, ProfThreadData * tdata, ProfCounters * cnt_all)
{
    MutexLock lock(tsdn, *tdata->lock);
    if (!tdata->expired)
    {
        tdata->dumping = true;
        memset(&tdata->cnt_summed, 0, sizeof(ProfCounters));
        void * tctx_v = nullptr;
        for (size_t tabind = 0; !tdata->bt2tctx.iter(&tabind, nullptr, &tctx_v);)
            profTctxMergeTdata(tsdn, static_cast<ProfThreadContext *>(tctx_v), tdata);

        profCntsMerge(*cnt_all, tdata->cnt_summed);
    }
    else
    {
        tdata->dumping = false;
    }
}

/// jemalloc: prof_tdata_dump_iter
void profTdataDumpIter(ProfDumpIterArg * arg, ProfThreadData * tdata)
{
    if (!tdata->dumping)
        return;

    profDumpPrintf(arg->prof_dump_write, arg->cbopaque, "  t%llu: ", static_cast<unsigned long long>(tdata->thr_uid));
    profDumpPrintCnts(arg->prof_dump_write, arg->cbopaque, &tdata->cnt_summed);
    if (!profThreadNameEmpty(tdata))
    {
        arg->prof_dump_write(arg->cbopaque, " ");
        arg->prof_dump_write(arg->cbopaque, tdata->thread_name);
    }
    arg->prof_dump_write(arg->cbopaque, "\n");
}

/// jemalloc: prof_dump_header
void profDumpHeader(ProfDumpIterArg * arg, const ProfCounters * cnt_all)
{
    profDumpPrintf(
        arg->prof_dump_write, arg->cbopaque, "heap_v2/%llu\n  t*: ", static_cast<unsigned long long>(uint64_t(1U) << lg_prof_sample));
    profDumpPrintCnts(arg->prof_dump_write, arg->cbopaque, cnt_all);
    arg->prof_dump_write(arg->cbopaque, "\n");

    MutexLock lock(arg->tsdn, tdatas_mtx);
    tdatas.iter(
        nullptr,
        [&](ProfThreadData * tdata) -> ProfThreadData *
        {
            profTdataDumpIter(arg, tdata);
            return nullptr;
        });
}

/// jemalloc: prof_dump_gctx
void profDumpGctx(ProfDumpIterArg * arg, ProfGlobalContext * gctx, const ProfBacktrace * bt)
{
    gctx->lock->assertOwner(arg->tsdn);

    /// Avoid dumping such gctx's that have no useful data.
    if ((!opt.prof_accum && gctx->cnt_summed.curobjs == 0) || (opt.prof_accum && gctx->cnt_summed.accumobjs == 0))
    {
        JE_ASSERT(gctx->cnt_summed.curobjs == 0);
        JE_ASSERT(gctx->cnt_summed.curbytes == 0);
        /// The asserts on the unbiased cur counters would not be correct (races with `prof.reset`).
        JE_ASSERT(gctx->cnt_summed.accumobjs == 0);
        JE_ASSERT(gctx->cnt_summed.accumobjs_shifted_unbiased == 0);
        JE_ASSERT(gctx->cnt_summed.accumbytes == 0);
        JE_ASSERT(gctx->cnt_summed.accumbytes_unbiased == 0);
        return;
    }

    arg->prof_dump_write(arg->cbopaque, "@");
    for (unsigned i = 0; i < bt->len; ++i)
        profDumpPrintf(arg->prof_dump_write, arg->cbopaque, " %#lx", static_cast<unsigned long>(reinterpret_cast<uintptr_t>(bt->vec[i])));

    arg->prof_dump_write(arg->cbopaque, "\n  t*: ");
    profDumpPrintCnts(arg->prof_dump_write, arg->cbopaque, &gctx->cnt_summed);
    arg->prof_dump_write(arg->cbopaque, "\n");

    gctx->tctxs.iter(
        nullptr,
        [&](ProfThreadContext * tctx) -> ProfThreadContext *
        {
            profTctxDumpIter(arg, tctx);
            return nullptr;
        });

    /// One record per live sampled allocation attributed to this gctx, for fragmentation profiling (ignored by
    /// jeprof):
    ///     f: <age_ns> <request_size> <usize> <szind> <arena_ind> <thr_uid>
    /// An allocation on frag_objs cannot be concurrently deallocated (its sever point takes gctx->lock), and its tctx
    /// is pinned by the allocation's curobjs count, so all reads below are stable.
    for (Extent * edata = gctx->frag_objs.first(); edata != nullptr; edata = gctx->frag_objs.next(edata))
    {
        const NsTime * alloc_time = edata->profAllocTime();
        NsTime age = NsTime::zero();
        if (arg->now.compare(*alloc_time) > 0)
        {
            age.copy(arg->now);
            age.subtract(*alloc_time);
        }
        else
        {
            /// Sampled after this dump's timestamp was taken (or within the prof clock resolution).
            age = NsTime::zero();
        }
        ProfThreadContext * tctx = edata->profTctx();
        JE_ASSERT(profTctxIsValid(tctx));
        profDumpPrintf(
            arg->prof_dump_write,
            arg->cbopaque,
            "  f: %llu %zu %zu %u %u %llu\n",
            static_cast<unsigned long long>(age.ns()),
            edata->profAllocSize(),
            edata->usize(),
            static_cast<unsigned>(edata->szind()),
            edata->arenaInd(),
            static_cast<unsigned long long>(tctx->thr_uid));
    }
}

/// Scaling is equivalent AdjustSamples() in jeprof, but the result may differ slightly from what jeprof reports,
/// because here we scale the summary values, whereas jeprof scales each context individually and reports the sums of
/// the scaled values.
/// jemalloc: prof_leakcheck
void profLeakcheck(const ProfCounters * cnt_all, size_t leak_ngctx)
{
    if (cnt_all->curbytes != 0)
    {
        double sample_period = static_cast<double>(uint64_t(1) << lg_prof_sample);
        double ratio = ((static_cast<double>(cnt_all->curbytes)) / static_cast<double>(cnt_all->curobjs)) / sample_period;
        double scale_factor = 1.0 / (1.0 - exp(-ratio));
        auto curbytes = static_cast<uint64_t>(round((static_cast<double>(cnt_all->curbytes)) * scale_factor));
        auto curobjs = static_cast<uint64_t>(round((static_cast<double>(cnt_all->curobjs)) * scale_factor));

        printMessage(
            "<jemalloc>: Leak approximation summary: ~%llu byte%s, ~%llu object%s, >= %zu context%s\n",
            static_cast<unsigned long long>(curbytes),
            (curbytes != 1) ? "s" : "",
            static_cast<unsigned long long>(curobjs),
            (curobjs != 1) ? "s" : "",
            leak_ngctx,
            (leak_ngctx != 1) ? "s" : "");
        printMessage("<jemalloc>: Run jeprof on dump output for leak detail\n");
        if (opt.prof_leak_error)
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
void profDumpFragUtil(ProfDumpIterArg * arg)
{
    if constexpr (!config::stats)
    {
        /// curslabs/curregs/nonfull_slabs are not maintained.
        return;
    }
    for (unsigned i = 0; i < narenasTotalGet(); ++i)
    {
        Arena * arena = arenaGet(arg->tsdn, i, false);
        if (arena == nullptr)
            continue;
        for (szind_t j = 0; j < SC_NBINS; ++j)
        {
            const BinInfo * info = &bin_infos[j];
            size_t curslabs = 0;
            size_t curregs = 0;
            size_t nonfull_slabs = 0;
            for (unsigned k = 0; k < info->n_shards; ++k)
            {
                Bin * bin = arenaGetBin(arena, j, k);
                MutexLock lock(arg->tsdn, bin->lock);
                curslabs += bin->stats.curslabs;
                curregs += bin->stats.curregs;
                nonfull_slabs += bin->stats.nonfull_slabs;
            }
            if (curslabs == 0)
                continue;
            profDumpPrintf(
                arg->prof_dump_write,
                arg->cbopaque,
                "frag_util: %u %u %zu %zu %u %u %zu %zu %zu\n",
                i,
                static_cast<unsigned>(j),
                info->reg_size,
                info->slab_size,
                info->nregs,
                info->n_shards,
                curslabs,
                curregs,
                nonfull_slabs);
        }
    }
}

/// jemalloc: prof_dump_prep
void profDumpPrep(ThreadState & tsd, ProfThreadData * tdata, ProfCounters * cnt_all, size_t * leak_ngctx, ProfGctxTree * gctxs)
{
    profEnter(tsd, tdata);

    /// Put gctx's in limbo and clear their counters in preparation for summing.
    gctxs->init();
    void * gctx_v = nullptr;
    for (size_t tabind = 0; !bt2gctx.iter(&tabind, nullptr, &gctx_v);)
        profDumpGctxPrep(&tsd, static_cast<ProfGlobalContext *>(gctx_v), gctxs);

    /// Iterate over tdatas, and for the non-expired ones snapshot their tctx stats and merge them into the associated
    /// gctx's.
    memset(cnt_all, 0, sizeof(ProfCounters));
    {
        MutexLock lock(&tsd, tdatas_mtx);
        tdatas.iter(
            nullptr,
            [&](ProfThreadData * td) -> ProfThreadData *
            {
                profTdataMergeIter(&tsd, td, cnt_all);
                return nullptr;
            });
    }

    /// Merge tctx stats into gctx's.
    *leak_ngctx = 0;
    gctxs->iter(
        nullptr,
        [&](ProfGlobalContext * gctx) -> ProfGlobalContext *
        {
            profGctxMergeIter(&tsd, gctx, leak_ngctx);
            return nullptr;
        });

    profLeave(tsd, tdata);
}

}

/// jemalloc: prof_dump_impl
void profDumpImpl(ThreadState & tsd, WriteCallback * prof_dump_write, void * cbopaque, ProfThreadData * tdata, bool leakcheck)
{
    prof_dump_mtx.assertOwner(&tsd);
    ProfCounters cnt_all;
    size_t leak_ngctx;
    ProfGctxTree gctxs;
    profDumpPrep(tsd, tdata, &cnt_all, &leak_ngctx, &gctxs);
    ProfDumpIterArg prof_dump_iter_arg = {&tsd, prof_dump_write, cbopaque, NsTime::zero()};
    prof_dump_iter_arg.now.profInitUpdate();
    profDumpHeader(&prof_dump_iter_arg, &cnt_all);
    gctxs.iter(
        nullptr,
        [&](ProfGlobalContext * gctx) -> ProfGlobalContext *
        {
            /// jemalloc: prof_gctx_dump_iter
            MutexLock lock(&tsd, *gctx->lock);
            profDumpGctx(&prof_dump_iter_arg, gctx, &gctx->bt);
            return nullptr;
        });
    profDumpFragUtil(&prof_dump_iter_arg);
    profGctxFinish(tsd, &gctxs);
    if (leakcheck)
        profLeakcheck(&cnt_all, leak_ngctx);
}

/// jemalloc: prof_bt_hash
void profBtHash(const void * key, size_t r_hash[2])
{
    const auto * bt = static_cast<const ProfBacktrace *>(key);
    hash::hash(bt->vec, bt->len * sizeof(void *), 0x94122f33U, r_hash);
}

/// jemalloc: prof_bt_keycomp
bool profBtKeycomp(const void * k1, const void * k2)
{
    const auto * bt1 = static_cast<const ProfBacktrace *>(k1);
    const auto * bt2 = static_cast<const ProfBacktrace *>(k2);

    if (bt1->len != bt2->len)
        return false;
    return memcmp(bt1->vec, bt2->vec, bt1->len * sizeof(void *)) == 0;
}

/// --- Thread data ---------------------------------------------------------------------------------------------------

/// jemalloc: prof_tdata_init_impl
ProfThreadData *
profTdataInitImpl(ThreadState & tsd, uint64_t thr_uid, uint64_t thr_discrim, const char * thread_name, bool active)
{
    JE_ASSERT(tsd.reentrancyLevel() == 0);

    /// Initialize an empty cache for this thread.
    size_t tdata_sz = alignmentCeiling(sizeof(ProfThreadData), QUANTUM);
    size_t total_sz = tdata_sz + sizeof(void *) * opt.prof_bt_max;
    auto * tdata = static_cast<ProfThreadData *>(profAllocArena0(tsd, total_sz, true));
    if (tdata == nullptr)
        return nullptr;

    tdata->vec = reinterpret_cast<void **>(reinterpret_cast<uint8_t *>(tdata) + tdata_sz);
    tdata->lock = profTdataMutexChoose(thr_uid);
    tdata->thr_uid = thr_uid;
    tdata->thr_discrim = thr_discrim;
    tdata->attached = true;
    tdata->expired = false;
    tdata->tctx_uid_next = 0;
    if (thread_name == nullptr)
        profThreadNameClear(tdata);
    else
        profThreadNameWriteTdata(tdata, thread_name);
    profThreadNameAssert(tdata);

    if (tdata->bt2tctx.init(tsd, PROF_CKH_MINITEMS, profBtHash, profBtKeycomp))
    {
        profIdalloc(&tsd, tdata);
        return nullptr;
    }

    tdata->enq = false;
    tdata->enq_idump = false;
    tdata->enq_gdump = false;

    tdata->dumping = false;
    tdata->active = active;

    MutexLock lock(&tsd, tdatas_mtx);
    tdatas.insert(tdata);

    return tdata;
}

namespace
{

/// jemalloc: prof_tdata_should_destroy_unlocked
bool profTdataShouldDestroyUnlocked(ProfThreadData * tdata, bool even_if_attached)
{
    if (tdata->attached && !even_if_attached)
        return false;
    if (tdata->bt2tctx.count() != 0)
        return false;
    return true;
}

/// jemalloc: prof_tdata_should_destroy
bool profTdataShouldDestroy(ThreadState * tsdn, ProfThreadData * tdata, bool even_if_attached)
{
    tdata->lock->assertOwner(tsdn);
    return profTdataShouldDestroyUnlocked(tdata, even_if_attached);
}

/// jemalloc: prof_tdata_destroy_locked
void profTdataDestroyLocked(ThreadState & tsd, ProfThreadData * tdata, bool even_if_attached)
{
    tdatas_mtx.assertOwner(&tsd);
    tdata->lock->assertNotOwner(&tsd);

    tdatas.remove(tdata);
    JE_ASSERT(profTdataShouldDestroyUnlocked(tdata, even_if_attached));
    (void)even_if_attached;

    tdata->bt2tctx.destroy(tsd);
    profIdalloc(&tsd, tdata);
}

/// jemalloc: prof_tdata_destroy
void profTdataDestroy(ThreadState & tsd, ProfThreadData * tdata, bool even_if_attached)
{
    MutexLock lock(&tsd, tdatas_mtx);
    profTdataDestroyLocked(tsd, tdata, even_if_attached);
}

/// jemalloc: prof_tdata_expire
bool profTdataExpire(ThreadState * tsdn, ProfThreadData * tdata)
{
    bool destroy_tdata;

    MutexLock lock(tsdn, *tdata->lock);
    if (!tdata->expired)
    {
        tdata->expired = true;
        destroy_tdata = profTdataShouldDestroy(tsdn, tdata, false);
    }
    else
    {
        destroy_tdata = false;
    }

    return destroy_tdata;
}

}

/// jemalloc: prof_tdata_detach
void profTdataDetach(ThreadState & tsd, ProfThreadData * tdata)
{
    bool destroy_tdata;

    tdata->lock->lock(&tsd);
    if (tdata->attached)
    {
        destroy_tdata = profTdataShouldDestroy(&tsd, tdata, true);
        /// Only detach if !destroy_tdata, because detaching would allow another thread to win the race to destroy
        /// tdata.
        if (!destroy_tdata)
            tdata->attached = false;
        tsd.prof_tdata = nullptr;
    }
    else
    {
        destroy_tdata = false;
    }
    tdata->lock->unlock(&tsd);
    if (destroy_tdata)
        profTdataDestroy(tsd, tdata, true);
}

/// jemalloc: prof_reset
void profReset(ThreadState & tsd, size_t lg_sample)
{
    JE_ASSERT(lg_sample < (sizeof(uint64_t) << 3));

    MutexLock dump_lock(&tsd, prof_dump_mtx);
    MutexLock tdatas_lock(&tsd, tdatas_mtx);

    lg_prof_sample = lg_sample;
    profUnbiasMapInit();

    ProfThreadData * next = nullptr;
    do
    {
        /// jemalloc: prof_tdata_reset_iter
        ProfThreadData * to_destroy = tdatas.iter(
            next, [&](ProfThreadData * tdata) -> ProfThreadData * { return profTdataExpire(&tsd, tdata) ? tdata : nullptr; });
        if (to_destroy != nullptr)
        {
            next = tdatas.next(to_destroy);
            profTdataDestroyLocked(tsd, to_destroy, false);
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
bool profTctxShouldDestroy(ThreadState & tsd, ProfThreadContext * tctx)
{
    tctx->tdata->lock->assertOwner(&tsd);

    if (opt.prof_accum)
        return false;
    if (tctx->cnts.curobjs != 0)
        return false;
    if (tctx->prepared)
        return false;
    if (tctx->recent_count != 0)
        return false;
    return true;
}

/// jemalloc: prof_tctx_destroy
void profTctxDestroy(ThreadState & tsd, ProfThreadContext * tctx)
{
    tctx->tdata->lock->assertOwner(&tsd);

    JE_ASSERT(tctx->cnts.curobjs == 0);
    JE_ASSERT(tctx->cnts.curbytes == 0);
    /// The asserts on the unbiased cur counters are not correct (races with `prof.reset`).
    JE_ASSERT(!opt.prof_accum);
    JE_ASSERT(tctx->cnts.accumobjs == 0);
    JE_ASSERT(tctx->cnts.accumbytes == 0);
    /// These ones are, since accumbyte counts never go down.
    JE_ASSERT(tctx->cnts.accumobjs_shifted_unbiased == 0);
    JE_ASSERT(tctx->cnts.accumbytes_unbiased == 0);

    ProfGlobalContext * gctx = tctx->gctx;

    {
        ProfThreadData * tdata = tctx->tdata;
        tctx->tdata = nullptr;
        tdata->bt2tctx.remove(tsd, &gctx->bt, nullptr, nullptr);
        bool destroy_tdata = profTdataShouldDestroy(&tsd, tdata, false);
        tdata->lock->unlock(&tsd);
        if (destroy_tdata)
            profTdataDestroy(tsd, tdata, false);
    }

    bool destroy_tctx;
    bool destroy_gctx;

    gctx->lock->lock(&tsd);
    switch (tctx->state)
    {
        case prof_tctx_state_nominal:
            gctx->tctxs.remove(tctx);
            destroy_tctx = true;
            if (profGctxShouldDestroy(gctx))
            {
                /// Increment gctx->nlimbo in order to keep another thread from winning the race to destroy gctx while
                /// this one has gctx->lock dropped.
                ++gctx->nlimbo;
                destroy_gctx = true;
            }
            else
            {
                destroy_gctx = false;
            }
            break;
        case prof_tctx_state_dumping:
            /// A dumping thread needs tctx to remain valid until dumping has finished. Change state such that the
            /// dumping thread will complete destruction during a late dump iteration phase.
            tctx->state = prof_tctx_state_purgatory;
            destroy_tctx = false;
            destroy_gctx = false;
            break;
        case prof_tctx_state_initializing:
        case prof_tctx_state_purgatory:
        default:
            JE_NOT_REACHED();
            destroy_tctx = false;
            destroy_gctx = false;
    }
    gctx->lock->unlock(&tsd);
    if (destroy_gctx)
        profGctxTryDestroy(tsd, profTdataGet(tsd, false), gctx);
    if (destroy_tctx)
        profIdalloc(&tsd, tctx);
}

}

/// jemalloc: prof_tctx_try_destroy
void profTctxTryDestroy(ThreadState & tsd, ProfThreadContext * tctx)
{
    tctx->tdata->lock->assertOwner(&tsd);
    if (profTctxShouldDestroy(tsd, tctx))
    {
        /// tctx->tdata->lock will be released in `profTctxDestroy`.
        profTctxDestroy(tsd, tctx);
    }
    else
    {
        tctx->tdata->lock->unlock(&tsd);
    }
}

}
