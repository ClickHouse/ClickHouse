#include <allocator/ThreadState.h>

#include <allocator/Format.h>
#include <allocator/IntrusiveList.h>
#include <allocator/Mutex.h>
#include <allocator/Options.h>
#include <allocator/ThreadEvent.h>

#include <cstdlib>
#include <cstring>
#include <new>

namespace jemalloc
{


/// --- Data --------------------------------------------------------------------------------------------------------

namespace
{

/// The thread-local TSD of `TsdTls` and `TsdMallocThreadCleanup` (there is none with `TsdGeneric`: `JEMALLOC_TLS` is
/// not defined on Darwin). Only accessed by address through `tsd_detail::tlsAddrTsdTls`. `tsd_initialized` exists only
/// with `TsdMallocThreadCleanup` (FreeBSD), like in jemalloc: the thread-local variables must be exactly those of
/// jemalloc, because the address of `tsd_tls` (which depends on the size of the TLS segment with variant II TLS, e.g.
/// x86_64 and s390x) seeds the per-thread PRNG.
/// jemalloc: tsd_tls, tsd_initialized
#if ALLOCATOR_TLS_MODEL_INITIAL_EXEC
#    define ALLOCATOR_TSD_TLS_MODEL __attribute__((tls_model("initial-exec")))
#else
#    define ALLOCATOR_TSD_TLS_MODEL
#endif
#if !defined(__APPLE__)
constinit thread_local ThreadState tsd_tls ALLOCATOR_TSD_TLS_MODEL;
#endif
#if defined(__FreeBSD__)
constinit thread_local bool tsd_initialized ALLOCATOR_TSD_TLS_MODEL = false;
#endif
#undef ALLOCATOR_TSD_TLS_MODEL

}

constinit pthread_key_t TsdTls::key{};
constinit bool TsdTls::is_booted = false;

constinit bool TsdMallocThreadCleanup::is_booted = false;

constinit pthread_key_t TsdGeneric::key{};
constinit bool TsdGeneric::is_booted = false;
constinit TsdGeneric::Wrapper TsdGeneric::boot_wrapper{};

namespace tsd_detail
{

#if ALLOCATOR_TLS_ADDR_FAST

#    if !defined(__APPLE__)
constinit std::atomic<intptr_t> tls_offset_tsd_tls{TLS_OFFSET_UNINITIALIZED};

/// jemalloc: jemalloc_tls_offset_init_tsd_tls
JE_NOINLINE intptr_t tlsOffsetInitTsdTls()
{
    intptr_t tls_offset = reinterpret_cast<char *>(&tsd_tls) - threadPointer();
    tls_offset_tsd_tls.store(tls_offset, std::memory_order_relaxed);
    return tls_offset;
}
#    endif

#    if defined(__FreeBSD__)
constinit std::atomic<intptr_t> tls_offset_tsd_initialized{TLS_OFFSET_UNINITIALIZED};

/// jemalloc: jemalloc_tls_offset_init_tsd_initialized
JE_NOINLINE intptr_t tlsOffsetInitTsdInitialized()
{
    intptr_t tls_offset = reinterpret_cast<char *>(&tsd_initialized) - threadPointer();
    tls_offset_tsd_initialized.store(tls_offset, std::memory_order_relaxed);
    return tls_offset;
}
#    endif

#else

#    if !defined(__APPLE__)
/// jemalloc: jemalloc_tls_addr_tsd_tls (noinline variant)
JE_NOINLINE ThreadState * tlsAddrTsdTls()
{
    ThreadState * tls_addr = &tsd_tls;
    __asm__ __volatile__("" : "+r"(tls_addr) : : "memory");
    return tls_addr;
}
#    endif

#    if defined(__FreeBSD__)
/// jemalloc: jemalloc_tls_addr_tsd_initialized (noinline variant)
JE_NOINLINE bool * tlsAddrTsdInitialized()
{
    bool * tls_addr = &tsd_initialized;
    __asm__ __volatile__("" : "+r"(tls_addr) : : "memory");
    return tls_addr;
}
#    endif

#endif

}

namespace
{

/// A list of all the TSDs in the nominal state.
/// jemalloc: tsd_nominal_tsds, tsd_nominal_tsds_lock (WITNESS_RANK_OMIT)
using TsdList = IntrusiveList<ThreadState, &ThreadState::tsd_link>;
constinit TsdList tsd_nominal_tsds;
constinit Mutex tsd_nominal_tsds_lock;

/// How many slow-path-enabling features are turned on.
/// jemalloc: tsd_global_slow_count
constinit std::atomic<uint32_t> tsd_global_slow_count{0};

/// jemalloc: tsd_in_nominal_list
[[maybe_unused]] bool tsdInNominalList(ThreadState * tsd)
{
    bool found = false;
    /// We don't know that tsd is nominal; it might not be safe to get data out of it here.
    tsd_nominal_tsds_lock.lock(nullptr);
    for (ThreadState * tsd_list = tsd_nominal_tsds.first(); tsd_list != nullptr; tsd_list = tsd_nominal_tsds.next(tsd_list))
    {
        if (tsd == tsd_list)
        {
            found = true;
            break;
        }
    }
    tsd_nominal_tsds_lock.unlock(nullptr);
    return found;
}

/// jemalloc: tsd_add_nominal
void tsdAddNominal(ThreadState * tsd)
{
    JE_ASSERT(!tsdInNominalList(tsd));
    JE_ASSERT(tsd->stateGet() <= tsd_state_nominal_max);
    TsdList::elementInit(tsd);
    tsd_nominal_tsds_lock.lock(tsd);
    tsd_nominal_tsds.tailInsert(tsd);
    tsd_nominal_tsds_lock.unlock(tsd);
}

/// jemalloc: tsd_remove_nominal
void tsdRemoveNominal(ThreadState * tsd)
{
    JE_ASSERT(tsdInNominalList(tsd));
    JE_ASSERT(tsd->stateGet() <= tsd_state_nominal_max);
    tsd_nominal_tsds_lock.lock(tsd);
    tsd_nominal_tsds.remove(tsd);
    tsd_nominal_tsds_lock.unlock(tsd);
}

/// jemalloc: tsd_force_recompute
void tsdForceRecompute(ThreadState * tsdn)
{
    /// The stores to the states here need to synchronize with the exchange in `slowUpdate`.
    std::atomic_thread_fence(std::memory_order_release);
    tsd_nominal_tsds_lock.lock(tsdn);
    for (ThreadState * remote_tsd = tsd_nominal_tsds.first(); remote_tsd != nullptr; remote_tsd = tsd_nominal_tsds.next(remote_tsd))
    {
        JE_ASSERT(remote_tsd->state.load(std::memory_order_relaxed) <= tsd_state_nominal_max);
        remote_tsd->state.store(tsd_state_nominal_recompute, std::memory_order_relaxed);
        /// See the comments in `teRecomputeFastThreshold`.
        std::atomic_thread_fence(std::memory_order_seq_cst);
        teNextEventFastSetNonNominal(*remote_tsd);
    }
    tsd_nominal_tsds_lock.unlock(tsdn);
}

/// The registered cleanups of `TsdMallocThreadCleanup`.
/// jemalloc: ncleanups, cleanups
constinit unsigned ncleanups = 0;
constinit MallocTsdCleanup cleanups[MALLOC_TSD_CLEANUPS_MAX] = {};

/// Copies a TSD (only when the destination differs, which does not happen in practice). jemalloc: `*tsd = *val`
void tsdCopy(ThreadState * dst, const ThreadState * src)
{
    memcpy(static_cast<void *>(dst), static_cast<const void *>(src), sizeof(ThreadState));
}

}

/// jemalloc: tsd_global_slow_inc
void ThreadState::globalSlowInc(ThreadState * tsdn)
{
    tsd_global_slow_count.fetch_add(1, std::memory_order_relaxed);
    /// We unconditionally force a recompute, even if the global slow count was already positive. If we didn't, then
    /// it would be possible for us to return to the user, have the user synchronize externally with some other thread,
    /// and then have that other thread not have picked up the update yet (since the original incrementing thread might
    /// still be making its way through the tsd list).
    tsdForceRecompute(tsdn);
}

/// jemalloc: tsd_global_slow_dec
void ThreadState::globalSlowDec(ThreadState * tsdn)
{
    tsd_global_slow_count.fetch_sub(1, std::memory_order_relaxed);
    /// See the note in `globalSlowInc`.
    tsdForceRecompute(tsdn);
}

/// jemalloc: tsd_global_slow
bool ThreadState::globalSlow()
{
    return tsd_global_slow_count.load(std::memory_order_relaxed) > 0;
}

/// --- State machine -----------------------------------------------------------------------------------------------

/// jemalloc: tsd_state_compute
uint8_t ThreadState::stateCompute() const
{
    if (!nominal())
        return stateGet();
    /// We're in *a* nominal state; but which one?
    if (malloc_slow || localSlow() || globalSlow())
        return tsd_state_nominal_slow;
    return tsd_state_nominal;
}

/// jemalloc: tsd_slow_update
void ThreadState::slowUpdate()
{
    uint8_t old_state;
    do
    {
        uint8_t new_state = stateCompute();
        old_state = state.exchange(new_state, std::memory_order_acquire);
    } while (old_state == tsd_state_nominal_recompute);

    teRecomputeFastThreshold(*this);
}

/// jemalloc: tsd_state_set
void ThreadState::stateSet(uint8_t new_state)
{
    /// Only the tsd module can change the state *to* recompute.
    JE_ASSERT(new_state != tsd_state_nominal_recompute);
    uint8_t old_state = state.load(std::memory_order_relaxed);
    if (old_state > tsd_state_nominal_max)
    {
        /// Not currently in the nominal list, but it might need to be inserted there.
        JE_ASSERT(!tsdInNominalList(this));
        state.store(new_state, std::memory_order_relaxed);
        if (new_state <= tsd_state_nominal_max)
            tsdAddNominal(this);
    }
    else
    {
        /// We're currently nominal. If the new state is non-nominal, great; we take ourselves off the list and just
        /// enter the new state.
        JE_ASSERT(tsdInNominalList(this));
        if (new_state > tsd_state_nominal_max)
        {
            tsdRemoveNominal(this);
            state.store(new_state, std::memory_order_relaxed);
        }
        else
        {
            /// This is the tricky case. We're transitioning from one nominal state to another. The caller can't know
            /// about any races that are occurring at the same time, so we always have to recompute no matter what.
            slowUpdate();
        }
    }
    teRecomputeFastThreshold(*this);
}

/// A nondeterministic seed based on the address of the TSD reduces the likelihood of lockstep non-uniform cache
/// index utilization among identical concurrent processes. jemalloc uses a deterministic seed (0) only with
/// `config_debug`, which is never enabled in ClickHouse, so this does not depend on `config::debug`.
/// jemalloc: tsd_prng_state_init
void ThreadState::prngStateInit()
{
    prng_state = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(this));
}

/// jemalloc: tsd_san_init
void ThreadState::sanInit()
{
    san_extents_until_guard_small = opt.san_guard_small;
    san_extents_until_guard_large = opt.san_guard_large;
}

/// jemalloc: tsd_data_init
bool ThreadState::dataInit()
{
    /// The rtree context is initialized first (before the tcache), since the tcache initialization depends on it.
    rtree_ctx.init();
    prngStateInit();
    tsdTeInit(*this); /// The event init may use the prng state above.
    sanInit();
    return tcacheTsdDataInit(*this);
}

/// jemalloc: assert_tsd_data_cleanup_done
void ThreadState::assertDataCleanupDone() const
{
    JE_ASSERT(!nominal());
    JE_ASSERT(!tsdInNominalList(const_cast<ThreadState *>(this)));
    JE_ASSERT(arena == nullptr);
    JE_ASSERT(iarena == nullptr);
    JE_ASSERT(tcache_enabled == false);
    JE_ASSERT(prof_tdata == nullptr);
}

/// jemalloc: tsd_data_init_nocleanup
bool ThreadState::dataInitNocleanup()
{
    JE_ASSERT(stateGet() == tsd_state_reincarnated || stateGet() == tsd_state_minimal_initialized);
    /// During reincarnation, there is no guarantee that the cleanup function will be called (deallocation may happen
    /// after all tsd destructors). We set up tsd in a way that no cleanup is needed.
    rtree_ctx.init();
    tcache_enabled = false;
    reentrancy_level = 1;
    prngStateInit();
    tsdTeInit(*this); /// The event init may use the prng state above.
    sanInit();
    assertDataCleanupDone();

    return false;
}

/// jemalloc: tsd_fetch_slow
ThreadState & ThreadState::fetchSlow(bool minimal)
{
    JE_ASSERT(!fast());

    if (stateGet() == tsd_state_nominal_slow)
    {
        /// On slow path but no work needed. Note that we can't necessarily *assert* that we're slow, because we might
        /// be slow because of an asynchronous modification to global state, which might be asynchronously modified
        /// *back*.
    }
    else if (stateGet() == tsd_state_nominal_recompute)
    {
        slowUpdate();
    }
    else if (stateGet() == tsd_state_uninitialized)
    {
        if (!minimal)
        {
            if (Tsd::is_booted)
            {
                stateSet(tsd_state_nominal);
                slowUpdate();
                /// Trigger cleanup handler registration.
                Tsd::set(this);
                dataInit();
            }
        }
        else
        {
            stateSet(tsd_state_minimal_initialized);
            Tsd::set(this);
            dataInitNocleanup();
            min_init_state_nfetched = 1;
        }
    }
    else if (stateGet() == tsd_state_minimal_initialized)
    {
        /// If a thread only ever deallocates (e.g. dedicated reclamation threads), we want to help it to eventually
        /// escape the slow path (caused by the minimal initialized state). The counter tracks the number of times the
        /// tsd has been accessed under the min init state, and triggers the switch to nominal once reached the max
        /// allowed count. This means at most 128 deallocations stay on the slow path.
        JE_ASSERT(min_init_state_nfetched >= 1);
        ++min_init_state_nfetched;
        if (!minimal || min_init_state_nfetched == TSD_MIN_INIT_STATE_MAX_FETCHED)
        {
            /// Switch to fully initialized.
            stateSet(tsd_state_nominal);
            JE_ASSERT(reentrancy_level >= 1);
            --reentrancy_level;
            slowUpdate();
            dataInit();
        }
        else
        {
            assertDataCleanupDone();
        }
    }
    else if (stateGet() == tsd_state_purgatory)
    {
        stateSet(tsd_state_reincarnated);
        Tsd::set(this);
        dataInitNocleanup();
    }
    else
    {
        JE_ASSERT(stateGet() == tsd_state_reincarnated);
    }

    return *this;
}

/// jemalloc: tsd_do_data_cleanup
void ThreadState::doDataCleanup()
{
    profTdataCleanup(*this);
    iarenaCleanup(*this);
    arenaCleanup(*this);
    tcacheCleanup(*this);
    /// `witnesses_cleanup`: there is no witness.
    reentrancy_level = 1;
}

/// jemalloc: tsd_cleanup
void ThreadState::cleanup(void * arg)
{
    ThreadState * tsd = static_cast<ThreadState *>(arg);

    switch (tsd->stateGet())
    {
        case tsd_state_uninitialized:
            /// Do nothing.
            break;
        case tsd_state_minimal_initialized:
            /// This implies the thread only did free() in its life time.
            [[fallthrough]];
        case tsd_state_reincarnated:
            /// Reincarnated means another destructor deallocated memory after the destructor was called. Cleanup isn't
            /// required but is still called for testing and completeness.
            tsd->assertDataCleanupDone();
            [[fallthrough]];
        case tsd_state_nominal:
        case tsd_state_nominal_slow:
            tsd->doDataCleanup();
            tsd->stateSet(tsd_state_purgatory);
            Tsd::set(tsd);
            break;
        case tsd_state_purgatory:
            /// The previous time this destructor was called, we set the state to purgatory so that other destructors
            /// wouldn't cause re-creation of the tsd. This time, do nothing, and do not request another callback.
            break;
        default:
            /// `nominal_recompute`: jemalloc hits `not_reached()` here (undefined behavior in release builds).
            JE_NOT_REACHED();
    }
}

/// jemalloc: malloc_tsd_boot0
ThreadState * ThreadState::mallocTsdBoot0()
{
    if constexpr (config::tsd_impl == TsdImpl::MallocThreadCleanup)
        ncleanups = 0;
    if (tsd_nominal_tsds_lock.init("tsd_nominal_tsds_lock", MutexRank::OMIT, MutexLockOrder::RankExclusive))
        return nullptr;
    if (Tsd::boot0())
        return nullptr;
    return &fetch();
}

/// jemalloc: malloc_tsd_boot1
void ThreadState::mallocTsdBoot1()
{
    Tsd::boot1();
    ThreadState & tsd = fetch();
    /// `malloc_slow` has been set properly. Update the slow state.
    tsd.slowUpdate();
}

/// jemalloc: tsd_prefork
void ThreadState::prefork()
{
    tsd_nominal_tsds_lock.prefork(this);
}

/// jemalloc: tsd_postfork_parent
void ThreadState::postforkParent()
{
    tsd_nominal_tsds_lock.postforkParent(this);
}

/// jemalloc: tsd_postfork_child
void ThreadState::postforkChild()
{
    tsd_nominal_tsds_lock.postforkChild(this);
    tsd_nominal_tsds.init();

    if (stateGet() <= tsd_state_nominal_max)
        tsdAddNominal(this);
}

/// --- TsdTls ------------------------------------------------------------------------------------------------------

/// The implementations of the TSD flavours that use a thread-local variable are only compiled where that variable
/// exists (see `tsd_tls` above); elsewhere they are only named in discarded `if constexpr` branches.
#if !defined(__APPLE__)

/// jemalloc: tsd_boot0 (tsd_tls.h)
bool TsdTls::boot0()
{
    if (pthread_key_create(&key, &ThreadState::cleanup) != 0)
        return true;
    is_booted = true;
    return false;
}

/// jemalloc: tsd_set (tsd_tls.h)
void TsdTls::set(ThreadState * val)
{
    ThreadState * tsd = tsd_detail::tlsAddrTsdTls();

    JE_ASSERT(is_booted);
    if (JE_LIKELY(tsd != val))
        tsdCopy(tsd, val);
    if (pthread_setspecific(key, static_cast<void *>(tsd)) != 0)
    {
        writeMessage("<jemalloc>: Error setting tsd.\n");
        if (opt.abort)
            abort();
    }
}

#endif

/// --- TsdMallocThreadCleanup --------------------------------------------------------------------------------------

#if defined(__FreeBSD__)

/// jemalloc: tsd_cleanup_wrapper (tsd_malloc_thread_cleanup.h)
bool TsdMallocThreadCleanup::cleanupWrapper()
{
    bool * initialized = tsd_detail::tlsAddrTsdInitialized();
    if (*initialized)
    {
        *initialized = false;
        ThreadState::cleanup(tsd_detail::tlsAddrTsdTls());
    }
    return *initialized;
}

/// jemalloc: tsd_boot0 (tsd_malloc_thread_cleanup.h)
bool TsdMallocThreadCleanup::boot0()
{
    mallocTsdCleanupRegister(&cleanupWrapper);
    is_booted = true;
    return false;
}

/// jemalloc: tsd_set (tsd_malloc_thread_cleanup.h)
void TsdMallocThreadCleanup::set(ThreadState * val)
{
    ThreadState * tsd = tsd_detail::tlsAddrTsdTls();

    JE_ASSERT(is_booted);
    if (JE_LIKELY(tsd != val))
        tsdCopy(tsd, val);
    *tsd_detail::tlsAddrTsdInitialized() = true;
}

#endif

/// jemalloc: _malloc_tsd_cleanup_register
void mallocTsdCleanupRegister(MallocTsdCleanup f)
{
    JE_ASSERT(ncleanups < MALLOC_TSD_CLEANUPS_MAX);
    cleanups[ncleanups] = f;
    ++ncleanups;
}

/// jemalloc: _malloc_thread_cleanup
void mallocThreadCleanup()
{
    bool pending[MALLOC_TSD_CLEANUPS_MAX];
    bool again;

    for (unsigned i = 0; i < ncleanups; ++i)
        pending[i] = true;

    do
    {
        again = false;
        for (unsigned i = 0; i < ncleanups; ++i)
        {
            if (pending[i])
            {
                pending[i] = cleanups[i]();
                if (pending[i])
                    again = true;
            }
        }
    } while (again);
}

/// --- TsdGeneric --------------------------------------------------------------------------------------------------

namespace
{

/// jemalloc: tsd_init_head_t tsd_init_head
struct TsdInitHead
{
    IntrusiveList<TsdGeneric::InitBlock, &TsdGeneric::InitBlock::link> blocks;
    Mutex lock;
};

constinit TsdInitHead tsd_init_head;

/// jemalloc: malloc_tsd_malloc
void * mallocTsdMalloc(size_t size)
{
    return a0malloc(cachelineCeiling(size));
}

/// jemalloc: malloc_tsd_dalloc
void mallocTsdDalloc(void * wrapper)
{
    a0dalloc(wrapper);
}

}

/// jemalloc: tsd_init_check_recursion
void * TsdGeneric::initCheckRecursion(InitBlock * block)
{
    pthread_t self = pthread_self();

    /// Check whether this thread has already inserted into the list.
    tsd_init_head.lock.lock(nullptr);
    for (InitBlock * iter = tsd_init_head.blocks.first(); iter != nullptr; iter = tsd_init_head.blocks.next(iter))
    {
        if (pthread_equal(iter->thread, self))
        {
            tsd_init_head.lock.unlock(nullptr);
            return iter->data;
        }
    }
    /// Insert the block into the list.
    tsd_init_head.blocks.elementInit(block);
    block->thread = self;
    tsd_init_head.blocks.tailInsert(block);
    tsd_init_head.lock.unlock(nullptr);
    return nullptr;
}

/// jemalloc: tsd_init_finish
void TsdGeneric::initFinish(InitBlock * block)
{
    tsd_init_head.lock.lock(nullptr);
    tsd_init_head.blocks.remove(block);
    tsd_init_head.lock.unlock(nullptr);
}

/// jemalloc: tsd_cleanup_wrapper (tsd_generic.h)
void TsdGeneric::cleanupWrapper(void * arg)
{
    Wrapper * wrapper = static_cast<Wrapper *>(arg);

    if (wrapper->initialized)
    {
        wrapper->initialized = false;
        ThreadState::cleanup(&wrapper->val);
        if (wrapper->initialized)
        {
            /// Trigger another cleanup round.
            if (pthread_setspecific(key, static_cast<void *>(wrapper)) != 0)
            {
                writeMessage("<jemalloc>: Error setting TSD\n");
                if (opt.abort)
                    abort();
            }
            return;
        }
    }
    mallocTsdDalloc(wrapper);
}

/// jemalloc: tsd_wrapper_set
void TsdGeneric::wrapperSet(Wrapper * wrapper)
{
    if (JE_UNLIKELY(!is_booted))
        return;
    if (pthread_setspecific(key, static_cast<void *>(wrapper)) != 0)
    {
        writeMessage("<jemalloc>: Error setting TSD\n");
        abort();
    }
}

/// The `init && wrapper == NULL` part of `tsd_wrapper_get`.
/// jemalloc: tsd_wrapper_get
JE_NOINLINE TsdGeneric::Wrapper * TsdGeneric::wrapperGetSlow()
{
    InitBlock block;
    Wrapper * wrapper = static_cast<Wrapper *>(initCheckRecursion(&block));
    if (wrapper)
        return wrapper;
    wrapper = static_cast<Wrapper *>(mallocTsdMalloc(sizeof(Wrapper)));
    block.data = static_cast<void *>(wrapper);
    if (wrapper == nullptr)
    {
        writeMessage("<jemalloc>: Error allocating TSD\n");
        abort();
    }
    else
    {
        wrapper->initialized = false;
        new (&wrapper->val) ThreadState(); /// TSD_INITIALIZER
    }
    wrapperSet(wrapper);
    initFinish(&block);
    return wrapper;
}

/// jemalloc: tsd_boot0 (tsd_generic.h)
bool TsdGeneric::boot0()
{
    InitBlock block;

    Wrapper * wrapper = static_cast<Wrapper *>(initCheckRecursion(&block));
    if (wrapper)
        return false;
    block.data = &boot_wrapper;
    if (pthread_key_create(&key, &cleanupWrapper) != 0)
        return true;
    is_booted = true;
    wrapperSet(&boot_wrapper);
    initFinish(&block);
    return false;
}

/// Tears down the boot thread's TSD contents (arena bindings, tcache) and restarts it with a fresh uninitialized TSD
/// in a heap wrapper.
/// jemalloc: tsd_boot1 (tsd_generic.h)
void TsdGeneric::boot1()
{
    Wrapper * wrapper = static_cast<Wrapper *>(mallocTsdMalloc(sizeof(Wrapper)));
    if (wrapper == nullptr)
    {
        writeMessage("<jemalloc>: Error allocating TSD\n");
        abort();
    }
    boot_wrapper.initialized = false;
    ThreadState::cleanup(&boot_wrapper.val);
    wrapper->initialized = false;
    new (&wrapper->val) ThreadState(); /// TSD_INITIALIZER
    wrapperSet(wrapper);
}

/// jemalloc: tsd_set (tsd_generic.h)
void TsdGeneric::set(ThreadState * val)
{
    JE_ASSERT(is_booted);
    Wrapper * wrapper = wrapperGet(true);
    if (JE_LIKELY(&wrapper->val != val))
        tsdCopy(&wrapper->val, val);
    wrapper->initialized = true;
}

}

#if defined(__FreeBSD__)
/// Called by FreeBSD's libthr at thread exit.
extern "C" __attribute__((visibility("default"))) void _malloc_thread_cleanup()
{
    jemalloc::mallocThreadCleanup();
}

/// Exported by jemalloc on FreeBSD (`JEMALLOC_EXPORT`).
extern "C" __attribute__((visibility("default"))) void _malloc_tsd_cleanup_register(bool (*f)())
{
    jemalloc::mallocTsdCleanupRegister(f);
}
#endif
