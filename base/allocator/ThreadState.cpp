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

/// The thread-local TSD of `ThreadStateTLS` and `ThreadStateMallocThreadCleanup` (there is none with `ThreadStateGeneric`: `JEMALLOC_TLS`
/// is
/// not defined on Darwin). Only accessed by address through `thread_state_detail::tlsAddrThreadStateTLS`. `thread_state_initialized` exists
/// only
/// with `ThreadStateMallocThreadCleanup` (FreeBSD), like in jemalloc: the thread-local variables must be exactly those of
/// jemalloc, because the address of `thread_state_tls` (which depends on the size of the TLS segment with variant II TLS, e.g.
/// x86_64 and s390x) seeds the per-thread PRNG.
/// jemalloc: tsd_tls, tsd_initialized
#if ALLOCATOR_TLS_MODEL_INITIAL_EXEC
#define ALLOCATOR_THREAD_STATE_TLS_MODEL __attribute__((tls_model("initial-exec")))
#else
#define ALLOCATOR_THREAD_STATE_TLS_MODEL
#endif
#if !defined(__APPLE__)
constinit thread_local ThreadState thread_state_tls ALLOCATOR_THREAD_STATE_TLS_MODEL;
#endif
#if defined(__FreeBSD__)
constinit thread_local bool thread_state_initialized ALLOCATOR_THREAD_STATE_TLS_MODEL = false;
#endif
#undef ALLOCATOR_THREAD_STATE_TLS_MODEL

}

constinit pthread_key_t ThreadStateTLS::key{};
constinit bool ThreadStateTLS::is_booted = false;

constinit bool ThreadStateMallocThreadCleanup::is_booted = false;

constinit pthread_key_t ThreadStateGeneric::key{};
constinit bool ThreadStateGeneric::is_booted = false;
constinit ThreadStateGeneric::Wrapper ThreadStateGeneric::boot_wrapper{};

namespace thread_state_detail
{

#if ALLOCATOR_TLS_ADDR_FAST

#if !defined(__APPLE__)
constinit std::atomic<intptr_t> tls_offset_thread_state_tls{TLS_OFFSET_UNINITIALIZED};

/// jemalloc: jemalloc_tls_offset_init_tsd_tls
ALLOCATOR_NOINLINE intptr_t tlsOffsetInitThreadStateTLS()
{
    intptr_t tls_offset = reinterpret_cast<char *>(&thread_state_tls) - threadPointer();
    tls_offset_thread_state_tls.store(tls_offset, std::memory_order_relaxed);
    return tls_offset;
}
#endif

#if defined(__FreeBSD__)
constinit std::atomic<intptr_t> tls_offset_thread_state_initialized{TLS_OFFSET_UNINITIALIZED};

/// jemalloc: jemalloc_tls_offset_init_tsd_initialized
ALLOCATOR_NOINLINE intptr_t tlsOffsetInitThreadStateInitialized()
{
    intptr_t tls_offset = reinterpret_cast<char *>(&thread_state_initialized) - threadPointer();
    tls_offset_thread_state_initialized.store(tls_offset, std::memory_order_relaxed);
    return tls_offset;
}
#endif

#else

#if !defined(__APPLE__)
/// jemalloc: jemalloc_tls_addr_tsd_tls (noinline variant)
ALLOCATOR_NOINLINE ThreadState * tlsAddrThreadStateTLS()
{
    ThreadState * tls_addr = &thread_state_tls;
    __asm__ __volatile__("" : "+r"(tls_addr) : : "memory");
    return tls_addr;
}
#endif

#if defined(__FreeBSD__)
/// jemalloc: jemalloc_tls_addr_tsd_initialized (noinline variant)
ALLOCATOR_NOINLINE bool * tlsAddrThreadStateInitialized()
{
    bool * tls_addr = &thread_state_initialized;
    __asm__ __volatile__("" : "+r"(tls_addr) : : "memory");
    return tls_addr;
}
#endif

#endif

}

namespace
{

/// A list of all the TSDs in the nominal state.
/// jemalloc: tsd_nominal_tsds, tsd_nominal_tsds_lock (WITNESS_RANK_OMIT)
using ThreadStateList = IntrusiveList<ThreadState, &ThreadState::thread_state_link>;
constinit ThreadStateList thread_state_nominal_thread_states;
constinit Mutex thread_state_nominal_thread_states_lock;

/// How many slow-path-enabling features are turned on.
/// jemalloc: tsd_global_slow_count
constinit std::atomic<uint32_t> thread_state_global_slow_count{0};

/// jemalloc: tsd_in_nominal_list
[[maybe_unused]] bool threadStateInNominalList(ThreadState * thread_state)
{
    bool found = false;
    /// We don't know that tsd is nominal; it might not be safe to get data out of it here.
    thread_state_nominal_thread_states_lock.lock(nullptr);
    for (ThreadState * thread_state_list = thread_state_nominal_thread_states.first(); thread_state_list != nullptr;
         thread_state_list = thread_state_nominal_thread_states.next(thread_state_list))
    {
        if (thread_state == thread_state_list)
        {
            found = true;
            break;
        }
    }
    thread_state_nominal_thread_states_lock.unlock(nullptr);
    return found;
}

/// jemalloc: tsd_add_nominal
void threadStateAddNominal(ThreadState * thread_state)
{
    ALLOCATOR_ASSERT(!threadStateInNominalList(thread_state));
    ALLOCATOR_ASSERT(thread_state->stateGet() <= thread_state_nominal_max);
    ThreadStateList::elementInit(thread_state);
    thread_state_nominal_thread_states_lock.lock(thread_state);
    thread_state_nominal_thread_states.tailInsert(thread_state);
    thread_state_nominal_thread_states_lock.unlock(thread_state);
}

/// jemalloc: tsd_remove_nominal
void threadStateRemoveNominal(ThreadState * thread_state)
{
    ALLOCATOR_ASSERT(threadStateInNominalList(thread_state));
    ALLOCATOR_ASSERT(thread_state->stateGet() <= thread_state_nominal_max);
    thread_state_nominal_thread_states_lock.lock(thread_state);
    thread_state_nominal_thread_states.remove(thread_state);
    thread_state_nominal_thread_states_lock.unlock(thread_state);
}

/// jemalloc: tsd_force_recompute
void threadStateForceRecompute(ThreadState * thread_state)
{
    /// The stores to the states here need to synchronize with the exchange in `slowUpdate`.
    std::atomic_thread_fence(std::memory_order_release);
    thread_state_nominal_thread_states_lock.lock(thread_state);
    for (ThreadState * remote_thread_state = thread_state_nominal_thread_states.first(); remote_thread_state != nullptr;
         remote_thread_state = thread_state_nominal_thread_states.next(remote_thread_state))
    {
        ALLOCATOR_ASSERT(remote_thread_state->state.load(std::memory_order_relaxed) <= thread_state_nominal_max);
        remote_thread_state->state.store(thread_state_nominal_recompute, std::memory_order_relaxed);
        /// See the comments in `threadEventRecomputeFastThreshold`.
        std::atomic_thread_fence(std::memory_order_seq_cst);
        threadEventNextEventFastSetNonNominal(*remote_thread_state);
    }
    thread_state_nominal_thread_states_lock.unlock(thread_state);
}

/// The registered cleanups of `ThreadStateMallocThreadCleanup`.
/// jemalloc: ncleanups, cleanups
constinit unsigned num_cleanups = 0;
constinit MallocThreadStateCleanup cleanups[MALLOC_THREAD_STATE_CLEANUPS_MAX] = {};

/// Copies a TSD (only when the destination differs, which does not happen in practice). jemalloc: `*tsd = *val`
void threadStateCopy(ThreadState * dst, const ThreadState * src)
{
    memcpy(static_cast<void *>(dst), static_cast<const void *>(src), sizeof(ThreadState));
}

}

/// jemalloc: tsd_global_slow_inc
void ThreadState::globalSlowIncrement(ThreadState * thread_state)
{
    thread_state_global_slow_count.fetch_add(1, std::memory_order_relaxed);
    /// We unconditionally force a recompute, even if the global slow count was already positive. If we didn't, then
    /// it would be possible for us to return to the user, have the user synchronize externally with some other thread,
    /// and then have that other thread not have picked up the update yet (since the original incrementing thread might
    /// still be making its way through the tsd list).
    threadStateForceRecompute(thread_state);
}

/// jemalloc: tsd_global_slow_dec
void ThreadState::globalSlowDecrement(ThreadState * thread_state)
{
    thread_state_global_slow_count.fetch_sub(1, std::memory_order_relaxed);
    /// See the note in `globalSlowIncrement`.
    threadStateForceRecompute(thread_state);
}

/// jemalloc: tsd_global_slow
bool ThreadState::globalSlow()
{
    return thread_state_global_slow_count.load(std::memory_order_relaxed) > 0;
}

/// --- State machine -----------------------------------------------------------------------------------------------

/// jemalloc: tsd_state_compute
uint8_t ThreadState::stateCompute() const
{
    if (!nominal())
        return stateGet();
    /// We're in *a* nominal state; but which one?
    if (malloc_slow || localSlow() || globalSlow())
        return thread_state_nominal_slow;
    return thread_state_nominal;
}

/// jemalloc: tsd_slow_update
void ThreadState::slowUpdate()
{
    uint8_t old_state;
    do
    {
        uint8_t new_state = stateCompute();
        old_state = state.exchange(new_state, std::memory_order_acquire);
    } while (old_state == thread_state_nominal_recompute);

    threadEventRecomputeFastThreshold(*this);
}

/// jemalloc: tsd_state_set
void ThreadState::stateSet(uint8_t new_state)
{
    /// Only the tsd module can change the state *to* recompute.
    ALLOCATOR_ASSERT(new_state != thread_state_nominal_recompute);
    uint8_t old_state = state.load(std::memory_order_relaxed);
    if (old_state > thread_state_nominal_max)
    {
        /// Not currently in the nominal list, but it might need to be inserted there.
        ALLOCATOR_ASSERT(!threadStateInNominalList(this));
        state.store(new_state, std::memory_order_relaxed);
        if (new_state <= thread_state_nominal_max)
            threadStateAddNominal(this);
    }
    else
    {
        /// We're currently nominal. If the new state is non-nominal, great; we take ourselves off the list and just
        /// enter the new state.
        ALLOCATOR_ASSERT(threadStateInNominalList(this));
        if (new_state > thread_state_nominal_max)
        {
            threadStateRemoveNominal(this);
            state.store(new_state, std::memory_order_relaxed);
        }
        else
        {
            /// This is the tricky case. We're transitioning from one nominal state to another. The caller can't know
            /// about any races that are occurring at the same time, so we always have to recompute no matter what.
            slowUpdate();
        }
    }
    threadEventRecomputeFastThreshold(*this);
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
void ThreadState::sanitizerInit()
{
    sanitizer_extents_until_guard_small = options.sanitizer_guard_small;
    sanitizer_extents_until_guard_large = options.sanitizer_guard_large;
}

/// jemalloc: tsd_data_init
bool ThreadState::dataInit()
{
    /// The rtree context is initialized first (before the tcache), since the tcache initialization depends on it.
    radix_tree_context.init();
    prngStateInit();
    threadStateThreadEventInit(*this); /// The event init may use the prng state above.
    sanitizerInit();
    return threadCacheThreadStateDataInit(*this);
}

/// jemalloc: assert_tsd_data_cleanup_done
void ThreadState::assertDataCleanupDone() const
{
    ALLOCATOR_ASSERT(!nominal());
    ALLOCATOR_ASSERT(!threadStateInNominalList(const_cast<ThreadState *>(this)));
    ALLOCATOR_ASSERT(arena == nullptr);
    ALLOCATOR_ASSERT(internal_arena == nullptr);
    ALLOCATOR_ASSERT(thread_cache_enabled == false);
    ALLOCATOR_ASSERT(profiling_thread_data == nullptr);
}

/// jemalloc: tsd_data_init_nocleanup
bool ThreadState::dataInitNoCleanup()
{
    ALLOCATOR_ASSERT(stateGet() == thread_state_reincarnated || stateGet() == thread_state_minimal_initialized);
    /// During reincarnation, there is no guarantee that the cleanup function will be called (deallocation may happen
    /// after all tsd destructors). We set up tsd in a way that no cleanup is needed.
    radix_tree_context.init();
    thread_cache_enabled = false;
    reentrancy_level = 1;
    prngStateInit();
    threadStateThreadEventInit(*this); /// The event init may use the prng state above.
    sanitizerInit();
    assertDataCleanupDone();

    return false;
}

/// jemalloc: tsd_fetch_slow
ThreadState & ThreadState::fetchSlow(bool minimal)
{
    ALLOCATOR_ASSERT(!fast());

    if (stateGet() == thread_state_nominal_slow)
    {
        /// On slow path but no work needed. Note that we can't necessarily *assert* that we're slow, because we might
        /// be slow because of an asynchronous modification to global state, which might be asynchronously modified
        /// *back*.
    }
    else if (stateGet() == thread_state_nominal_recompute)
    {
        slowUpdate();
    }
    else if (stateGet() == thread_state_uninitialized)
    {
        if (!minimal)
        {
            if (ThreadStateStorage::is_booted)
            {
                stateSet(thread_state_nominal);
                slowUpdate();
                /// Trigger cleanup handler registration.
                ThreadStateStorage::set(this);
                dataInit();
            }
        }
        else
        {
            stateSet(thread_state_minimal_initialized);
            ThreadStateStorage::set(this);
            dataInitNoCleanup();
            min_init_state_num_fetched = 1;
        }
    }
    else if (stateGet() == thread_state_minimal_initialized)
    {
        /// If a thread only ever deallocates (e.g. dedicated reclamation threads), we want to help it to eventually
        /// escape the slow path (caused by the minimal initialized state). The counter tracks the number of times the
        /// tsd has been accessed under the min init state, and triggers the switch to nominal once reached the max
        /// allowed count. This means at most 128 deallocations stay on the slow path.
        ALLOCATOR_ASSERT(min_init_state_num_fetched >= 1);
        ++min_init_state_num_fetched;
        if (!minimal || min_init_state_num_fetched == THREAD_STATE_MIN_INIT_STATE_MAX_FETCHED)
        {
            /// Switch to fully initialized.
            stateSet(thread_state_nominal);
            ALLOCATOR_ASSERT(reentrancy_level >= 1);
            --reentrancy_level;
            slowUpdate();
            dataInit();
        }
        else
        {
            assertDataCleanupDone();
        }
    }
    else if (stateGet() == thread_state_purgatory)
    {
        stateSet(thread_state_reincarnated);
        ThreadStateStorage::set(this);
        dataInitNoCleanup();
    }
    else
    {
        ALLOCATOR_ASSERT(stateGet() == thread_state_reincarnated);
    }

    return *this;
}

/// jemalloc: tsd_do_data_cleanup
void ThreadState::doDataCleanup()
{
    profilingThreadDataCleanup(*this);
    internalArenaCleanup(*this);
    arenaCleanup(*this);
    threadCacheCleanup(*this);
    /// `witnesses_cleanup`: there is no witness.
    reentrancy_level = 1;
}

/// jemalloc: tsd_cleanup
void ThreadState::cleanup(void * arg)
{
    ThreadState * thread_state = static_cast<ThreadState *>(arg);

    switch (thread_state->stateGet())
    {
        case thread_state_uninitialized:
            /// Do nothing.
            break;
        case thread_state_minimal_initialized:
            /// This implies the thread only did free() in its life time.
            [[fallthrough]];
        case thread_state_reincarnated:
            /// Reincarnated means another destructor deallocated memory after the destructor was called. Cleanup isn't
            /// required but is still called for testing and completeness.
            thread_state->assertDataCleanupDone();
            [[fallthrough]];
        case thread_state_nominal:
        case thread_state_nominal_slow:
            thread_state->doDataCleanup();
            thread_state->stateSet(thread_state_purgatory);
            ThreadStateStorage::set(thread_state);
            break;
        case thread_state_purgatory:
            /// The previous time this destructor was called, we set the state to purgatory so that other destructors
            /// wouldn't cause re-creation of the tsd. This time, do nothing, and do not request another callback.
            break;
        default:
            /// `nominal_recompute`: jemalloc hits `not_reached()` here (undefined behavior in release builds).
            ALLOCATOR_NOT_REACHED();
    }
}

/// jemalloc: malloc_tsd_boot0
ThreadState * ThreadState::mallocThreadStateBoot0()
{
    if constexpr (config::thread_state_impl == ThreadStateImpl::MallocThreadCleanup)
        num_cleanups = 0;
    if (thread_state_nominal_thread_states_lock.init("tsd_nominal_tsds_lock", MutexRank::OMIT, MutexLockOrder::RankExclusive))
        return nullptr;
    if (ThreadStateStorage::boot0())
        return nullptr;
    return &fetch();
}

/// jemalloc: malloc_tsd_boot1
void ThreadState::mallocThreadStateBoot1()
{
    ThreadStateStorage::boot1();
    ThreadState & thread_state = fetch();
    /// `malloc_slow` has been set properly. Update the slow state.
    thread_state.slowUpdate();
}

/// jemalloc: tsd_prefork
void ThreadState::prefork()
{
    thread_state_nominal_thread_states_lock.prefork(this);
}

/// jemalloc: tsd_postfork_parent
void ThreadState::postforkParent()
{
    thread_state_nominal_thread_states_lock.postforkParent(this);
}

/// jemalloc: tsd_postfork_child
void ThreadState::postforkChild()
{
    thread_state_nominal_thread_states_lock.postforkChild(this);
    thread_state_nominal_thread_states.init();

    if (stateGet() <= thread_state_nominal_max)
        threadStateAddNominal(this);
}

/// --- TSDTLS ------------------------------------------------------------------------------------------------------

/// The implementations of the TSD flavours that use a thread-local variable are only compiled where that variable
/// exists (see `thread_state_tls` above); elsewhere they are only named in discarded `if constexpr` branches.
#if !defined(__APPLE__)

/// jemalloc: tsd_boot0 (tsd_tls.h)
bool ThreadStateTLS::boot0()
{
    if (pthread_key_create(&key, &ThreadState::cleanup) != 0)
        return true;
    is_booted = true;
    return false;
}

/// jemalloc: tsd_set (tsd_tls.h)
void ThreadStateTLS::set(ThreadState * value)
{
    ThreadState * thread_state = thread_state_detail::tlsAddrThreadStateTLS();

    ALLOCATOR_ASSERT(is_booted);
    if (ALLOCATOR_LIKELY(thread_state != value))
        threadStateCopy(thread_state, value);
    if (pthread_setspecific(key, static_cast<void *>(thread_state)) != 0)
    {
        writeMessage("<jemalloc>: Error setting tsd.\n");
        if (options.abort)
            abort();
    }
}

#endif

/// --- ThreadStateMallocThreadCleanup --------------------------------------------------------------------------------------

#if defined(__FreeBSD__)

/// jemalloc: tsd_cleanup_wrapper (tsd_malloc_thread_cleanup.h)
bool ThreadStateMallocThreadCleanup::cleanupWrapper()
{
    bool * initialized = thread_state_detail::tlsAddrThreadStateInitialized();
    if (*initialized)
    {
        *initialized = false;
        ThreadState::cleanup(thread_state_detail::tlsAddrThreadStateTLS());
    }
    return *initialized;
}

/// jemalloc: tsd_boot0 (tsd_malloc_thread_cleanup.h)
bool ThreadStateMallocThreadCleanup::boot0()
{
    mallocThreadStateCleanupRegister(&cleanupWrapper);
    is_booted = true;
    return false;
}

/// jemalloc: tsd_set (tsd_malloc_thread_cleanup.h)
void ThreadStateMallocThreadCleanup::set(ThreadState * value)
{
    ThreadState * thread_state = thread_state_detail::tlsAddrThreadStateTLS();

    ALLOCATOR_ASSERT(is_booted);
    if (ALLOCATOR_LIKELY(thread_state != value))
        threadStateCopy(thread_state, value);
    *thread_state_detail::tlsAddrThreadStateInitialized() = true;
}

#endif

/// jemalloc: _malloc_tsd_cleanup_register
void mallocThreadStateCleanupRegister(MallocThreadStateCleanup f)
{
    ALLOCATOR_ASSERT(num_cleanups < MALLOC_THREAD_STATE_CLEANUPS_MAX);
    cleanups[num_cleanups] = f;
    ++num_cleanups;
}

/// jemalloc: _malloc_thread_cleanup
void mallocThreadCleanup()
{
    bool pending[MALLOC_THREAD_STATE_CLEANUPS_MAX];
    bool again;

    for (unsigned i = 0; i < num_cleanups; ++i)
        pending[i] = true;

    do
    {
        again = false;
        for (unsigned i = 0; i < num_cleanups; ++i)
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

/// --- ThreadStateGeneric --------------------------------------------------------------------------------------------------

namespace
{

/// jemalloc: tsd_init_head_t tsd_init_head
struct ThreadStateInitHead
{
    IntrusiveList<ThreadStateGeneric::InitBlock, &ThreadStateGeneric::InitBlock::link> blocks;
    Mutex lock;
};

constinit ThreadStateInitHead thread_state_init_head;

/// jemalloc: malloc_tsd_malloc
void * mallocThreadStateMalloc(size_t size)
{
    return arena0Allocate(cacheLineCeiling(size));
}

/// jemalloc: malloc_tsd_dalloc
void mallocThreadStateDeallocate(void * wrapper)
{
    arena0Deallocate(wrapper);
}

}

/// jemalloc: tsd_init_check_recursion
void * ThreadStateGeneric::initCheckRecursion(InitBlock * block)
{
    pthread_t self = pthread_self();

    /// Check whether this thread has already inserted into the list.
    thread_state_init_head.lock.lock(nullptr);
    for (InitBlock * iterate = thread_state_init_head.blocks.first(); iterate != nullptr;
         iterate = thread_state_init_head.blocks.next(iterate))
    {
        if (pthread_equal(iterate->thread, self))
        {
            thread_state_init_head.lock.unlock(nullptr);
            return iterate->data;
        }
    }
    /// Insert the block into the list.
    thread_state_init_head.blocks.elementInit(block);
    block->thread = self;
    thread_state_init_head.blocks.tailInsert(block);
    thread_state_init_head.lock.unlock(nullptr);
    return nullptr;
}

/// jemalloc: tsd_init_finish
void ThreadStateGeneric::initFinish(InitBlock * block)
{
    thread_state_init_head.lock.lock(nullptr);
    thread_state_init_head.blocks.remove(block);
    thread_state_init_head.lock.unlock(nullptr);
}

/// jemalloc: tsd_cleanup_wrapper (tsd_generic.h)
void ThreadStateGeneric::cleanupWrapper(void * arg)
{
    Wrapper * wrapper = static_cast<Wrapper *>(arg);

    if (wrapper->initialized)
    {
        wrapper->initialized = false;
        ThreadState::cleanup(&wrapper->value);
        if (wrapper->initialized)
        {
            /// Trigger another cleanup round.
            if (pthread_setspecific(key, static_cast<void *>(wrapper)) != 0)
            {
                writeMessage("<jemalloc>: Error setting TSD\n");
                if (options.abort)
                    abort();
            }
            return;
        }
    }
    mallocThreadStateDeallocate(wrapper);
}

/// jemalloc: tsd_wrapper_set
void ThreadStateGeneric::wrapperSet(Wrapper * wrapper)
{
    if (ALLOCATOR_UNLIKELY(!is_booted))
        return;
    if (pthread_setspecific(key, static_cast<void *>(wrapper)) != 0)
    {
        writeMessage("<jemalloc>: Error setting TSD\n");
        abort();
    }
}

/// The `init && wrapper == NULL` part of `tsd_wrapper_get`.
/// jemalloc: tsd_wrapper_get
ALLOCATOR_NOINLINE ThreadStateGeneric::Wrapper * ThreadStateGeneric::wrapperGetSlow()
{
    InitBlock block;
    Wrapper * wrapper = static_cast<Wrapper *>(initCheckRecursion(&block));
    if (wrapper)
        return wrapper;
    wrapper = static_cast<Wrapper *>(mallocThreadStateMalloc(sizeof(Wrapper)));
    block.data = static_cast<void *>(wrapper);
    if (wrapper == nullptr)
    {
        writeMessage("<jemalloc>: Error allocating TSD\n");
        abort();
    }
    else
    {
        wrapper->initialized = false;
        new (&wrapper->value) ThreadState(); /// TSD_INITIALIZER
    }
    wrapperSet(wrapper);
    initFinish(&block);
    return wrapper;
}

/// jemalloc: tsd_boot0 (tsd_generic.h)
bool ThreadStateGeneric::boot0()
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
void ThreadStateGeneric::boot1()
{
    Wrapper * wrapper = static_cast<Wrapper *>(mallocThreadStateMalloc(sizeof(Wrapper)));
    if (wrapper == nullptr)
    {
        writeMessage("<jemalloc>: Error allocating TSD\n");
        abort();
    }
    boot_wrapper.initialized = false;
    ThreadState::cleanup(&boot_wrapper.value);
    wrapper->initialized = false;
    new (&wrapper->value) ThreadState(); /// TSD_INITIALIZER
    wrapperSet(wrapper);
}

/// jemalloc: tsd_set (tsd_generic.h)
void ThreadStateGeneric::set(ThreadState * value)
{
    ALLOCATOR_ASSERT(is_booted);
    Wrapper * wrapper = wrapperGet(true);
    if (ALLOCATOR_LIKELY(&wrapper->value != value))
        threadStateCopy(&wrapper->value, value);
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
    jemalloc::mallocThreadStateCleanupRegister(f);
}
#endif
