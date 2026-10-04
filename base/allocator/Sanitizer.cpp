#include <allocator/Sanitizer.h>

#include <allocator/BackgroundThread.h>
#include <allocator/ExtentOps.h>
#include <allocator/Format.h>
#include <allocator/PageAllocator.h>

#include <cstdarg>
#include <cstdlib>

namespace jemalloc
{

constinit uintptr_t san_cache_bin_nonfast_mask = SAN_CACHE_BIN_NONFAST_MASK_DEFAULT;

namespace
{

/// jemalloc: san_find_guarded_addr
JE_ALWAYS_INLINE void sanFindGuardedAddr(Extent * edata, void ** guard1, void ** guard2, void ** addr, size_t size, bool left, bool right)
{
    JE_ASSERT(!edata->guarded());
    JE_ASSERT(size % PAGE == 0);
    *addr = edata->base();
    if (left)
    {
        *guard1 = *addr;
        *addr = static_cast<std::byte *>(*addr) + SAN_PAGE_GUARD;
    }
    else
    {
        *guard1 = nullptr;
    }

    if (right)
        *guard2 = static_cast<std::byte *>(*addr) + size;
    else
        *guard2 = nullptr;
}

/// jemalloc: san_find_unguarded_addr
JE_ALWAYS_INLINE void sanFindUnguardedAddr(Extent * edata, void ** guard1, void ** guard2, void ** addr, size_t size, bool left, bool right)
{
    JE_ASSERT(edata->guarded());
    JE_ASSERT(size % PAGE == 0);
    *addr = edata->base();
    if (right)
        *guard2 = static_cast<std::byte *>(*addr) + size;
    else
        *guard2 = nullptr;

    if (left)
    {
        *guard1 = static_cast<std::byte *>(*addr) - SAN_PAGE_GUARD;
        JE_ASSERT(*guard1 != nullptr);
        *addr = *guard1;
    }
    else
    {
        *guard1 = nullptr;
    }
}

/// jemalloc: san_unguard_pages_impl
void sanUnguardPagesImpl(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, ExtentMap * emap, bool left, bool right, bool remap)
{
    JE_ASSERT(left || right);
    /// Remove the inner boundary which no longer exists.
    if (remap)
    {
        JE_ASSERT(edata->state() == extent_state_active);
        emap->deregisterBoundary(tsdn, edata);
    }
    else
    {
        JE_ASSERT(edata->state() == extent_state_retained);
    }

    size_t size = edata->size();
    size_t size_with_guards = (left && right) ? sanTwoSideGuardedSize(size) : sanOneSideGuardedSize(size);

    void * guard1;
    void * guard2;
    void * addr;
    sanFindUnguardedAddr(edata, &guard1, &guard2, &addr, size, left, right);

    ehooks->unguard(tsdn, guard1, guard2);

    /// Update the true addr and usable size of the extent.
    edata->setSize(size_with_guards);
    edata->setAddr(addr);
    edata->setGuarded(false);

    /// Then re-register the outer boundary including the guards, if requested.
    if (remap)
        emap->registerBoundary(tsdn, edata, SC_NSIZES, /* slab */ false);
}

/// jemalloc: san_stashed_corrupted
bool sanStashedCorrupted(void * ptr, size_t size)
{
    if constexpr (sanJunkPtrShouldSlow())
    {
        for (size_t i = 0; i < size; ++i)
            if (static_cast<char *>(ptr)[i] != char(uaf_detect_junk))
                return true;
        return false;
    }

    void * first;
    void * mid;
    void * last;
    sanJunkPtrLocations(ptr, size, &first, &mid, &last);
    if (*static_cast<uintptr_t *>(first) != uaf_detect_junk || *static_cast<uintptr_t *>(mid) != uaf_detect_junk
        || *static_cast<uintptr_t *>(last) != uaf_detect_junk)
        return true;

    return false;
}

}

void sanGuardPages(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, ExtentMap * emap, bool left, bool right, bool remap)
{
    JE_ASSERT(left || right);
    if (remap)
        emap->deregisterBoundary(tsdn, edata);

    size_t size_with_guards = edata->size();
    size_t usize = (left && right) ? sanTwoSideUnguardedSize(size_with_guards) : sanOneSideUnguardedSize(size_with_guards);

    void * guard1;
    void * guard2;
    void * addr;
    sanFindGuardedAddr(edata, &guard1, &guard2, &addr, usize, left, right);

    JE_ASSERT(edata->state() == extent_state_active);
    ehooks->guard(tsdn, guard1, guard2);

    /// Update the guarded addr and usable size of the extent.
    edata->setSize(usize);
    edata->setAddr(addr);
    edata->setGuarded(true);

    if (remap)
        emap->registerBoundary(tsdn, edata, SC_NSIZES, /* slab */ false);
}

void sanUnguardPages(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, ExtentMap * emap, bool left, bool right)
{
    sanUnguardPagesImpl(tsdn, ehooks, edata, emap, left, right, /* remap */ true);
}

void sanUnguardPagesPreDestroy(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, ExtentMap * emap)
{
    emap->assertNotMapped(tsdn, edata);
    /// We don't want to touch the emap of about to be destroyed extents, as they have been unmapped upon eviction from
    /// the retained ecache. Also, we unguard the extents to the right, because retained extents only own their right
    /// guard page per `SanBumpAlloc::alloc`'s logic.
    sanUnguardPagesImpl(tsdn, ehooks, edata, emap, /* left */ false, /* right */ true, /* remap */ false);
}

void sanCheckStashedPtrs(void ** ptrs, size_t nstashed, size_t usize)
{
    /// Verify that the junk-filled and stashed pointers remain unchanged, to detect write-after-free.
    for (size_t n = 0; n < nstashed; ++n)
    {
        void * stashed = ptrs[n];
        JE_ASSERT(stashed != nullptr);
        JE_ASSERT(!config::uaf_detection || (reinterpret_cast<uintptr_t>(stashed) & san_cache_bin_nonfast_mask) == 0);
        if (JE_UNLIKELY(sanStashedCorrupted(stashed, usize)))
            safetyCheckFail("<jemalloc>: Write-after-free detected on deallocated pointer %p (size %zu).\n", stashed, usize);
    }
}

void tsdSanInit(ThreadState & tsd)
{
    tsd.san_extents_until_guard_small = opt.san_guard_small;
    tsd.san_extents_until_guard_large = opt.san_guard_large;
}

void sanInit(ssize_t lg_san_uaf_align)
{
    JE_ASSERT(lg_san_uaf_align == -1 || lg_san_uaf_align >= ssize_t(LG_PAGE));
    if (lg_san_uaf_align == -1)
    {
        san_cache_bin_nonfast_mask = uintptr_t(-1);
        return;
    }

    san_cache_bin_nonfast_mask = (uintptr_t(1) << lg_san_uaf_align) - 1;
}

/// --- SanBumpAlloc --------------------------------------------------------------------------------------------------

Extent * SanBumpAlloc::alloc(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, size_t size, bool zero)
{
    JE_ASSERT(sanBumpEnabled());

    Extent * to_destroy;
    size_t guarded_size = sanOneSideGuardedSize(size);
    Extent * edata;

    mtx.lock(tsdn);

    if (curr_reg == nullptr || curr_reg->size() < guarded_size)
    {
        /// If the current region can't accommodate the allocation, try replacing it with a larger one and destroy the
        /// current one if the replacement succeeds.
        to_destroy = curr_reg;
        bool err = growLocked(tsdn, pac, ehooks, guarded_size);
        if (err)
            goto label_err;
    }
    else
    {
        to_destroy = nullptr;
    }
    JE_ASSERT(guarded_size <= curr_reg->size());

    {
        size_t trail_size = curr_reg->size() - guarded_size;
        if (trail_size != 0)
        {
            Extent * curr_reg_trail
                = extentSplitWrapper(tsdn, pac, ehooks, curr_reg, guarded_size, trail_size, /* holding_core_locks */ true);
            if (curr_reg_trail == nullptr)
                goto label_err;
            edata = curr_reg;
            curr_reg = curr_reg_trail;
        }
        else
        {
            edata = curr_reg;
            curr_reg = nullptr;
        }
    }

    mtx.unlock(tsdn);

    JE_ASSERT(!edata->guarded());
    JE_ASSERT(curr_reg == nullptr || !curr_reg->guarded());
    JE_ASSERT(to_destroy == nullptr || !to_destroy->guarded());

    if (to_destroy != nullptr)
        extentDestroyWrapper(tsdn, pac, ehooks, to_destroy);

    sanGuardPages(tsdn, ehooks, edata, pac->emap, /* left */ false, /* right */ true, /* remap */ true);

    if (extentCommitZero(tsdn, ehooks, edata, /* commit */ true, zero, /* growing_retained */ false))
    {
        extentRecord(tsdn, pac, ehooks, &pac->ecache_retained, edata);
        return nullptr;
    }

    if constexpr (config::prof)
        extentGdumpAdd(tsdn, edata);

    return edata;

label_err:
    mtx.unlock(tsdn);
    return nullptr;
}

bool SanBumpAlloc::growLocked(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, size_t size)
{
    mtx.assertOwner(tsdn);

    bool committed = false;
    bool zeroed = false;
    size_t alloc_size = size > SBA_RETAINED_ALLOC_SIZE ? size : SBA_RETAINED_ALLOC_SIZE;
    JE_ASSERT((alloc_size & PAGE_MASK) == 0);
    curr_reg = extentAllocWrapper(tsdn, pac, ehooks, nullptr, alloc_size, PAGE, zeroed, &committed, /* growing_retained */ true);
    if (curr_reg == nullptr)
        return true;
    return false;
}

/// --- Safety checks -------------------------------------------------------------------------------------------------

namespace
{

/// jemalloc: safety_check_abort (static in `safety_check.c`)
constinit SafetyCheckAbortHook safety_check_abort = nullptr;

}

void safetyCheckFailSizedDealloc(bool current_dealloc, const void * ptr, size_t true_size, size_t input_size)
{
    const char * src = current_dealloc ? "the current pointer being freed" : "in thread cache, possibly from previous deallocations";
    /// jemalloc's `config_debug` (never set in ClickHouse).
    const char * suggest_debug_build = " --enable-debug or";

    safetyCheckFail(
        "<jemalloc>: size mismatch detected (true size %zu vs input size %zu), likely caused by application sized "
        "deallocation bugs (source address: %p, %s). Suggest building with%s address sanitizer for debugging. Abort.\n",
        true_size,
        input_size,
        ptr,
        src,
        suggest_debug_build);
}

void safetyCheckSetAbort(SafetyCheckAbortHook abort_fn)
{
    safety_check_abort = abort_fn;
}

/// In addition to `writeMessage`, also embed a hint in the abort function name, because there are cases where only
/// crash stack traces are logged. The name is kept verbatim from jemalloc for that reason.
/// jemalloc: safety_check_detected_heap_corruption___run_address_sanitizer_build_to_debug
JE_NOINLINE static void safety_check_detected_heap_corruption___run_address_sanitizer_build_to_debug(const char * buf)
{
    if (safety_check_abort == nullptr)
    {
        writeMessage(buf);
        abort();
    }
    else
    {
        safety_check_abort(buf);
    }
}

void safetyCheckFail(const char * format, ...)
{
    char buf[MALLOC_PRINTF_BUFSIZE];

    va_list ap;
    va_start(ap, format);
    formatV(buf, MALLOC_PRINTF_BUFSIZE, format, ap);
    va_end(ap);

    safety_check_detected_heap_corruption___run_address_sanitizer_build_to_debug(buf);
}

}
