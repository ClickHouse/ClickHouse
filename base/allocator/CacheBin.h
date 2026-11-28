#pragma once

/// The cache bins: the mechanism that the tcache and the arena use to communicate (jemalloc: `cache_bin.h`,
/// `src/cache_bin.c`).
///
/// The tcache fills from and flushes to the arena by passing a `CacheBin` to fill/flush. When the arena needs to pull
/// stats from the tcaches associated with it, it iterates over its `CacheBinArrayDescriptor` objects and reads out the
/// per-bin stats they contain, so the arena need not know about the tcache at all.
///
/// The layout (24 bytes) and the 16-bit low-bits encoding are exactly those of `cache_bin_t`: the fast paths compile to
/// the same instructions (one load of `stack_head`, one 16-bit compare).

#include <allocator/Common.h>
#include <allocator/IntrusiveList.h>

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace jemalloc
{

/// The size in bytes of each cache bin stack; also used to indicate *counts* of individual objects.
/// jemalloc: cache_bin_sz_t
using CacheBinSize = uint16_t;

/// jemalloc: JUNK_ADDR
inline constexpr uintptr_t JUNK_ADDR = 0x7a7a7a7a7a7a7a7aULL;

/// A noticeable mark pattern on the cache bin stack boundaries, in case a bug starts leaking those. It looks like the
/// junk pattern but is distinct from it: `JUNK_ADDR` vs. `JUNK_ADDR + 1` tells which pointer leaked.
/// jemalloc: cache_bin_preceding_junk, cache_bin_trailing_junk
inline constexpr uintptr_t cache_bin_preceding_junk = JUNK_ADDR;
inline constexpr uintptr_t cache_bin_trailing_junk = JUNK_ADDR + 1;

/// The word used as a fake `stack_head` of disabled bins, so that the enabled/disabled assessment does not rely on
/// `num_cached_max`. Its value is `JUNK_ADDR`.
/// jemalloc: disabled_bin
extern const uintptr_t disabled_bin;

/// The cache bins track their bounds looking just at the low bits of a pointer, compared against a `CacheBinSize`,
/// so a stack spans at most 2^16 bytes.
/// jemalloc: CACHE_BIN_NCACHED_MAX
inline constexpr size_t CACHE_BIN_NUM_CACHED_MAX = ((size_t(1) << (sizeof(CacheBinSize) * 8)) / sizeof(void *)) - 1;

/// jemalloc: VARIABLE_ARRAY_SIZE_MAX (`jemalloc_internal_types.h`)
inline constexpr size_t VARIABLE_ARRAY_SIZE_MAX = 2048;

/// Limits how many items can be flushed in a batch (the batch edata lookup reserves a stack array of this size).
/// jemalloc: CACHE_BIN_NFLUSH_BATCH_MAX
inline constexpr size_t CACHE_BIN_NUM_FLUSH_BATCH_MAX = (VARIABLE_ARRAY_SIZE_MAX >> LG_SIZEOF_PTR) - 1;

/// The bitmask of pointer bits that must be zero for a pointer to be junked and stashed on deallocation (UAF
/// detection). Defined by the sanitizer module (`san.c`); only referenced when `config::use_after_free_detection`.
/// jemalloc: san_cache_bin_nonfast_mask
extern uintptr_t sanitizer_cache_bin_non_fast_mask;

/// Lives inside the cache bin (for locality), initialized alongside it, but otherwise not modified by any cache bin
/// operation; it is maintained by the callers.
/// jemalloc: cache_bin_stats_t
struct CacheBinStats
{
    /// Number of allocation requests that corresponded to the size of this bin.
    uint64_t num_requests = 0;
};

/// jemalloc: cache_bin_info_t
struct CacheBinInfo
{
    CacheBinSize num_cached_max = 0;

    /// Initializes the info to represent up to `num_cached_max` items in the bins it is associated with.
    /// jemalloc: cache_bin_info_init
    void init(CacheBinSize num_cached_max_);
};

/// For small bins, used to calculate how many items to fill at a time: nfill = ncached_max >> (base - offset).
/// jemalloc: cache_bin_fill_ctl_t
struct CacheBinFillControl
{
    uint8_t base = 0;
    uint8_t offset = 0;
};

/// Filling and flushing are done in batch, on arrays of `void *`s. For filling, the arrays go forward, and can be
/// accessed with ordinary array arithmetic. For flushing, the objects at the end of the array (those we would return
/// last) are flushed. This preserves first-fit and cache locality (the arena fills with the earliest objects first, so
/// those are returned first by `alloc`; the most recently freed objects stay cached).
/// jemalloc: cache_bin_ptr_array_t, CACHE_BIN_PTR_ARRAY_DECLARE
struct CacheBinPtrArray
{
    CacheBinSize n = 0;
    void ** ptr = nullptr;

    constexpr CacheBinPtrArray() = default;
    constexpr explicit CacheBinPtrArray(CacheBinSize num_values)
        : n(num_values)
    {
    }
};

/// Responsible for caching allocations associated with a single size.
///
/// Several pointers are used to track the stack. To save on metadata bytes, only `stack_head` is a full sized pointer
/// (which is dereferenced on the fast path), while the others store only the low 16 bits: this is correct because a
/// single stack never takes more space than 2^16 bytes, and only equality checks / wrapping differences are performed
/// on the low bits.
///
///     (low addr)                                                  (high addr)
///     |------stashed------|------available------|------cached-----|
///     ^                   ^                     ^                 ^
///     low_bound(derived)  low_bits_full         stack_head        low_bits_empty
///
/// jemalloc: cache_bin_t
class CacheBin
{
public:
    /// The stack grows down. Whenever the bin is nonempty, the head points to an array entry containing a valid
    /// allocation. When it is empty, the head points to one element past the owned array.
    void ** stack_head = nullptr;
    /// `stack_head` and the stats are both modified frequently: keep them close (same cacheline, fewer write-backs).
    CacheBinStats thread_cache_stats;
    /// The low bits of the address of the first item in the stack that hasn't been used since the last GC, to track
    /// the low water mark (min # of cached items). Since the stack grows down, this is a higher address than
    /// `low_bits_full`.
    CacheBinSize low_bits_low_water = 0;
    /// The low bits of the value that `stack_head` will take on when the array is full (of cached & stashed items).
    /// This is the lowest available address in the array for caching. Only adjusted when stashing items.
    CacheBinSize low_bits_full = 0;
    /// The low bits of the value that `stack_head` will take on when the array is empty: one past the highest address
    /// in the array. Immutable after initialization.
    CacheBinSize low_bits_empty = 0;
    /// The maximum number of cached items in the bin.
    CacheBinInfo bin_info;

    /// Zero-initialized (as in TLS before `init`).
    constexpr CacheBin() = default;

    /// jemalloc: cache_bin_disabled_bin_stack
    static ALLOCATOR_ALWAYS_INLINE const void * disabledBinStack() { return &disabled_bin; }

    /// If a cache bin was zero initialized (because it lives in static or thread-local storage, or was memset to 0),
    /// this indicates whether or not `init` was called on it.
    /// jemalloc: cache_bin_still_zero_initialized
    ALLOCATOR_ALWAYS_INLINE bool stillZeroInitialized() const { return stack_head == nullptr; }

    /// jemalloc: cache_bin_disabled
    ALLOCATOR_ALWAYS_INLINE bool disabled() const
    {
        bool is_disabled = (stack_head == disabledBinStack());
        if (is_disabled)
            ALLOCATOR_ASSERT(reinterpret_cast<uintptr_t>(*stack_head) == JUNK_ADDR);
        return is_disabled;
    }

    /// Gets `num_cached_max` without asserting that the bin is enabled.
    /// jemalloc: cache_bin_ncached_max_get_unsafe
    ALLOCATOR_ALWAYS_INLINE CacheBinSize numCachedMaxGetUnsafe() const { return bin_info.num_cached_max; }

    /// The upper limit on ncached.
    /// jemalloc: cache_bin_ncached_max_get
    ALLOCATOR_ALWAYS_INLINE CacheBinSize numCachedMaxGet() const
    {
        ALLOCATOR_ASSERT(!disabled());
        return numCachedMaxGetUnsafe();
    }

    /// Asserts that the pointer associated with `earlier` is <= the one associated with `later`.
    /// jemalloc: cache_bin_assert_earlier
    ALLOCATOR_ALWAYS_INLINE void assertEarlier([[maybe_unused]] CacheBinSize earlier, [[maybe_unused]] CacheBinSize later) const
    {
        if (earlier > later)
            ALLOCATOR_ASSERT(low_bits_full > low_bits_empty);
    }

    /// Difference calculation that handles wraparound correctly. `earlier` must be associated with the position earlier
    /// in memory.
    /// jemalloc: cache_bin_diff
    ALLOCATOR_ALWAYS_INLINE CacheBinSize diff(CacheBinSize earlier, CacheBinSize later) const
    {
        assertEarlier(earlier, later);
        return static_cast<CacheBinSize>(later - earlier);
    }

    /// The low bits of `stack_head`.
    ALLOCATOR_ALWAYS_INLINE CacheBinSize lowBitsHead() const { return static_cast<CacheBinSize>(reinterpret_cast<uintptr_t>(stack_head)); }

    /// Number of items currently cached in the bin, without checking `num_cached_max`.
    /// jemalloc: cache_bin_ncached_get_internal
    ALLOCATOR_ALWAYS_INLINE CacheBinSize numCachedGetInternal() const
    {
        CacheBinSize d = diff(lowBitsHead(), low_bits_empty);
        CacheBinSize n = static_cast<CacheBinSize>(d / sizeof(void *));
        /// jemalloc's comment: this is racy (UB) when called from the arena stats code, but generates the correct
        /// assembly; the loads are kept non-atomic for the fast paths.
        ALLOCATOR_ASSERT(n == 0 || *stack_head != nullptr);
        return n;
    }

    /// Number of items currently cached in the bin, checking `num_cached_max`. The caller must know that no concurrent
    /// modification of the bin is possible.
    /// jemalloc: cache_bin_ncached_get_local
    ALLOCATOR_ALWAYS_INLINE CacheBinSize numCachedGetLocal() const
    {
        CacheBinSize n = numCachedGetInternal();
        ALLOCATOR_ASSERT(n <= numCachedMaxGet());
        return n;
    }

    /// A pointer to the position one past the end of the backing array. Do not call if racy.
    /// jemalloc: cache_bin_empty_position_get
    ALLOCATOR_ALWAYS_INLINE void ** emptyPositionGet() const
    {
        CacheBinSize d = diff(lowBitsHead(), low_bits_empty);
        void ** result = reinterpret_cast<void **>(reinterpret_cast<std::byte *>(stack_head) + d);
        ALLOCATOR_ASSERT(result >= stack_head);
        return result;
    }

    /// The low bits of the lower bound of the usable range. No values are concurrently modified, so this is safe in
    /// a multithreaded environment (the arena stats collection).
    /// jemalloc: cache_bin_low_bits_low_bound_get
    ALLOCATOR_ALWAYS_INLINE CacheBinSize lowBitsLowBoundGet() const
    {
        return static_cast<CacheBinSize>(low_bits_empty - numCachedMaxGet() * sizeof(void *));
    }

    /// A pointer to the position with the lowest address of the backing array.
    /// jemalloc: cache_bin_low_bound_get
    ALLOCATOR_ALWAYS_INLINE void ** lowBoundGet() const
    {
        CacheBinSize num_cached_max = numCachedMaxGet();
        void ** result = emptyPositionGet() - num_cached_max;
        ALLOCATOR_ASSERT(result <= stack_head);
        return result;
    }

    /// It's not correct to try to batch fill a nonempty cache bin.
    /// jemalloc: cache_bin_assert_empty
    ALLOCATOR_ALWAYS_INLINE void assertEmpty() const
    {
        ALLOCATOR_ASSERT(numCachedGetLocal() == 0);
        ALLOCATOR_ASSERT(emptyPositionGet() == stack_head);
    }

    /// The low water without the correctness checks, for when the invariants are temporarily broken (like
    /// ncached >= low_water during a flush).
    /// jemalloc: cache_bin_low_water_get_internal
    ALLOCATOR_ALWAYS_INLINE CacheBinSize lowWaterGetInternal() const
    {
        return static_cast<CacheBinSize>(diff(low_bits_low_water, low_bits_empty) / sizeof(void *));
    }

    /// The numeric value of low water in [0, ncached].
    /// jemalloc: cache_bin_low_water_get
    ALLOCATOR_ALWAYS_INLINE CacheBinSize lowWaterGet() const
    {
        CacheBinSize low_water = lowWaterGetInternal();
        ALLOCATOR_ASSERT(low_water <= numCachedMaxGet());
        ALLOCATOR_ASSERT(low_water <= numCachedGetLocal());
        assertEarlier(lowBitsHead(), low_bits_low_water);
        return low_water;
    }

    /// Indicates that the current position should be the low water mark going forward.
    /// jemalloc: cache_bin_low_water_set
    ALLOCATOR_ALWAYS_INLINE void lowWaterSet()
    {
        ALLOCATOR_ASSERT(!disabled());
        low_bits_low_water = lowBitsHead();
    }

    /// jemalloc: cache_bin_low_water_adjust
    ALLOCATOR_ALWAYS_INLINE void lowWaterAdjust()
    {
        ALLOCATOR_ASSERT(!disabled());
        if (numCachedGetInternal() < lowWaterGetInternal())
            lowWaterSet();
    }

    /// `success` (instead of the result) should be checked: there is never a null on the stack (unknown to the
    /// compiler), and eagerly checking the result would stall the pipeline waiting for the cacheline.
    /// jemalloc: cache_bin_alloc_impl
    ALLOCATOR_ALWAYS_INLINE void * allocImpl(bool & success, bool adjust_low_water)
    {
        /// This may read from the empty position; the loaded value won't be used. It's safe because the stack has one
        /// more slot reserved.
        void * result = *stack_head;
        /// Spelled out (not `lowBitsHead()`): this order of the IR makes clang emit exactly jemalloc's code (one
        /// `compare ..., uxth` instead of an extra `and`).
        CacheBinSize low_bits = static_cast<CacheBinSize>(reinterpret_cast<uintptr_t>(stack_head));
        void ** new_head = stack_head + 1;

        /// The low water mark is at most empty; if we pass this check, we know we're non-empty.
        if (ALLOCATOR_LIKELY(low_bits != low_bits_low_water))
        {
            stack_head = new_head;
            success = true;
            return result;
        }
        if (!adjust_low_water)
        {
            success = false;
            return nullptr;
        }
        /// In the fast-path case where we call `allocEasy` and then `alloc`, the previous checking and computation is
        /// optimized away: we didn't actually commit any of our operations.
        if (ALLOCATOR_LIKELY(low_bits != low_bits_empty))
        {
            stack_head = new_head;
            low_bits_low_water = static_cast<CacheBinSize>(reinterpret_cast<uintptr_t>(new_head));
            success = true;
            return result;
        }
        success = false;
        return nullptr;
    }

    /// Allocates an item out of the bin, failing if we're at the low-water mark.
    /// jemalloc: cache_bin_alloc_easy
    ALLOCATOR_ALWAYS_INLINE void * allocEasy(bool & success) { return allocImpl(success, false); }

    /// Allocates an item out of the bin, even if we're currently at the low-water mark (failing only if the bin is
    /// empty).
    /// jemalloc: cache_bin_alloc
    ALLOCATOR_ALWAYS_INLINE void * alloc(bool & success) { return allocImpl(success, true); }

    /// jemalloc: cache_bin_alloc_batch
    ALLOCATOR_ALWAYS_INLINE CacheBinSize allocBatch(size_t num, void ** out)
    {
        CacheBinSize n = numCachedGetInternal();
        if (n > num)
            n = static_cast<CacheBinSize>(num);
        memcpy(out, stack_head, n * sizeof(void *));
        stack_head += n;
        lowWaterAdjust();
        return n;
    }

    /// jemalloc: cache_bin_full
    ALLOCATOR_ALWAYS_INLINE bool full() const { return lowBitsHead() == low_bits_full; }

    /// Frees an object into the bin. Fails only if the bin is full.
    /// The double free scan of jemalloc (`cache_bin_dalloc_safety_checks`) is only compiled with `config_debug`, which
    /// is never enabled in ClickHouse (and `opt_debug_double_free_max_scan` is forced to 0 without it): dead code.
    /// jemalloc: cache_bin_dalloc_easy
    ALLOCATOR_ALWAYS_INLINE bool deallocateEasy(void * ptr)
    {
        if (ALLOCATOR_UNLIKELY(full()))
            return false;

        --stack_head;
        *stack_head = ptr;
        assertEarlier(low_bits_full, lowBitsHead());
        return true;
    }

    /// Returns false if failed to stash (i.e. the bin is full).
    /// jemalloc: cache_bin_stash
    ALLOCATOR_ALWAYS_INLINE bool stash(void * ptr)
    {
        if (full())
            return false;

        /// Stash at the full position, in the [full, head) range.
        CacheBinSize low_bits_head = lowBitsHead();
        /// Wraparound handled as well.
        CacheBinSize d = diff(low_bits_full, low_bits_head);
        *reinterpret_cast<void **>(reinterpret_cast<std::byte *>(stack_head) - d) = ptr;

        ALLOCATOR_ASSERT(!full());
        low_bits_full = static_cast<CacheBinSize>(low_bits_full + sizeof(void *));
        assertEarlier(low_bits_full, low_bits_head);
        return true;
    }

    /// The number of stashed pointers.
    /// jemalloc: cache_bin_nstashed_get_internal
    ALLOCATOR_ALWAYS_INLINE CacheBinSize numStashedGetInternal() const
    {
        [[maybe_unused]] CacheBinSize num_cached_max = numCachedMaxGet();
        CacheBinSize low_bits_low_bound = lowBitsLowBoundGet();

        CacheBinSize n = static_cast<CacheBinSize>(diff(low_bits_low_bound, low_bits_full) / sizeof(void *));
        ALLOCATOR_ASSERT(n <= num_cached_max);
        if constexpr (config::debug)
        {
            if (n != 0)
            {
                /// For assertions only. jemalloc also asserts `cache_bin_nonfast_aligned(stashed)` except in its own
                /// tests (`JEMALLOC_JET`), which stash arbitrary pointers; our tests do the same, so only the non-null
                /// check is kept.
                [[maybe_unused]] void ** low_bound = lowBoundGet();
                ALLOCATOR_ASSERT(static_cast<CacheBinSize>(reinterpret_cast<uintptr_t>(low_bound)) == low_bits_low_bound);
                ALLOCATOR_ASSERT(*(low_bound + n - 1) != nullptr);
            }
        }
        return n;
    }

    /// jemalloc: cache_bin_nstashed_get_local
    ALLOCATOR_ALWAYS_INLINE CacheBinSize numStashedGetLocal() const
    {
        CacheBinSize n = numStashedGetInternal();
        ALLOCATOR_ASSERT(n <= numCachedMaxGet());
        return n;
    }

    /// A racy view of the number of items currently in the bin, in the presence of possible concurrent modifications
    /// (from the arena stats code, read-only). The only difference is that assertions on mutable fields are omitted,
    /// and no utility functions on mutable fields are called.
    /// jemalloc: cache_bin_nitems_get_remote
    ALLOCATOR_ALWAYS_INLINE void numItemsGetRemote(CacheBinSize & num_cached, CacheBinSize & num_stashed) const
    {
        /// Racy version of `numCachedGetInternal`.
        CacheBinSize d = static_cast<CacheBinSize>(low_bits_empty - lowBitsHead());
        num_cached = static_cast<CacheBinSize>(d / sizeof(void *));

        /// Racy version of `numStashedGetInternal`.
        CacheBinSize low_bits_low_bound = lowBitsLowBoundGet();
        /// jemalloc compatibility: the subtraction is done in `int` (integer promotion) and divided as `size_t`, so
        /// unlike everywhere else a stack crossing a 64 KiB boundary gives a wrong (huge) racy count here.
        num_stashed = static_cast<CacheBinSize>(
            static_cast<size_t>(static_cast<int>(low_bits_full) - static_cast<int>(low_bits_low_bound)) / sizeof(void *));
        /// Nothing can be asserted regarding `num_cached_max` because it can be configured on the fly.
    }

    /// Starts a fill. The bin must be empty, and this must be followed by `finishFill` before any alloc/dalloc.
    /// jemalloc: cache_bin_init_ptr_array_for_fill
    ALLOCATOR_ALWAYS_INLINE void initPtrArrayForFill(CacheBinPtrArray & array, CacheBinSize num_fill) const
    {
        assertEmpty();
        array.ptr = emptyPositionGet() - num_fill;
    }

    /// `num_filled` is the number actually filled (which may be less than intended in case of OOM).
    /// jemalloc: cache_bin_finish_fill
    ALLOCATOR_ALWAYS_INLINE void finishFill(const CacheBinPtrArray & array, CacheBinSize num_filled)
    {
        assertEmpty();
        void ** empty_position = emptyPositionGet();
        if (num_filled < array.n)
            memmove(empty_position - num_filled, empty_position - array.n, num_filled * sizeof(void *));
        stack_head = empty_position - num_filled;
        /// Reset the bin stats as they are merged during the fill.
        if constexpr (config::stats)
            thread_cache_stats.num_requests = 0;
    }

    /// Same, but with flush. Unlike fill (which can fail), the user must flush everything we give them.
    /// jemalloc: cache_bin_init_ptr_array_for_flush
    ALLOCATOR_ALWAYS_INLINE void initPtrArrayForFlush(CacheBinPtrArray & array, CacheBinSize num_flush) const
    {
        array.ptr = emptyPositionGet() - num_flush;
        ALLOCATOR_ASSERT(numCachedGetLocal() == 0 || *array.ptr != nullptr);
    }

    /// jemalloc: cache_bin_finish_flush
    ALLOCATOR_ALWAYS_INLINE void finishFlush(const CacheBinPtrArray & /*arr*/, CacheBinSize num_flushed)
    {
        unsigned remainder = numCachedGetLocal() - num_flushed;
        memmove(stack_head + num_flushed, stack_head, remainder * sizeof(void *));
        stack_head += num_flushed;
        lowWaterAdjust();
        /// Reset the bin stats as they are merged during the flush.
        if constexpr (config::stats)
            thread_cache_stats.num_requests = 0;
    }

    /// jemalloc: cache_bin_init_ptr_array_for_stashed
    ALLOCATOR_ALWAYS_INLINE void
    initPtrArrayForStashed(SizeClassIdx /*binind*/, CacheBinPtrArray & array, [[maybe_unused]] CacheBinSize num_stashed) const
    {
        ALLOCATOR_ASSERT(num_stashed > 0);
        ALLOCATOR_ASSERT(numStashedGetLocal() == num_stashed);

        void ** low_bound = lowBoundGet();
        array.ptr = low_bound;
        ALLOCATOR_ASSERT(*array.ptr != nullptr);
    }

    /// jemalloc: cache_bin_finish_flush_stashed
    ALLOCATOR_ALWAYS_INLINE void finishFlushStashed()
    {
        void ** low_bound = lowBoundGet();

        /// Reset the bin local full position.
        low_bits_full = static_cast<CacheBinSize>(reinterpret_cast<uintptr_t>(low_bound));
        ALLOCATOR_ASSERT(numStashedGetLocal() == 0);
        /// Reset the bin stats as they are merged during the flush.
        if constexpr (config::stats)
            thread_cache_stats.num_requests = 0;
    }

    /// Initializes the bin with its stack at `alloc + current_offset`; advances `current_offset` past the stack. Callers
    /// allocate the memory indicated by `cacheBinInfoComputeAlloc`, call `cacheBinPreincrement`, `init` once for each
    /// bin and info, and then `cacheBinPostincrement`; `current_offset` then points immediately past the end of the
    /// allocation.
    /// jemalloc: cache_bin_init
    void init(const CacheBinInfo & info, void * alloc, size_t & current_offset);

    /// Makes the bin point at `disabled_bin` (so that alloc and dalloc always fail without extra branches), keeping
    /// `num_cached_max` in `bin_info`.
    /// jemalloc: cache_bin_init_disabled
    void initDisabled(CacheBinSize num_cached_max);
};

static_assert(sizeof(CacheBin) == 24, "Must have the size of cache_bin_t");
static_assert(offsetof(CacheBin, thread_cache_stats) == 8);
static_assert(offsetof(CacheBin, low_bits_low_water) == 16);
static_assert(offsetof(CacheBin, low_bits_full) == 18);
static_assert(offsetof(CacheBin, low_bits_empty) == 20);
static_assert(offsetof(CacheBin, bin_info) == 22);

/// The arena keeps a list of these (one per tcache) to iterate over the cache bins of its tcaches for stats
/// collection, without seeing the tcache definition.
/// jemalloc: cache_bin_array_descriptor_t
struct CacheBinArrayDescriptor
{
    RingLink<CacheBinArrayDescriptor> link{};
    /// Pointers to the tcache bins.
    CacheBin * bins = nullptr;

    /// jemalloc: cache_bin_array_descriptor_init
    void init(CacheBin * bins_)
    {
        Ring<CacheBinArrayDescriptor, &CacheBinArrayDescriptor::link>::init(this);
        bins = bins_;
    }
};

/// Whether the pointer is one that is junked and stashed on deallocation (UAF detection), decided by alignment. In
/// common cases a page-aligned check is needed already (sized deallocation with `config_prof`), so this adds no
/// instructions to the free fast path.
/// jemalloc: cache_bin_nonfast_aligned
ALLOCATOR_ALWAYS_INLINE bool cacheBinNonFastAligned(const void * ptr)
{
    if constexpr (!config::use_after_free_detection)
        return false;
    else
        return (reinterpret_cast<uintptr_t>(ptr) & sanitizer_cache_bin_non_fast_mask) == 0;
}

/// Given an array of initialized infos, determines how big an allocation is required to initialize a full set of
/// cache bins.
/// jemalloc: cache_bin_info_compute_alloc
void cacheBinInfoComputeAlloc(const CacheBinInfo * infos, SizeClassIdx num_infos, size_t & size, size_t & alignment);

/// Writes the preceding junk word at `alloc + current_offset` and advances `current_offset`.
/// jemalloc: cache_bin_preincrement
void cacheBinPreincrement(const CacheBinInfo * infos, SizeClassIdx num_infos, void * alloc, size_t & current_offset);

/// Writes the trailing junk word at `alloc + current_offset` and advances `current_offset`.
/// jemalloc: cache_bin_postincrement
void cacheBinPostincrement(void * alloc, size_t & current_offset);

/// If `metadata_transparent_huge_pages` is enabled, the tcache stacks are allocated from the base allocator.
/// jemalloc: cache_bin_stack_use_thp
bool cacheBinStackUseTransparentHugePages();

}
