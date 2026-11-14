#pragma once

/// The radix tree that maps page addresses to extent metadata (jemalloc: `rtree.h`, `rtree_tsd.h`, `rtree.c`).
///
/// The geometry (height, bits per level) is computed at compile time from LOG2_VIRTUAL_ADDRESS and LOG2_PAGE. The leaf element is
/// a single tagged pointer (compact) when the high insignificant address bits can hold a size class index, otherwise
/// a pointer plus a metadata word. All lookups go through the per-thread `RadixTreeContext` (a 16-entry direct-mapped
/// L1 cache and an 8-entry LRU L2 cache of leaves), with exactly jemalloc's replacement policy.
///
/// Node and leaf memory comes from `Base` (zeroed, never freed). All tree words are accessed atomically through
/// `std::atomic_ref`, so the tree (with its large embedded root array) is a trivial, zero-initialized object.

#include <allocator/Common.h>
#include <allocator/Extent.h>
#include <allocator/Mutex.h>
#include <allocator/SizeClasses.h>

#include <array>
#include <atomic>
#include <bit>
#include <cstring>
#include <type_traits>

namespace jemalloc
{

class Base;
class ThreadState;

/// --- Geometry --------------------------------------------------------------------------------------------------------

/// Number of high insignificant bits. jemalloc: RTREE_NHIB
inline constexpr unsigned RADIX_TREE_NUM_HIGH_INSIGNIFICANT_BITS = (1U << (LG_SIZEOF_PTR + 3)) - LOG2_VIRTUAL_ADDRESS;
/// Number of low insignificant bits. jemalloc: RTREE_NLIB
inline constexpr unsigned RADIX_TREE_NUM_LOW_INSIGNIFICANT_BITS = LOG2_PAGE;
/// Number of significant bits. jemalloc: RTREE_NSB
inline constexpr unsigned RADIX_TREE_NUM_SIGNIFICANT_BITS = LOG2_VIRTUAL_ADDRESS - RADIX_TREE_NUM_LOW_INSIGNIFICANT_BITS;
/// Number of levels in the radix tree. jemalloc: RTREE_HEIGHT
inline constexpr unsigned RADIX_TREE_HEIGHT = RADIX_TREE_NUM_SIGNIFICANT_BITS <= 10 ? 1 : (RADIX_TREE_NUM_SIGNIFICANT_BITS <= 36 ? 2 : 3);
static_assert(RADIX_TREE_NUM_SIGNIFICANT_BITS <= 52, "Unsupported number of significant virtual address bits");
static_assert(RADIX_TREE_HEIGHT == 2 || RADIX_TREE_HEIGHT == 3, "Only heights 2 and 3 are reachable on supported platforms");

/// Use the compact leaf representation if the virtual address encoding allows. jemalloc: RTREE_LEAF_COMPACT
inline constexpr bool RADIX_TREE_LEAF_COMPACT = RADIX_TREE_NUM_HIGH_INSIGNIFICANT_BITS >= log2CeilConst(SIZE_CLASS_NUM_SIZES);

/// jemalloc: rtree_level_t
struct RadixTreeLevel
{
    /// Number of key bits distinguished by this level.
    unsigned bits;
    /// Cumulative number of key bits distinguished by traversing to the corresponding tree level.
    unsigned cumulative_bits;
};

/// Split the bits into partitions by the number of levels. If the number of bits does not divide evenly into the
/// number of levels, place one remainder bit per level starting at the leaf level.
/// jemalloc: rtree_levels
inline constexpr std::array<RadixTreeLevel, RADIX_TREE_HEIGHT> radix_tree_levels = []
{
    if constexpr (RADIX_TREE_HEIGHT == 2)
    {
        return std::to_array<RadixTreeLevel>({
            {RADIX_TREE_NUM_SIGNIFICANT_BITS / 2, RADIX_TREE_NUM_HIGH_INSIGNIFICANT_BITS + RADIX_TREE_NUM_SIGNIFICANT_BITS / 2},
            {RADIX_TREE_NUM_SIGNIFICANT_BITS / 2 + RADIX_TREE_NUM_SIGNIFICANT_BITS % 2,
             RADIX_TREE_NUM_HIGH_INSIGNIFICANT_BITS + RADIX_TREE_NUM_SIGNIFICANT_BITS},
        });
    }
    else
    {
        return std::to_array<RadixTreeLevel>({
            {RADIX_TREE_NUM_SIGNIFICANT_BITS / 3, RADIX_TREE_NUM_HIGH_INSIGNIFICANT_BITS + RADIX_TREE_NUM_SIGNIFICANT_BITS / 3},
            {RADIX_TREE_NUM_SIGNIFICANT_BITS / 3 + RADIX_TREE_NUM_SIGNIFICANT_BITS % 3 / 2,
             RADIX_TREE_NUM_HIGH_INSIGNIFICANT_BITS + RADIX_TREE_NUM_SIGNIFICANT_BITS / 3 * 2 + RADIX_TREE_NUM_SIGNIFICANT_BITS % 3 / 2},
            {RADIX_TREE_NUM_SIGNIFICANT_BITS / 3 + RADIX_TREE_NUM_SIGNIFICANT_BITS % 3 - RADIX_TREE_NUM_SIGNIFICANT_BITS % 3 / 2,
             RADIX_TREE_NUM_HIGH_INSIGNIFICANT_BITS + RADIX_TREE_NUM_SIGNIFICANT_BITS},
        });
    }
}();

/// The number of low key bits that are not used to select a leaf. jemalloc: rtree_leaf_maskbits
ALLOCATOR_ALWAYS_INLINE constexpr unsigned radixTreeLeafMaskBits()
{
    unsigned pointer_bits = 1U << (LG_SIZEOF_PTR + 3);
    unsigned cumulative_bits = radix_tree_levels[RADIX_TREE_HEIGHT - 1].cumulative_bits - radix_tree_levels[RADIX_TREE_HEIGHT - 1].bits;
    return pointer_bits - cumulative_bits;
}

/// jemalloc: rtree_leafkey
ALLOCATOR_ALWAYS_INLINE constexpr uintptr_t radixTreeLeafKey(uintptr_t key)
{
    uintptr_t mask = ~((uintptr_t(1) << radixTreeLeafMaskBits()) - 1);
    return key & mask;
}

/// --- Per-thread lookup cache -----------------------------------------------------------------------------------------

/// Number of leafkey/leaf pairs to cache in L1 and L2 respectively. jemalloc: RTREE_CTX_NCACHE, RTREE_CTX_NCACHE_L2
inline constexpr unsigned RADIX_TREE_CONTEXT_NUM_CACHE = 16;
inline constexpr unsigned RADIX_TREE_CONTEXT_NUM_CACHE_L2 = 8;

/// jemalloc: RTREE_LEAFKEY_INVALID
inline constexpr uintptr_t RADIX_TREE_LEAF_KEY_INVALID = 1;

/// jemalloc: rtree_cache_direct_map
ALLOCATOR_ALWAYS_INLINE constexpr size_t radixTreeCacheDirectMap(uintptr_t key)
{
    return size_t((key >> radixTreeLeafMaskBits()) & (RADIX_TREE_CONTEXT_NUM_CACHE - 1));
}

/// jemalloc: rtree_subkey
ALLOCATOR_ALWAYS_INLINE constexpr uintptr_t radixTreeSubkey(uintptr_t key, unsigned level)
{
    unsigned pointer_bits = 1U << (LG_SIZEOF_PTR + 3);
    unsigned cumulative_bits = radix_tree_levels[level].cumulative_bits;
    unsigned shift_bits = pointer_bits - cumulative_bits;
    unsigned mask_bits = radix_tree_levels[level].bits;
    uintptr_t mask = (uintptr_t(1) << mask_bits) - 1;
    return (key >> shift_bits) & mask;
}

/// A compact leaf element: a single pointer-width word (see `RadixTreeLeafPolicy<true>`).
struct RadixTreeLeafElementCompact
{
    uintptr_t le_bits; /// Atomic.
};

/// A non-compact leaf element: the edata pointer and a metadata word (see `RadixTreeLeafPolicy<false>`).
struct RadixTreeLeafElementWide
{
    Extent * le_extent; /// Atomic.
    unsigned le_metadata; /// Atomic.
};

/// jemalloc: rtree_leaf_elm_t
using RadixTreeLeafElement = std::conditional_t<RADIX_TREE_LEAF_COMPACT, RadixTreeLeafElementCompact, RadixTreeLeafElementWide>;

/// The operations on a leaf element; specialized below for the two encodings.
template <bool compact>
struct RadixTreeLeafPolicy;

/// jemalloc: rtree_node_elm_t
struct RadixTreeNodeElement
{
    void * child; /// Atomic: `RadixTreeNodeElement *` or `RadixTreeLeafElement *`.
};

/// jemalloc: rtree_ctx_cache_elm_t
struct RadixTreeCacheElement
{
    uintptr_t leaf_key;
    RadixTreeLeafElement * leaf;
};

/// jemalloc: rtree_ctx_t
struct RadixTreeContext
{
    /// Direct mapped cache.
    RadixTreeCacheElement cache[RADIX_TREE_CONTEXT_NUM_CACHE];
    /// L2 LRU cache.
    RadixTreeCacheElement l2_cache[RADIX_TREE_CONTEXT_NUM_CACHE_L2];

    /// A static initializer (to invalidate the cache entries) is required because the free fast path may access the
    /// rtree cache before a full tsd initialization.
    /// jemalloc: RTREE_CTX_INITIALIZER
    constexpr RadixTreeContext()
    {
        for (auto & element : cache)
            element = {RADIX_TREE_LEAF_KEY_INVALID, nullptr};
        for (auto & element : l2_cache)
            element = {RADIX_TREE_LEAF_KEY_INVALID, nullptr};
    }

    /// Leaves the caches uninitialized: for the on-stack fallback of `threadStateRadixTreeContext`, which initializes it only when it
    /// is used (like the uninitialized `radix_tree_context_fallback` in `EMAP_DECLARE_RTREE_CTX`).
    struct NoInit
    {
    };
    explicit RadixTreeContext(NoInit) { }

    /// jemalloc: rtree_ctx_data_init
    void init();
};

static_assert(sizeof(RadixTreeContext) == 384, "Must have the size of rtree_ctx_t");

/// --- Contents ------------------------------------------------------------------------------------------------------

/// jemalloc: rtree_metadata_t
struct RadixTreeMetadata
{
    SizeClassIdx size_class_idx;
    ExtentState state; /// Mirrors `Extent::state`.
    bool is_head; /// Mirrors `Extent::isHead`.
    bool slab;
};

/// jemalloc: rtree_contents_t
struct RadixTreeContents
{
    Extent * extent;
    RadixTreeMetadata metadata;
};

/// jemalloc: RTREE_LEAF_STATE_WIDTH, RTREE_LEAF_STATE_SHIFT, RTREE_LEAF_STATE_MASK
inline constexpr unsigned RADIX_TREE_LEAF_STATE_WIDTH = extent_bits::state.width;
inline constexpr unsigned RADIX_TREE_LEAF_STATE_SHIFT = 2;
inline constexpr uintptr_t RADIX_TREE_LEAF_STATE_MASK = ((uintptr_t(1) << RADIX_TREE_LEAF_STATE_WIDTH) - 1) << RADIX_TREE_LEAF_STATE_SHIFT;

/// The encoded form of the contents, as written by `leafElementWriteCommit`: `bits` is the word (compact) or the edata
/// pointer, `additional` is the metadata word (non-compact only).
struct RadixTreeEncoded
{
    uintptr_t bits;
    unsigned additional;
};

/// LOG2_VIRTUAL_ADDRESS for the compact encoding. The compact encoding functions below are only used when `RADIX_TREE_LEAF_COMPACT`
/// (LOG2_VIRTUAL_ADDRESS < 64); the clamp only avoids shift-count warnings when they are compiled but unused.
inline constexpr unsigned RADIX_TREE_COMPACT_LOG2_VIRTUAL_ADDRESS = RADIX_TREE_LEAF_COMPACT ? LOG2_VIRTUAL_ADDRESS : 0;

/// jemalloc: rtree_leaf_elm_bits_encode
ALLOCATOR_ALWAYS_INLINE constexpr uintptr_t radixTreeLeafElementBitsEncode(RadixTreeContents contents)
{
    ALLOCATOR_ASSERT(std::bit_cast<uintptr_t>(contents.extent) % uintptr_t(EXTENT_ALIGNMENT) == 0);
    uintptr_t extent_bits = std::bit_cast<uintptr_t>(contents.extent) & ((uintptr_t(1) << RADIX_TREE_COMPACT_LOG2_VIRTUAL_ADDRESS) - 1);

    uintptr_t size_class_idx_bits = uintptr_t(contents.metadata.size_class_idx) << RADIX_TREE_COMPACT_LOG2_VIRTUAL_ADDRESS;
    uintptr_t slab_bits = uintptr_t(contents.metadata.slab);
    uintptr_t is_head_bits = uintptr_t(contents.metadata.is_head) << 1;
    uintptr_t state_bits = uintptr_t(contents.metadata.state) << RADIX_TREE_LEAF_STATE_SHIFT;
    uintptr_t metadata_bits = size_class_idx_bits | state_bits | is_head_bits | slab_bits;
    ALLOCATOR_ASSERT((extent_bits & metadata_bits) == 0);

    return extent_bits | metadata_bits;
}

/// jemalloc: rtree_leaf_elm_bits_decode
ALLOCATOR_ALWAYS_INLINE constexpr RadixTreeContents radixTreeLeafElementBitsDecode(uintptr_t bits)
{
    RadixTreeContents contents;
    /// Do the easy things first.
    contents.metadata.size_class_idx = SizeClassIdx(bits >> RADIX_TREE_COMPACT_LOG2_VIRTUAL_ADDRESS);
    contents.metadata.slab = bool(bits & 1);
    contents.metadata.is_head = bool(bits & (1 << 1));

    uintptr_t state_bits = (bits & RADIX_TREE_LEAF_STATE_MASK) >> RADIX_TREE_LEAF_STATE_SHIFT;
    ALLOCATOR_ASSERT(state_bits <= extent_state_max);
    contents.metadata.state = ExtentState(state_bits);

    uintptr_t low_bit_mask = ~(uintptr_t(EXTENT_ALIGNMENT) - 1);
    if constexpr (config::arch == Arch::AArch64)
    {
        /// aarch64 doesn't sign extend the highest virtual address bit to set the higher ones. Instead, the high bits
        /// get zeroed.
        uintptr_t high_bit_mask = (uintptr_t(1) << RADIX_TREE_COMPACT_LOG2_VIRTUAL_ADDRESS) - 1;
        /// Mask off metadata.
        uintptr_t mask = high_bit_mask & low_bit_mask;
        contents.extent = std::bit_cast<Extent *>(bits & mask);
    }
    else
    {
        /// Restore sign-extended high bits, mask metadata bits.
        contents.extent = std::bit_cast<Extent *>(
            uintptr_t(static_cast<intptr_t>(bits << RADIX_TREE_NUM_HIGH_INSIGNIFICANT_BITS) >> RADIX_TREE_NUM_HIGH_INSIGNIFICANT_BITS)
            & low_bit_mask);
    }
    ALLOCATOR_ASSERT(std::bit_cast<uintptr_t>(contents.extent) % uintptr_t(EXTENT_ALIGNMENT) == 0);
    return contents;
}

/// jemalloc: rtree_contents_encode
ALLOCATOR_ALWAYS_INLINE constexpr RadixTreeEncoded radixTreeContentsEncode(RadixTreeContents contents)
{
    RadixTreeEncoded encoded{};
    if constexpr (RADIX_TREE_LEAF_COMPACT)
    {
        encoded.bits = radixTreeLeafElementBitsEncode(contents);
        encoded.additional = 0;
    }
    else
    {
        encoded.additional = unsigned(contents.metadata.slab) | (unsigned(contents.metadata.is_head) << 1)
            | (unsigned(contents.metadata.state) << RADIX_TREE_LEAF_STATE_SHIFT)
            | (unsigned(contents.metadata.size_class_idx) << (RADIX_TREE_LEAF_STATE_SHIFT + RADIX_TREE_LEAF_STATE_WIDTH));
        encoded.bits = std::bit_cast<uintptr_t>(contents.extent);
    }
    return encoded;
}

/// Atomic getters (`read` of both policies).
///
/// dependent: Reading a value on behalf of a pointer to a valid allocation is guaranteed to be a clean read even
///            without synchronization, because the rtree update became visible in memory before the pointer came
///            into existence.
/// !dependent: An arbitrary read, e.g. on behalf of `allocationSizeIfOwned`, may not be dependent on a previous rtree write, which
///             means a stale read could result if synchronization were omitted here.

/// Compact: a single pointer-width word. On 64-bit with 48 significant address bits:
///
///   x: szind, e: edata, s: state, h: is_head, b: slab
///   00000000 xxxxxxxx eeeeeeee [...] eeeeeeee e00ssshb
template <>
struct RadixTreeLeafPolicy<true>
{
    using Element = RadixTreeLeafElementCompact;

    /// jemalloc: rtree_leaf_elm_bits_read
    static ALLOCATOR_ALWAYS_INLINE uintptr_t bitsRead(ThreadState * /*tsdn*/, Element * element, bool dependent)
    {
        return std::atomic_ref<uintptr_t>(element->le_bits).load(dependent ? std::memory_order_relaxed : std::memory_order_acquire);
    }

    /// jemalloc: rtree_leaf_elm_read
    static ALLOCATOR_ALWAYS_INLINE RadixTreeContents read(ThreadState * thread_state, Element * element, bool dependent)
    {
        uintptr_t bits = bitsRead(thread_state, element, dependent);
        return radixTreeLeafElementBitsDecode(bits);
    }

    /// jemalloc: rtree_leaf_elm_write_commit
    static ALLOCATOR_ALWAYS_INLINE void writeCommit(ThreadState * /*tsdn*/, Element * element, RadixTreeEncoded encoded)
    {
        std::atomic_ref<uintptr_t>(element->le_bits).store(encoded.bits, std::memory_order_release);
    }

    /// jemalloc: rtree_leaf_elm_state_update
    static ALLOCATOR_ALWAYS_INLINE void stateUpdate(ThreadState * thread_state, Element * element1, Element * element2, ExtentState state)
    {
        ALLOCATOR_ASSERT(element1 != nullptr);
        uintptr_t bits = bitsRead(thread_state, element1, /* dependent */ true);
        bits &= ~RADIX_TREE_LEAF_STATE_MASK;
        bits |= uintptr_t(state) << RADIX_TREE_LEAF_STATE_SHIFT;
        std::atomic_ref<uintptr_t>(element1->le_bits).store(bits, std::memory_order_release);
        if (element2 != nullptr)
            std::atomic_ref<uintptr_t>(element2->le_bits).store(bits, std::memory_order_release);
    }
};

/// Non-compact: the edata pointer and a metadata word with, from high to low bits: szind, state, is_head, slab.
template <>
struct RadixTreeLeafPolicy<false>
{
    using Element = RadixTreeLeafElementWide;

    /// jemalloc: rtree_leaf_elm_read
    static ALLOCATOR_ALWAYS_INLINE RadixTreeContents read(ThreadState * /*tsdn*/, Element * element, bool dependent)
    {
        RadixTreeContents contents;
        unsigned metadata_bits
            = std::atomic_ref<unsigned>(element->le_metadata).load(dependent ? std::memory_order_relaxed : std::memory_order_acquire);
        contents.metadata.slab = bool(metadata_bits & 1);
        contents.metadata.is_head = bool(metadata_bits & (1 << 1));

        uintptr_t state_bits = (metadata_bits & RADIX_TREE_LEAF_STATE_MASK) >> RADIX_TREE_LEAF_STATE_SHIFT;
        ALLOCATOR_ASSERT(state_bits <= extent_state_max);
        contents.metadata.state = ExtentState(state_bits);
        contents.metadata.size_class_idx = metadata_bits >> (RADIX_TREE_LEAF_STATE_SHIFT + RADIX_TREE_LEAF_STATE_WIDTH);

        contents.extent
            = std::atomic_ref<Extent *>(element->le_extent).load(dependent ? std::memory_order_relaxed : std::memory_order_acquire);
        return contents;
    }

    /// jemalloc: rtree_leaf_elm_write_commit
    static ALLOCATOR_ALWAYS_INLINE void writeCommit(ThreadState * /*tsdn*/, Element * element, RadixTreeEncoded encoded)
    {
        std::atomic_ref<unsigned>(element->le_metadata).store(encoded.additional, std::memory_order_release);
        /// Write edata last, since the element is atomically considered valid as soon as the edata field is non-null.
        std::atomic_ref<Extent *>(element->le_extent).store(std::bit_cast<Extent *>(encoded.bits), std::memory_order_release);
    }

    /// jemalloc: rtree_leaf_elm_state_update
    static ALLOCATOR_ALWAYS_INLINE void stateUpdate(ThreadState * /*tsdn*/, Element * element1, Element * element2, ExtentState state)
    {
        ALLOCATOR_ASSERT(element1 != nullptr);
        unsigned bits = std::atomic_ref<unsigned>(element1->le_metadata).load(std::memory_order_relaxed);
        bits &= ~unsigned(RADIX_TREE_LEAF_STATE_MASK);
        bits |= unsigned(state) << RADIX_TREE_LEAF_STATE_SHIFT;
        std::atomic_ref<unsigned>(element1->le_metadata).store(bits, std::memory_order_release);
        if (element2 != nullptr)
            std::atomic_ref<unsigned>(element2->le_metadata).store(bits, std::memory_order_release);
    }
};

/// The contents of an element written by `clear` / `clearRange`.
inline constexpr RadixTreeContents radix_tree_contents_cleared = {nullptr, {SIZE_CLASS_NUM_SIZES, ExtentState(0), false, false}};

/// --- The tree --------------------------------------------------------------------------------------------------------

/// jemalloc: rtree_t
class RadixTree
{
public:
    /// The tree is zero-initialized; `init` must be called before use.
    constexpr RadixTree() = default;

    RadixTree(const RadixTree &) = delete;
    RadixTree & operator=(const RadixTree &) = delete;

    /// Only the most significant bits of keys passed to read/write are used. `zeroed` must be true (the root array
    /// is expected to be zero). Returns true on error.
    /// jemalloc: rtree_new
    bool init(Base * base_, bool zeroed);

    /// --- Element access ------------------------------------------------------------------------------------------

    /// jemalloc: rtree_leaf_elm_read
    static ALLOCATOR_ALWAYS_INLINE RadixTreeContents
    leafElementRead(ThreadState * thread_state, RadixTreeLeafElement * element, bool dependent)
    {
        return RadixTreeLeafPolicy<RADIX_TREE_LEAF_COMPACT>::read(thread_state, element, dependent);
    }

    /// jemalloc: rtree_leaf_elm_write_commit
    static ALLOCATOR_ALWAYS_INLINE void
    leafElementWriteCommit(ThreadState * thread_state, RadixTreeLeafElement * element, RadixTreeEncoded encoded)
    {
        RadixTreeLeafPolicy<RADIX_TREE_LEAF_COMPACT>::writeCommit(thread_state, element, encoded);
    }

    /// jemalloc: rtree_leaf_elm_write
    static ALLOCATOR_ALWAYS_INLINE void
    leafElementWrite(ThreadState * thread_state, RadixTreeLeafElement * element, RadixTreeContents contents)
    {
        ALLOCATOR_ASSERT(std::bit_cast<uintptr_t>(contents.extent) % EXTENT_ALIGNMENT == 0);
        leafElementWriteCommit(thread_state, element, radixTreeContentsEncode(contents));
    }

    /// The state field can be updated independently (and more frequently).
    /// jemalloc: rtree_leaf_elm_state_update
    static ALLOCATOR_ALWAYS_INLINE void
    leafElementStateUpdate(ThreadState * thread_state, RadixTreeLeafElement * element1, RadixTreeLeafElement * element2, ExtentState state)
    {
        RadixTreeLeafPolicy<RADIX_TREE_LEAF_COMPACT>::stateUpdate(thread_state, element1, element2, state);
    }

    /// --- Lookup ----------------------------------------------------------------------------------------------------

    /// Tries to look up the key in the L1 cache, returning false if there's a hit, or true if there's a miss.
    /// The key is allowed to be 0; returns true in this case.
    /// jemalloc: rtree_leaf_elm_lookup_fast
    ALLOCATOR_ALWAYS_INLINE bool
    leafElementLookupFast(ThreadState * /*tsdn*/, RadixTreeContext * radix_tree_context, uintptr_t key, RadixTreeLeafElement ** element)
    {
        size_t slot = radixTreeCacheDirectMap(key);
        uintptr_t leaf_key = radixTreeLeafKey(key);
        ALLOCATOR_ASSERT(leaf_key != RADIX_TREE_LEAF_KEY_INVALID);

        if (ALLOCATOR_UNLIKELY(radix_tree_context->cache[slot].leaf_key != leaf_key))
            return true;

        RadixTreeLeafElement * leaf = radix_tree_context->cache[slot].leaf;
        ALLOCATOR_ASSERT(leaf != nullptr);
        uintptr_t subkey = radixTreeSubkey(key, RADIX_TREE_HEIGHT - 1);
        *element = &leaf[subkey];

        return false;
    }

    /// jemalloc: rtree_leaf_elm_lookup
    ALLOCATOR_ALWAYS_INLINE RadixTreeLeafElement *
    leafElementLookup(ThreadState * thread_state, RadixTreeContext * radix_tree_context, uintptr_t key, bool dependent, bool init_missing)
    {
        ALLOCATOR_ASSERT(key != 0);
        ALLOCATOR_ASSERT(!dependent || !init_missing);

        size_t slot = radixTreeCacheDirectMap(key);
        uintptr_t leaf_key = radixTreeLeafKey(key);
        ALLOCATOR_ASSERT(leaf_key != RADIX_TREE_LEAF_KEY_INVALID);

        /// Fast path: L1 direct mapped cache.
        if (ALLOCATOR_LIKELY(radix_tree_context->cache[slot].leaf_key == leaf_key))
        {
            RadixTreeLeafElement * leaf = radix_tree_context->cache[slot].leaf;
            ALLOCATOR_ASSERT(leaf != nullptr);
            uintptr_t subkey = radixTreeSubkey(key, RADIX_TREE_HEIGHT - 1);
            return &leaf[subkey];
        }

        /// Search the L2 LRU cache. On hit, swap the matching element into the slot in L1 cache, and move the
        /// position in L2 up by 1.
        auto check_l2 = [&](unsigned i) __attribute__((always_inline)) -> RadixTreeLeafElement *
        {
            if (ALLOCATOR_LIKELY(radix_tree_context->l2_cache[i].leaf_key == leaf_key))
            {
                RadixTreeLeafElement * leaf = radix_tree_context->l2_cache[i].leaf;
                ALLOCATOR_ASSERT(leaf != nullptr);
                if (i > 0)
                {
                    /// Bubble up by one.
                    radix_tree_context->l2_cache[i].leaf_key = radix_tree_context->l2_cache[i - 1].leaf_key;
                    radix_tree_context->l2_cache[i].leaf = radix_tree_context->l2_cache[i - 1].leaf;
                    radix_tree_context->l2_cache[i - 1].leaf_key = radix_tree_context->cache[slot].leaf_key;
                    radix_tree_context->l2_cache[i - 1].leaf = radix_tree_context->cache[slot].leaf;
                }
                else
                {
                    radix_tree_context->l2_cache[0].leaf_key = radix_tree_context->cache[slot].leaf_key;
                    radix_tree_context->l2_cache[0].leaf = radix_tree_context->cache[slot].leaf;
                }
                radix_tree_context->cache[slot].leaf_key = leaf_key;
                radix_tree_context->cache[slot].leaf = leaf;
                uintptr_t subkey = radixTreeSubkey(key, RADIX_TREE_HEIGHT - 1);
                return &leaf[subkey];
            }
            return nullptr;
        };

        /// Check the first cache entry.
        if (RadixTreeLeafElement * element = check_l2(0))
            return element;
        /// Search the remaining cache elements.
        for (unsigned i = 1; i < RADIX_TREE_CONTEXT_NUM_CACHE_L2; ++i)
            if (RadixTreeLeafElement * element = check_l2(i))
                return element;

        return leafElementLookupHard(thread_state, radix_tree_context, key, dependent, init_missing);
    }

    /// The lookup after a miss in both caches: walk the tree (allocating missing nodes if `init_missing`), then
    /// (1) evict the last entry of L2, (2) move the colliding L1 slot down to L2, (3) fill L1.
    /// A lookup that ends with null does not touch the cache.
    /// jemalloc: rtree_leaf_elm_lookup_hard
    RadixTreeLeafElement * leafElementLookupHard(
        ThreadState * thread_state, RadixTreeContext * radix_tree_context, uintptr_t key, bool dependent, bool init_missing);

    /// --- Read / write ----------------------------------------------------------------------------------------------

    /// Returns true on lookup failure.
    /// jemalloc: rtree_read_independent
    ALLOCATOR_ALWAYS_INLINE bool
    readIndependent(ThreadState * thread_state, RadixTreeContext * radix_tree_context, uintptr_t key, RadixTreeContents * result_contents)
    {
        RadixTreeLeafElement * element
            = leafElementLookup(thread_state, radix_tree_context, key, /* dependent */ false, /* init_missing */ false);
        if (element == nullptr)
            return true;
        *result_contents = leafElementRead(thread_state, element, /* dependent */ false);
        return false;
    }

    /// jemalloc: rtree_read
    ALLOCATOR_ALWAYS_INLINE RadixTreeContents read(ThreadState * thread_state, RadixTreeContext * radix_tree_context, uintptr_t key)
    {
        RadixTreeLeafElement * element
            = leafElementLookup(thread_state, radix_tree_context, key, /* dependent */ true, /* init_missing */ false);
        ALLOCATOR_ASSERT(element != nullptr);
        return leafElementRead(thread_state, element, /* dependent */ true);
    }

    /// jemalloc: rtree_metadata_read
    ALLOCATOR_ALWAYS_INLINE RadixTreeMetadata metadataRead(ThreadState * thread_state, RadixTreeContext * radix_tree_context, uintptr_t key)
    {
        RadixTreeLeafElement * element
            = leafElementLookup(thread_state, radix_tree_context, key, /* dependent */ true, /* init_missing */ false);
        ALLOCATOR_ASSERT(element != nullptr);
        return leafElementRead(thread_state, element, /* dependent */ true).metadata;
    }

    /// Returns true when the request cannot be fulfilled by the fast path (L1 cache only).
    /// jemalloc: rtree_metadata_try_read_fast
    ALLOCATOR_ALWAYS_INLINE bool metadataTryReadFast(
        ThreadState * thread_state, RadixTreeContext * radix_tree_context, uintptr_t key, RadixTreeMetadata * r_radix_tree_metadata)
    {
        RadixTreeLeafElement * element;
        /// Check the bool return value instead of elm == null (which would result in an extra branch), because a
        /// cache hit never returns null (which is unknown to the compiler).
        if (leafElementLookupFast(thread_state, radix_tree_context, key, &element))
            return true;
        ALLOCATOR_ASSERT(element != nullptr);
        *r_radix_tree_metadata = leafElementRead(thread_state, element, /* dependent */ true).metadata;
        return false;
    }

    /// jemalloc: rtree_write_range_impl
    ALLOCATOR_ALWAYS_INLINE void writeRangeImpl(
        ThreadState * thread_state,
        RadixTreeContext * radix_tree_context,
        uintptr_t base_addr,
        uintptr_t end,
        RadixTreeContents contents,
        [[maybe_unused]] bool clearing)
    {
        ALLOCATOR_ASSERT((base_addr & PAGE_MASK) == 0 && (end & PAGE_MASK) == 0);
        /// Only used for `extent_map_(de)register_interior`, which implies the boundaries have been registered already.
        /// Therefore all the lookups are dependent without init_missing, assuming the range spans across at most 2
        /// rtree leaf nodes (each covers 1 GiB of vaddr).
        RadixTreeEncoded encoded = radixTreeContentsEncode(contents);

        RadixTreeLeafElement * element = nullptr; /// Dead store.
        for (uintptr_t addr = base_addr; addr <= end; addr += PAGE)
        {
            if (addr == base_addr || (addr & ((uintptr_t(1) << radixTreeLeafMaskBits()) - 1)) == 0)
            {
                element = leafElementLookup(thread_state, radix_tree_context, addr, /* dependent */ true, /* init_missing */ false);
                ALLOCATOR_ASSERT(element != nullptr);
            }
            ALLOCATOR_ASSERT(
                element == leafElementLookup(thread_state, radix_tree_context, addr, /* dependent */ true, /* init_missing */ false));
            ALLOCATOR_ASSERT(!clearing || leafElementRead(thread_state, element, /* dependent */ true).extent != nullptr);
            leafElementWriteCommit(thread_state, element, encoded);
            ++element;
        }
    }

    /// jemalloc: rtree_write_range
    ALLOCATOR_ALWAYS_INLINE void writeRange(
        ThreadState * thread_state, RadixTreeContext * radix_tree_context, uintptr_t base_addr, uintptr_t end, RadixTreeContents contents)
    {
        writeRangeImpl(thread_state, radix_tree_context, base_addr, end, contents, /* clearing */ false);
    }

    /// Returns true on error (failure to allocate a node).
    /// jemalloc: rtree_write
    ALLOCATOR_ALWAYS_INLINE bool
    write(ThreadState * thread_state, RadixTreeContext * radix_tree_context, uintptr_t key, RadixTreeContents contents)
    {
        RadixTreeLeafElement * element
            = leafElementLookup(thread_state, radix_tree_context, key, /* dependent */ false, /* init_missing */ true);
        if (element == nullptr)
            return true;

        leafElementWrite(thread_state, element, contents);
        return false;
    }

    /// jemalloc: rtree_clear
    ALLOCATOR_ALWAYS_INLINE void clear(ThreadState * thread_state, RadixTreeContext * radix_tree_context, uintptr_t key)
    {
        RadixTreeLeafElement * element
            = leafElementLookup(thread_state, radix_tree_context, key, /* dependent */ true, /* init_missing */ false);
        ALLOCATOR_ASSERT(element != nullptr);
        ALLOCATOR_ASSERT(leafElementRead(thread_state, element, /* dependent */ true).extent != nullptr);
        leafElementWrite(thread_state, element, radix_tree_contents_cleared);
    }

    /// jemalloc: rtree_clear_range
    ALLOCATOR_ALWAYS_INLINE void
    clearRange(ThreadState * thread_state, RadixTreeContext * radix_tree_context, uintptr_t base_addr, uintptr_t end)
    {
        writeRangeImpl(thread_state, radix_tree_context, base_addr, end, radix_tree_contents_cleared, /* clearing */ true);
    }

    /// The number of elements of the root (`rtree_levels[0].bits`).
    static constexpr size_t root_size = size_t(1) << (RADIX_TREE_NUM_SIGNIFICANT_BITS / RADIX_TREE_HEIGHT);

    Base * base = nullptr;
    Mutex init_lock;
    /// The root node (an interior node, since the height is at least 2).
    RadixTreeNodeElement root[root_size] = {};

private:
    /// jemalloc: rtree_node_alloc
    RadixTreeNodeElement * nodeAlloc(ThreadState * thread_state, size_t num_elements);
    /// jemalloc: rtree_leaf_alloc
    RadixTreeLeafElement * leafAlloc(ThreadState * thread_state, size_t num_elements);
    /// jemalloc: rtree_node_init
    RadixTreeNodeElement * nodeInit(ThreadState * thread_state, unsigned level, void ** element_ptr);
    /// jemalloc: rtree_leaf_init
    RadixTreeLeafElement * leafInit(ThreadState * thread_state, void ** element_ptr);
    /// jemalloc: rtree_child_node_read
    RadixTreeNodeElement * childNodeRead(ThreadState * thread_state, RadixTreeNodeElement * element, unsigned level, bool dependent);
    /// jemalloc: rtree_child_leaf_read
    RadixTreeLeafElement * childLeafRead(ThreadState * thread_state, RadixTreeNodeElement * element, unsigned level, bool dependent);
};

static_assert(std::is_standard_layout_v<RadixTree>);

}
