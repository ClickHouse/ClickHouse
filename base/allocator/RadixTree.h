#pragma once

/// The radix tree that maps page addresses to extent metadata (jemalloc: `rtree.h`, `rtree_tsd.h`, `rtree.c`).
///
/// The geometry (height, bits per level) is computed at compile time from LG_VADDR and LG_PAGE. The leaf element is
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
inline constexpr unsigned RTREE_NHIB = (1U << (LG_SIZEOF_PTR + 3)) - LG_VADDR;
/// Number of low insignificant bits. jemalloc: RTREE_NLIB
inline constexpr unsigned RTREE_NLIB = LG_PAGE;
/// Number of significant bits. jemalloc: RTREE_NSB
inline constexpr unsigned RTREE_NSB = LG_VADDR - RTREE_NLIB;
/// Number of levels in the radix tree. jemalloc: RTREE_HEIGHT
inline constexpr unsigned RTREE_HEIGHT = RTREE_NSB <= 10 ? 1 : (RTREE_NSB <= 36 ? 2 : 3);
static_assert(RTREE_NSB <= 52, "Unsupported number of significant virtual address bits");
static_assert(RTREE_HEIGHT == 2 || RTREE_HEIGHT == 3, "Only heights 2 and 3 are reachable on supported platforms");

/// Use the compact leaf representation if the virtual address encoding allows. jemalloc: RTREE_LEAF_COMPACT
inline constexpr bool RTREE_LEAF_COMPACT = RTREE_NHIB >= lgCeilConst(SC_NSIZES);

/// jemalloc: rtree_level_t
struct RadixTreeLevel
{
    /// Number of key bits distinguished by this level.
    unsigned bits;
    /// Cumulative number of key bits distinguished by traversing to the corresponding tree level.
    unsigned cumbits;
};

/// Split the bits into partitions by the number of levels. If the number of bits does not divide evenly into the
/// number of levels, place one remainder bit per level starting at the leaf level.
/// jemalloc: rtree_levels
inline constexpr std::array<RadixTreeLevel, RTREE_HEIGHT> rtree_levels = []
{
    if constexpr (RTREE_HEIGHT == 2)
    {
        return std::to_array<RadixTreeLevel>({
            {RTREE_NSB / 2, RTREE_NHIB + RTREE_NSB / 2},
            {RTREE_NSB / 2 + RTREE_NSB % 2, RTREE_NHIB + RTREE_NSB},
        });
    }
    else
    {
        return std::to_array<RadixTreeLevel>({
            {RTREE_NSB / 3, RTREE_NHIB + RTREE_NSB / 3},
            {RTREE_NSB / 3 + RTREE_NSB % 3 / 2, RTREE_NHIB + RTREE_NSB / 3 * 2 + RTREE_NSB % 3 / 2},
            {RTREE_NSB / 3 + RTREE_NSB % 3 - RTREE_NSB % 3 / 2, RTREE_NHIB + RTREE_NSB},
        });
    }
}();

/// The number of low key bits that are not used to select a leaf. jemalloc: rtree_leaf_maskbits
JE_ALWAYS_INLINE constexpr unsigned rtreeLeafMaskbits()
{
    unsigned ptrbits = 1U << (LG_SIZEOF_PTR + 3);
    unsigned cumbits = rtree_levels[RTREE_HEIGHT - 1].cumbits - rtree_levels[RTREE_HEIGHT - 1].bits;
    return ptrbits - cumbits;
}

/// jemalloc: rtree_leafkey
JE_ALWAYS_INLINE constexpr uintptr_t rtreeLeafkey(uintptr_t key)
{
    uintptr_t mask = ~((uintptr_t(1) << rtreeLeafMaskbits()) - 1);
    return key & mask;
}

/// --- Per-thread lookup cache -----------------------------------------------------------------------------------------

/// Number of leafkey/leaf pairs to cache in L1 and L2 respectively. jemalloc: RTREE_CTX_NCACHE, RTREE_CTX_NCACHE_L2
inline constexpr unsigned RTREE_CTX_NCACHE = 16;
inline constexpr unsigned RTREE_CTX_NCACHE_L2 = 8;

/// jemalloc: RTREE_LEAFKEY_INVALID
inline constexpr uintptr_t RTREE_LEAFKEY_INVALID = 1;

/// jemalloc: rtree_cache_direct_map
JE_ALWAYS_INLINE constexpr size_t rtreeCacheDirectMap(uintptr_t key)
{
    return size_t((key >> rtreeLeafMaskbits()) & (RTREE_CTX_NCACHE - 1));
}

/// jemalloc: rtree_subkey
JE_ALWAYS_INLINE constexpr uintptr_t rtreeSubkey(uintptr_t key, unsigned level)
{
    unsigned ptrbits = 1U << (LG_SIZEOF_PTR + 3);
    unsigned cumbits = rtree_levels[level].cumbits;
    unsigned shiftbits = ptrbits - cumbits;
    unsigned maskbits = rtree_levels[level].bits;
    uintptr_t mask = (uintptr_t(1) << maskbits) - 1;
    return (key >> shiftbits) & mask;
}

/// A compact leaf element: a single pointer-width word (see `RadixTreeLeafPolicy<true>`).
struct RadixTreeLeafElmCompact
{
    uintptr_t le_bits; /// Atomic.
};

/// A non-compact leaf element: the edata pointer and a metadata word (see `RadixTreeLeafPolicy<false>`).
struct RadixTreeLeafElmWide
{
    Extent * le_edata; /// Atomic.
    unsigned le_metadata; /// Atomic.
};

/// jemalloc: rtree_leaf_elm_t
using RadixTreeLeafElm = std::conditional_t<RTREE_LEAF_COMPACT, RadixTreeLeafElmCompact, RadixTreeLeafElmWide>;

/// The operations on a leaf element; specialized below for the two encodings.
template <bool compact>
struct RadixTreeLeafPolicy;

/// jemalloc: rtree_node_elm_t
struct RadixTreeNodeElm
{
    void * child; /// Atomic: `RadixTreeNodeElm *` or `RadixTreeLeafElm *`.
};

/// jemalloc: rtree_ctx_cache_elm_t
struct RadixTreeCacheElm
{
    uintptr_t leafkey;
    RadixTreeLeafElm * leaf;
};

/// jemalloc: rtree_ctx_t
struct RadixTreeContext
{
    /// Direct mapped cache.
    RadixTreeCacheElm cache[RTREE_CTX_NCACHE];
    /// L2 LRU cache.
    RadixTreeCacheElm l2_cache[RTREE_CTX_NCACHE_L2];

    /// A static initializer (to invalidate the cache entries) is required because the free fast path may access the
    /// rtree cache before a full tsd initialization.
    /// jemalloc: RTREE_CTX_INITIALIZER
    constexpr RadixTreeContext()
    {
        for (auto & elm : cache)
            elm = {RTREE_LEAFKEY_INVALID, nullptr};
        for (auto & elm : l2_cache)
            elm = {RTREE_LEAFKEY_INVALID, nullptr};
    }

    /// Leaves the caches uninitialized: for the on-stack fallback of `tsdnRtreeCtx`, which initializes it only when it
    /// is used (like the uninitialized `rtree_ctx_fallback` in `EMAP_DECLARE_RTREE_CTX`).
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
    szind_t szind;
    ExtentState state; /// Mirrors `Extent::state`.
    bool is_head; /// Mirrors `Extent::isHead`.
    bool slab;
};

/// jemalloc: rtree_contents_t
struct RadixTreeContents
{
    Extent * edata;
    RadixTreeMetadata metadata;
};

/// jemalloc: RTREE_LEAF_STATE_WIDTH, RTREE_LEAF_STATE_SHIFT, RTREE_LEAF_STATE_MASK
inline constexpr unsigned RTREE_LEAF_STATE_WIDTH = extent_bits::state.width;
inline constexpr unsigned RTREE_LEAF_STATE_SHIFT = 2;
inline constexpr uintptr_t RTREE_LEAF_STATE_MASK = ((uintptr_t(1) << RTREE_LEAF_STATE_WIDTH) - 1) << RTREE_LEAF_STATE_SHIFT;

/// The encoded form of the contents, as written by `leafElmWriteCommit`: `bits` is the word (compact) or the edata
/// pointer, `additional` is the metadata word (non-compact only).
struct RadixTreeEncoded
{
    uintptr_t bits;
    unsigned additional;
};

/// LG_VADDR for the compact encoding. The compact encoding functions below are only used when `RTREE_LEAF_COMPACT`
/// (LG_VADDR < 64); the clamp only avoids shift-count warnings when they are compiled but unused.
inline constexpr unsigned RTREE_COMPACT_LG_VADDR = RTREE_LEAF_COMPACT ? LG_VADDR : 0;

/// jemalloc: rtree_leaf_elm_bits_encode
JE_ALWAYS_INLINE constexpr uintptr_t rtreeLeafElmBitsEncode(RadixTreeContents contents)
{
    JE_ASSERT(std::bit_cast<uintptr_t>(contents.edata) % uintptr_t(EDATA_ALIGNMENT) == 0);
    uintptr_t edata_bits = std::bit_cast<uintptr_t>(contents.edata) & ((uintptr_t(1) << RTREE_COMPACT_LG_VADDR) - 1);

    uintptr_t szind_bits = uintptr_t(contents.metadata.szind) << RTREE_COMPACT_LG_VADDR;
    uintptr_t slab_bits = uintptr_t(contents.metadata.slab);
    uintptr_t is_head_bits = uintptr_t(contents.metadata.is_head) << 1;
    uintptr_t state_bits = uintptr_t(contents.metadata.state) << RTREE_LEAF_STATE_SHIFT;
    uintptr_t metadata_bits = szind_bits | state_bits | is_head_bits | slab_bits;
    JE_ASSERT((edata_bits & metadata_bits) == 0);

    return edata_bits | metadata_bits;
}

/// jemalloc: rtree_leaf_elm_bits_decode
JE_ALWAYS_INLINE constexpr RadixTreeContents rtreeLeafElmBitsDecode(uintptr_t bits)
{
    RadixTreeContents contents;
    /// Do the easy things first.
    contents.metadata.szind = szind_t(bits >> RTREE_COMPACT_LG_VADDR);
    contents.metadata.slab = bool(bits & 1);
    contents.metadata.is_head = bool(bits & (1 << 1));

    uintptr_t state_bits = (bits & RTREE_LEAF_STATE_MASK) >> RTREE_LEAF_STATE_SHIFT;
    JE_ASSERT(state_bits <= extent_state_max);
    contents.metadata.state = ExtentState(state_bits);

    uintptr_t low_bit_mask = ~(uintptr_t(EDATA_ALIGNMENT) - 1);
    if constexpr (config::arch == Arch::AArch64)
    {
        /// aarch64 doesn't sign extend the highest virtual address bit to set the higher ones. Instead, the high bits
        /// get zeroed.
        uintptr_t high_bit_mask = (uintptr_t(1) << RTREE_COMPACT_LG_VADDR) - 1;
        /// Mask off metadata.
        uintptr_t mask = high_bit_mask & low_bit_mask;
        contents.edata = std::bit_cast<Extent *>(bits & mask);
    }
    else
    {
        /// Restore sign-extended high bits, mask metadata bits.
        contents.edata = std::bit_cast<Extent *>(
            uintptr_t(static_cast<intptr_t>(bits << RTREE_NHIB) >> RTREE_NHIB) & low_bit_mask);
    }
    JE_ASSERT(std::bit_cast<uintptr_t>(contents.edata) % uintptr_t(EDATA_ALIGNMENT) == 0);
    return contents;
}

/// jemalloc: rtree_contents_encode
JE_ALWAYS_INLINE constexpr RadixTreeEncoded rtreeContentsEncode(RadixTreeContents contents)
{
    RadixTreeEncoded encoded{};
    if constexpr (RTREE_LEAF_COMPACT)
    {
        encoded.bits = rtreeLeafElmBitsEncode(contents);
        encoded.additional = 0;
    }
    else
    {
        encoded.additional = unsigned(contents.metadata.slab) | (unsigned(contents.metadata.is_head) << 1)
            | (unsigned(contents.metadata.state) << RTREE_LEAF_STATE_SHIFT)
            | (unsigned(contents.metadata.szind) << (RTREE_LEAF_STATE_SHIFT + RTREE_LEAF_STATE_WIDTH));
        encoded.bits = std::bit_cast<uintptr_t>(contents.edata);
    }
    return encoded;
}

/// Atomic getters (`read` of both policies).
///
/// dependent: Reading a value on behalf of a pointer to a valid allocation is guaranteed to be a clean read even
///            without synchronization, because the rtree update became visible in memory before the pointer came
///            into existence.
/// !dependent: An arbitrary read, e.g. on behalf of `ivsalloc`, may not be dependent on a previous rtree write, which
///             means a stale read could result if synchronization were omitted here.

/// Compact: a single pointer-width word. On 64-bit with 48 significant address bits:
///
///   x: szind, e: edata, s: state, h: is_head, b: slab
///   00000000 xxxxxxxx eeeeeeee [...] eeeeeeee e00ssshb
template <>
struct RadixTreeLeafPolicy<true>
{
    using Elm = RadixTreeLeafElmCompact;

    /// jemalloc: rtree_leaf_elm_bits_read
    static JE_ALWAYS_INLINE uintptr_t bitsRead(ThreadState * /*tsdn*/, Elm * elm, bool dependent)
    {
        return std::atomic_ref<uintptr_t>(elm->le_bits).load(dependent ? std::memory_order_relaxed : std::memory_order_acquire);
    }

    /// jemalloc: rtree_leaf_elm_read
    static JE_ALWAYS_INLINE RadixTreeContents read(ThreadState * tsdn, Elm * elm, bool dependent)
    {
        uintptr_t bits = bitsRead(tsdn, elm, dependent);
        return rtreeLeafElmBitsDecode(bits);
    }

    /// jemalloc: rtree_leaf_elm_write_commit
    static JE_ALWAYS_INLINE void writeCommit(ThreadState * /*tsdn*/, Elm * elm, RadixTreeEncoded encoded)
    {
        std::atomic_ref<uintptr_t>(elm->le_bits).store(encoded.bits, std::memory_order_release);
    }

    /// jemalloc: rtree_leaf_elm_state_update
    static JE_ALWAYS_INLINE void stateUpdate(ThreadState * tsdn, Elm * elm1, Elm * elm2, ExtentState state)
    {
        JE_ASSERT(elm1 != nullptr);
        uintptr_t bits = bitsRead(tsdn, elm1, /* dependent */ true);
        bits &= ~RTREE_LEAF_STATE_MASK;
        bits |= uintptr_t(state) << RTREE_LEAF_STATE_SHIFT;
        std::atomic_ref<uintptr_t>(elm1->le_bits).store(bits, std::memory_order_release);
        if (elm2 != nullptr)
            std::atomic_ref<uintptr_t>(elm2->le_bits).store(bits, std::memory_order_release);
    }
};

/// Non-compact: the edata pointer and a metadata word with, from high to low bits: szind, state, is_head, slab.
template <>
struct RadixTreeLeafPolicy<false>
{
    using Elm = RadixTreeLeafElmWide;

    /// jemalloc: rtree_leaf_elm_read
    static JE_ALWAYS_INLINE RadixTreeContents read(ThreadState * /*tsdn*/, Elm * elm, bool dependent)
    {
        RadixTreeContents contents;
        unsigned metadata_bits
            = std::atomic_ref<unsigned>(elm->le_metadata).load(dependent ? std::memory_order_relaxed : std::memory_order_acquire);
        contents.metadata.slab = bool(metadata_bits & 1);
        contents.metadata.is_head = bool(metadata_bits & (1 << 1));

        uintptr_t state_bits = (metadata_bits & RTREE_LEAF_STATE_MASK) >> RTREE_LEAF_STATE_SHIFT;
        JE_ASSERT(state_bits <= extent_state_max);
        contents.metadata.state = ExtentState(state_bits);
        contents.metadata.szind = metadata_bits >> (RTREE_LEAF_STATE_SHIFT + RTREE_LEAF_STATE_WIDTH);

        contents.edata
            = std::atomic_ref<Extent *>(elm->le_edata).load(dependent ? std::memory_order_relaxed : std::memory_order_acquire);
        return contents;
    }

    /// jemalloc: rtree_leaf_elm_write_commit
    static JE_ALWAYS_INLINE void writeCommit(ThreadState * /*tsdn*/, Elm * elm, RadixTreeEncoded encoded)
    {
        std::atomic_ref<unsigned>(elm->le_metadata).store(encoded.additional, std::memory_order_release);
        /// Write edata last, since the element is atomically considered valid as soon as the edata field is non-null.
        std::atomic_ref<Extent *>(elm->le_edata).store(std::bit_cast<Extent *>(encoded.bits), std::memory_order_release);
    }

    /// jemalloc: rtree_leaf_elm_state_update
    static JE_ALWAYS_INLINE void stateUpdate(ThreadState * /*tsdn*/, Elm * elm1, Elm * elm2, ExtentState state)
    {
        JE_ASSERT(elm1 != nullptr);
        unsigned bits = std::atomic_ref<unsigned>(elm1->le_metadata).load(std::memory_order_relaxed);
        bits &= ~unsigned(RTREE_LEAF_STATE_MASK);
        bits |= unsigned(state) << RTREE_LEAF_STATE_SHIFT;
        std::atomic_ref<unsigned>(elm1->le_metadata).store(bits, std::memory_order_release);
        if (elm2 != nullptr)
            std::atomic_ref<unsigned>(elm2->le_metadata).store(bits, std::memory_order_release);
    }
};

/// The contents of an element written by `clear` / `clearRange`.
inline constexpr RadixTreeContents rtree_contents_cleared = {nullptr, {SC_NSIZES, ExtentState(0), false, false}};

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
    static JE_ALWAYS_INLINE RadixTreeContents leafElmRead(ThreadState * tsdn, RadixTreeLeafElm * elm, bool dependent)
    {
        return RadixTreeLeafPolicy<RTREE_LEAF_COMPACT>::read(tsdn, elm, dependent);
    }

    /// jemalloc: rtree_leaf_elm_write_commit
    static JE_ALWAYS_INLINE void leafElmWriteCommit(ThreadState * tsdn, RadixTreeLeafElm * elm, RadixTreeEncoded encoded)
    {
        RadixTreeLeafPolicy<RTREE_LEAF_COMPACT>::writeCommit(tsdn, elm, encoded);
    }

    /// jemalloc: rtree_leaf_elm_write
    static JE_ALWAYS_INLINE void leafElmWrite(ThreadState * tsdn, RadixTreeLeafElm * elm, RadixTreeContents contents)
    {
        JE_ASSERT(std::bit_cast<uintptr_t>(contents.edata) % EDATA_ALIGNMENT == 0);
        leafElmWriteCommit(tsdn, elm, rtreeContentsEncode(contents));
    }

    /// The state field can be updated independently (and more frequently).
    /// jemalloc: rtree_leaf_elm_state_update
    static JE_ALWAYS_INLINE void
    leafElmStateUpdate(ThreadState * tsdn, RadixTreeLeafElm * elm1, RadixTreeLeafElm * elm2, ExtentState state)
    {
        RadixTreeLeafPolicy<RTREE_LEAF_COMPACT>::stateUpdate(tsdn, elm1, elm2, state);
    }

    /// --- Lookup ----------------------------------------------------------------------------------------------------

    /// Tries to look up the key in the L1 cache, returning false if there's a hit, or true if there's a miss.
    /// The key is allowed to be 0; returns true in this case.
    /// jemalloc: rtree_leaf_elm_lookup_fast
    JE_ALWAYS_INLINE bool
    leafElmLookupFast(ThreadState * /*tsdn*/, RadixTreeContext * rtree_ctx, uintptr_t key, RadixTreeLeafElm ** elm)
    {
        size_t slot = rtreeCacheDirectMap(key);
        uintptr_t leafkey = rtreeLeafkey(key);
        JE_ASSERT(leafkey != RTREE_LEAFKEY_INVALID);

        if (JE_UNLIKELY(rtree_ctx->cache[slot].leafkey != leafkey))
            return true;

        RadixTreeLeafElm * leaf = rtree_ctx->cache[slot].leaf;
        JE_ASSERT(leaf != nullptr);
        uintptr_t subkey = rtreeSubkey(key, RTREE_HEIGHT - 1);
        *elm = &leaf[subkey];

        return false;
    }

    /// jemalloc: rtree_leaf_elm_lookup
    JE_ALWAYS_INLINE RadixTreeLeafElm *
    leafElmLookup(ThreadState * tsdn, RadixTreeContext * rtree_ctx, uintptr_t key, bool dependent, bool init_missing)
    {
        JE_ASSERT(key != 0);
        JE_ASSERT(!dependent || !init_missing);

        size_t slot = rtreeCacheDirectMap(key);
        uintptr_t leafkey = rtreeLeafkey(key);
        JE_ASSERT(leafkey != RTREE_LEAFKEY_INVALID);

        /// Fast path: L1 direct mapped cache.
        if (JE_LIKELY(rtree_ctx->cache[slot].leafkey == leafkey))
        {
            RadixTreeLeafElm * leaf = rtree_ctx->cache[slot].leaf;
            JE_ASSERT(leaf != nullptr);
            uintptr_t subkey = rtreeSubkey(key, RTREE_HEIGHT - 1);
            return &leaf[subkey];
        }

        /// Search the L2 LRU cache. On hit, swap the matching element into the slot in L1 cache, and move the
        /// position in L2 up by 1.
        auto check_l2 = [&](unsigned i) __attribute__((always_inline)) -> RadixTreeLeafElm *
        {
            if (JE_LIKELY(rtree_ctx->l2_cache[i].leafkey == leafkey))
            {
                RadixTreeLeafElm * leaf = rtree_ctx->l2_cache[i].leaf;
                JE_ASSERT(leaf != nullptr);
                if (i > 0)
                {
                    /// Bubble up by one.
                    rtree_ctx->l2_cache[i].leafkey = rtree_ctx->l2_cache[i - 1].leafkey;
                    rtree_ctx->l2_cache[i].leaf = rtree_ctx->l2_cache[i - 1].leaf;
                    rtree_ctx->l2_cache[i - 1].leafkey = rtree_ctx->cache[slot].leafkey;
                    rtree_ctx->l2_cache[i - 1].leaf = rtree_ctx->cache[slot].leaf;
                }
                else
                {
                    rtree_ctx->l2_cache[0].leafkey = rtree_ctx->cache[slot].leafkey;
                    rtree_ctx->l2_cache[0].leaf = rtree_ctx->cache[slot].leaf;
                }
                rtree_ctx->cache[slot].leafkey = leafkey;
                rtree_ctx->cache[slot].leaf = leaf;
                uintptr_t subkey = rtreeSubkey(key, RTREE_HEIGHT - 1);
                return &leaf[subkey];
            }
            return nullptr;
        };

        /// Check the first cache entry.
        if (RadixTreeLeafElm * elm = check_l2(0))
            return elm;
        /// Search the remaining cache elements.
        for (unsigned i = 1; i < RTREE_CTX_NCACHE_L2; ++i)
            if (RadixTreeLeafElm * elm = check_l2(i))
                return elm;

        return leafElmLookupHard(tsdn, rtree_ctx, key, dependent, init_missing);
    }

    /// The lookup after a miss in both caches: walk the tree (allocating missing nodes if `init_missing`), then
    /// (1) evict the last entry of L2, (2) move the colliding L1 slot down to L2, (3) fill L1.
    /// A lookup that ends with null does not touch the cache.
    /// jemalloc: rtree_leaf_elm_lookup_hard
    RadixTreeLeafElm *
    leafElmLookupHard(ThreadState * tsdn, RadixTreeContext * rtree_ctx, uintptr_t key, bool dependent, bool init_missing);

    /// --- Read / write ----------------------------------------------------------------------------------------------

    /// Returns true on lookup failure.
    /// jemalloc: rtree_read_independent
    JE_ALWAYS_INLINE bool
    readIndependent(ThreadState * tsdn, RadixTreeContext * rtree_ctx, uintptr_t key, RadixTreeContents * r_contents)
    {
        RadixTreeLeafElm * elm = leafElmLookup(tsdn, rtree_ctx, key, /* dependent */ false, /* init_missing */ false);
        if (elm == nullptr)
            return true;
        *r_contents = leafElmRead(tsdn, elm, /* dependent */ false);
        return false;
    }

    /// jemalloc: rtree_read
    JE_ALWAYS_INLINE RadixTreeContents read(ThreadState * tsdn, RadixTreeContext * rtree_ctx, uintptr_t key)
    {
        RadixTreeLeafElm * elm = leafElmLookup(tsdn, rtree_ctx, key, /* dependent */ true, /* init_missing */ false);
        JE_ASSERT(elm != nullptr);
        return leafElmRead(tsdn, elm, /* dependent */ true);
    }

    /// jemalloc: rtree_metadata_read
    JE_ALWAYS_INLINE RadixTreeMetadata metadataRead(ThreadState * tsdn, RadixTreeContext * rtree_ctx, uintptr_t key)
    {
        RadixTreeLeafElm * elm = leafElmLookup(tsdn, rtree_ctx, key, /* dependent */ true, /* init_missing */ false);
        JE_ASSERT(elm != nullptr);
        return leafElmRead(tsdn, elm, /* dependent */ true).metadata;
    }

    /// Returns true when the request cannot be fulfilled by the fast path (L1 cache only).
    /// jemalloc: rtree_metadata_try_read_fast
    JE_ALWAYS_INLINE bool
    metadataTryReadFast(ThreadState * tsdn, RadixTreeContext * rtree_ctx, uintptr_t key, RadixTreeMetadata * r_rtree_metadata)
    {
        RadixTreeLeafElm * elm;
        /// Check the bool return value instead of elm == null (which would result in an extra branch), because a
        /// cache hit never returns null (which is unknown to the compiler).
        if (leafElmLookupFast(tsdn, rtree_ctx, key, &elm))
            return true;
        JE_ASSERT(elm != nullptr);
        *r_rtree_metadata = leafElmRead(tsdn, elm, /* dependent */ true).metadata;
        return false;
    }

    /// jemalloc: rtree_write_range_impl
    JE_ALWAYS_INLINE void writeRangeImpl(
        ThreadState * tsdn, RadixTreeContext * rtree_ctx, uintptr_t base_addr, uintptr_t end, RadixTreeContents contents,
        [[maybe_unused]] bool clearing)
    {
        JE_ASSERT((base_addr & PAGE_MASK) == 0 && (end & PAGE_MASK) == 0);
        /// Only used for `emap_(de)register_interior`, which implies the boundaries have been registered already.
        /// Therefore all the lookups are dependent without init_missing, assuming the range spans across at most 2
        /// rtree leaf nodes (each covers 1 GiB of vaddr).
        RadixTreeEncoded encoded = rtreeContentsEncode(contents);

        RadixTreeLeafElm * elm = nullptr; /// Dead store.
        for (uintptr_t addr = base_addr; addr <= end; addr += PAGE)
        {
            if (addr == base_addr || (addr & ((uintptr_t(1) << rtreeLeafMaskbits()) - 1)) == 0)
            {
                elm = leafElmLookup(tsdn, rtree_ctx, addr, /* dependent */ true, /* init_missing */ false);
                JE_ASSERT(elm != nullptr);
            }
            JE_ASSERT(elm == leafElmLookup(tsdn, rtree_ctx, addr, /* dependent */ true, /* init_missing */ false));
            JE_ASSERT(!clearing || leafElmRead(tsdn, elm, /* dependent */ true).edata != nullptr);
            leafElmWriteCommit(tsdn, elm, encoded);
            ++elm;
        }
    }

    /// jemalloc: rtree_write_range
    JE_ALWAYS_INLINE void
    writeRange(ThreadState * tsdn, RadixTreeContext * rtree_ctx, uintptr_t base_addr, uintptr_t end, RadixTreeContents contents)
    {
        writeRangeImpl(tsdn, rtree_ctx, base_addr, end, contents, /* clearing */ false);
    }

    /// Returns true on error (failure to allocate a node).
    /// jemalloc: rtree_write
    JE_ALWAYS_INLINE bool write(ThreadState * tsdn, RadixTreeContext * rtree_ctx, uintptr_t key, RadixTreeContents contents)
    {
        RadixTreeLeafElm * elm = leafElmLookup(tsdn, rtree_ctx, key, /* dependent */ false, /* init_missing */ true);
        if (elm == nullptr)
            return true;

        leafElmWrite(tsdn, elm, contents);
        return false;
    }

    /// jemalloc: rtree_clear
    JE_ALWAYS_INLINE void clear(ThreadState * tsdn, RadixTreeContext * rtree_ctx, uintptr_t key)
    {
        RadixTreeLeafElm * elm = leafElmLookup(tsdn, rtree_ctx, key, /* dependent */ true, /* init_missing */ false);
        JE_ASSERT(elm != nullptr);
        JE_ASSERT(leafElmRead(tsdn, elm, /* dependent */ true).edata != nullptr);
        leafElmWrite(tsdn, elm, rtree_contents_cleared);
    }

    /// jemalloc: rtree_clear_range
    JE_ALWAYS_INLINE void clearRange(ThreadState * tsdn, RadixTreeContext * rtree_ctx, uintptr_t base_addr, uintptr_t end)
    {
        writeRangeImpl(tsdn, rtree_ctx, base_addr, end, rtree_contents_cleared, /* clearing */ true);
    }

    /// The number of elements of the root (`rtree_levels[0].bits`).
    static constexpr size_t root_size = size_t(1) << (RTREE_NSB / RTREE_HEIGHT);

    Base * base = nullptr;
    Mutex init_lock;
    /// The root node (an interior node, since the height is at least 2).
    RadixTreeNodeElm root[root_size] = {};

private:
    /// jemalloc: rtree_node_alloc
    RadixTreeNodeElm * nodeAlloc(ThreadState * tsdn, size_t nelms);
    /// jemalloc: rtree_leaf_alloc
    RadixTreeLeafElm * leafAlloc(ThreadState * tsdn, size_t nelms);
    /// jemalloc: rtree_node_init
    RadixTreeNodeElm * nodeInit(ThreadState * tsdn, unsigned level, void ** elmp);
    /// jemalloc: rtree_leaf_init
    RadixTreeLeafElm * leafInit(ThreadState * tsdn, void ** elmp);
    /// jemalloc: rtree_child_node_read
    RadixTreeNodeElm * childNodeRead(ThreadState * tsdn, RadixTreeNodeElm * elm, unsigned level, bool dependent);
    /// jemalloc: rtree_child_leaf_read
    RadixTreeLeafElm * childLeafRead(ThreadState * tsdn, RadixTreeNodeElm * elm, unsigned level, bool dependent);
};

static_assert(std::is_standard_layout_v<RadixTree>);

}
