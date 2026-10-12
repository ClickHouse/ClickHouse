#pragma once

/// Extent metadata (jemalloc: `edata.h`, `edata.c`, `slab_data.h`).
///
/// `Extent` is jemalloc's `edata_t`: it describes a span of pages (or, for the base allocator, a span of bytes).
/// The layout is identical to the C struct (`sizeof` is observable through `stats.metadata`), including the unused
/// `unused_page_slab` slot of the dropped HPA. Extents are carved out of zero-filled memory by `Base`, so the type is trivial
/// (atomic fields are accessed through `std::atomic_ref` with the same memory orders as jemalloc's `atomic_p_t`).

#include <allocator/Bitmap.h>
#include <allocator/Common.h>
#include <allocator/IntrusiveList.h>
#include <allocator/Nanoseconds.h>
#include <allocator/PairingHeap.h>
#include <allocator/SizeClasses.h>

#include <atomic>
#include <type_traits>

namespace jemalloc
{

class ProfilingThreadContext; /// jemalloc: prof_tctx_t
class ProfilingRecent; /// jemalloc: prof_recent_t

/// Every `Extent` handed out by `Base` is aligned to this, which frees the low pointer bits for the compact rtree
/// leaf encoding. jemalloc: EDATA_ALIGNMENT
inline constexpr size_t EXTENT_ALIGNMENT = 128;

/// How many nodes are visited when enumerating an extent heap in search of a suitable extent; also the BFS queue size.
/// jemalloc: ESET_ENUMERATE_MAX_NUM
inline constexpr unsigned EXTENT_SET_ENUMERATE_MAX_NUM = 32;

/// jemalloc: extent_state_t
enum ExtentState : unsigned
{
    extent_state_active = 0,
    extent_state_dirty = 1,
    extent_state_muzzy = 2,
    extent_state_retained = 3,
    extent_state_transition = 4, /// States below are intermediate.
    extent_state_merging = 5,
    extent_state_max = 5, /// Sanity checking only.
};

/// jemalloc: extent_head_state_t
enum ExtentHeadState : unsigned
{
    EXTENT_NOT_HEAD,
    EXTENT_IS_HEAD, /// See comments in `ehooks_default_merge_impl`.
};

/// Which page allocator implementation owns the extent (HPA is dropped, so it is always PAC).
/// jemalloc: extent_pai_t
enum ExtentAllocatorKind : unsigned
{
    EXTENT_ALLOCATOR_PAGE_ALLOCATOR = 0,
    EXTENT_ALLOCATOR_HUGE_PAGE_ALLOCATOR = 1,
};

/// jemalloc: edata_state_in_transition
constexpr bool extentStateInTransition(ExtentState state)
{
    return state >= extent_state_transition;
}

class Extent;

/// Small region slab metadata: the per-region allocated/deallocated bitmap. jemalloc: slab_data_t
struct SlabData
{
    bitmap_t bitmap[BITMAP_GROUPS_MAX];
};

/// Profiling data, used for large (sampled) objects. jemalloc: e_prof_info_t
struct ExtentProfilingInfo
{
    /// Time when this was allocated.
    Nanoseconds allocation_time;
    /// Allocation request size.
    size_t allocation_size;
    /// Atomic (acquire/release). jemalloc: atomic_p_t e_prof_tctx
    ProfilingThreadContext * thread_context;
    /// Atomic (relaxed); null means the recent allocation record no longer exists. Protected by
    /// `profiling_recent_alloc_mutex`. jemalloc: atomic_p_t e_prof_recent_alloc
    ProfilingRecent * recent_allocation;
    /// Linkage into the owning gctx's list of live sampled allocations (`global_context->fragmentation_objects`). Both fields are protected
    /// by the owning gctx's lock; the gctx is reachable via `e_profiling_thread_context` while `e_profiling_fragmentation_tracked` is true.
    RingLink<Extent> fragmentation_link;
    bool fragmentation_tracked;
};

/// The information about an extent that lives in the emap (rtree leaf). jemalloc: edata_map_info_t
struct ExtentMapInfo
{
    bool slab;
    SizeClassIdx size_class_idx;
};

/// jemalloc: edata_cmp_summary_t
struct ExtentComparisonSummary
{
    uint64_t serial_number;
    uintptr_t addr;
};

/// A field of `Extent::packed_bits`. jemalloc: EDATA_BITS_*_WIDTH / _SHIFT / _MASK
struct ExtentBitField
{
    unsigned width;
    unsigned shift;

    constexpr uint64_t mask() const { return ((uint64_t(1) << width) - 1) << shift; }
    constexpr unsigned end() const { return width + shift; }
};

namespace extent_bits
{

/// 00000000 ... 0000ssss ssffffff ffffiiii iiiitttg zpcbaaaa aaaaaaaa (for LOG2_PAGE = 12)
inline constexpr ExtentBitField arena{MALLOCX_ARENA_BITS, 0};
inline constexpr ExtentBitField slab{1, arena.end()};
inline constexpr ExtentBitField committed{1, slab.end()};
inline constexpr ExtentBitField allocator_kind{1, committed.end()};
inline constexpr ExtentBitField zeroed{1, allocator_kind.end()};
inline constexpr ExtentBitField guarded{1, zeroed.end()};
inline constexpr ExtentBitField state{3, guarded.end()};
inline constexpr ExtentBitField size_class_idx{log2CeilConst(SIZE_CLASS_NUM_SIZES), state.end()};
inline constexpr ExtentBitField num_free{SIZE_CLASS_LOG2_SLAB_MAX_REGIONS + 1, size_class_idx.end()};
inline constexpr ExtentBitField bin_shard{6, num_free.end()};
inline constexpr ExtentBitField is_head{1, bin_shard.end()};

static_assert(is_head.end() <= 64);

}

/// jemalloc: EDATA_SIZE_MASK, EDATA_ESN_MASK
inline constexpr size_t EXTENT_SIZE_MASK = ~(PAGE - 1);
inline constexpr size_t EXTENT_STRUCT_SERIAL_NUMBER_MASK = PAGE - 1;

/// Extent (span of pages). jemalloc: edata_t
///
/// The fields are public and named as in jemalloc (they are accessed via the accessor methods everywhere except
/// in layout-sensitive code such as the containers' pointer-to-member links).
class Extent
{
public:
    /// a: arena_ind, b: slab, c: committed, p: pai, z: zeroed, g: guarded, t: state, i: szind, f: nfree,
    /// s: bin_shard, h: is_head. See `extent_bits`.
    uint64_t packed_bits;

    /// Pointer to the extent that this structure is responsible for.
    void * address;

    union
    {
        /// Extent size and serial number associated with the extent structure (different than the serial number
        /// for the extent at `address`). ssssssss [...] ssssssss ssssnnnn nnnnnnnn
        size_t size_and_serial_number;
        /// Base extent size, which may not be a multiple of PAGE.
        size_t base_size;
    };

    /// HPA pageslab (`hpdata_t *`). HPA is dropped; the slot is kept for an identical size.
    void * unused_page_slab;

    /// Serial number. These are not necessarily unique; splitting an extent results in two extents with the same
    /// serial number.
    uint64_t serial_number;

    union
    {
        /// List linkage used when the extent is active; either in the arena's large allocations or bin's `slabs_full`.
        RingLink<Extent> list_link_active;
        /// Pairing heap linkage. Used whenever the extent is inactive (in the page allocators), or when it is active
        /// and in `slabs_non_full`, or when the `Extent` is unassociated with an extent and sitting in an `ExtentPool`.
        PairingHeapLink<Extent> heap_link;
        PairingHeapLink<Extent> available_link;
    };

    union
    {
        /// List linkage used when the extent is inactive: stashed dirty extents, ecache LRU.
        RingLink<Extent> list_link_inactive;
        /// Small region slab metadata.
        SlabData slab_data;
        /// Profiling data, used for large objects.
        ExtentProfilingInfo profiling_info;
    };

    /// --- Getters ---------------------------------------------------------------------------------------------------

    /// jemalloc: edata_arena_ind_get
    ALLOCATOR_ALWAYS_INLINE unsigned arenaIdx() const
    {
        unsigned arena_idx = unsigned(getBits(extent_bits::arena));
        ALLOCATOR_ASSERT(arena_idx < MALLOCX_ARENA_LIMIT);
        return arena_idx;
    }

    /// jemalloc: edata_szind_get_maybe_invalid
    ALLOCATOR_ALWAYS_INLINE SizeClassIdx sizeClassIdxMaybeInvalid() const
    {
        SizeClassIdx size_class_idx = SizeClassIdx(getBits(extent_bits::size_class_idx));
        ALLOCATOR_ASSERT(size_class_idx <= SIZE_CLASS_NUM_SIZES);
        return size_class_idx;
    }

    /// jemalloc: edata_szind_get
    ALLOCATOR_ALWAYS_INLINE SizeClassIdx sizeClassIdx() const
    {
        SizeClassIdx size_class_idx = sizeClassIdxMaybeInvalid();
        ALLOCATOR_ASSERT(size_class_idx < SIZE_CLASS_NUM_SIZES); /// Never call when "invalid".
        return size_class_idx;
    }

    /// jemalloc: edata_usize_get
    ALLOCATOR_ALWAYS_INLINE size_t usableSize() const
    {
        /// When large size classes are disabled: if the usize from the index is not smaller than SIZE_CLASS_LARGE_MIN_CLASS,
        /// the usize from the size is accurate; otherwise the usize from the index is accurate. When they are not
        /// disabled, the two are the same for usize >= SIZE_CLASS_LARGE_MIN_CLASS. Sampled small allocations are promoted:
        /// their extent size is recorded in the size, while their szind reflects the true usize.
        SizeClassIdx idx = sizeClassIdx();
        if (!size_classes::largeSizeClassesDisabled() || idx < SIZE_CLASS_NUM_BINS)
        {
            size_t usable_size_from_idx = size_classes::indexToSize(idx);
            if constexpr (config::debug)
            {
                if (!size_classes::largeSizeClassesDisabled() && usable_size_from_idx >= SIZE_CLASS_LARGE_MIN_CLASS)
                {
                    size_t size = (size_and_serial_number & EXTENT_SIZE_MASK);
                    ALLOCATOR_ASSERT(size > large_pad);
                    ALLOCATOR_ASSERT(usable_size_from_idx == size - large_pad);
                }
            }
            return usable_size_from_idx;
        }

        size_t size = (size_and_serial_number & EXTENT_SIZE_MASK);
        ALLOCATOR_ASSERT(size > large_pad);
        size_t usable_size_from_size = size - large_pad;
        /// No matter whether large size classes are disabled or not, the usize from the size is not accurate when
        /// smaller than SIZE_CLASS_LARGE_MIN_CLASS.
        ALLOCATOR_ASSERT(usable_size_from_size >= SIZE_CLASS_LARGE_MIN_CLASS);
        return usable_size_from_size;
    }

    /// jemalloc: edata_binshard_get
    ALLOCATOR_ALWAYS_INLINE unsigned binShard() const
    {
        unsigned bin_shard = unsigned(getBits(extent_bits::bin_shard));
        ALLOCATOR_ASSERT(bin_shard < bin_infos[sizeClassIdx()].num_shards);
        return bin_shard;
    }

    /// jemalloc: edata_sn_get
    ALLOCATOR_ALWAYS_INLINE uint64_t serialNumber() const { return serial_number; }

    /// jemalloc: edata_state_get
    ALLOCATOR_ALWAYS_INLINE ExtentState state() const { return ExtentState(getBits(extent_bits::state)); }

    /// jemalloc: edata_guarded_get
    ALLOCATOR_ALWAYS_INLINE bool guarded() const { return bool(getBits(extent_bits::guarded)); }

    /// jemalloc: edata_zeroed_get
    ALLOCATOR_ALWAYS_INLINE bool zeroed() const { return bool(getBits(extent_bits::zeroed)); }

    /// jemalloc: edata_committed_get
    ALLOCATOR_ALWAYS_INLINE bool committed() const { return bool(getBits(extent_bits::committed)); }

    /// jemalloc: edata_pai_get
    ALLOCATOR_ALWAYS_INLINE ExtentAllocatorKind allocatorKind() const { return ExtentAllocatorKind(getBits(extent_bits::allocator_kind)); }

    /// jemalloc: edata_slab_get
    ALLOCATOR_ALWAYS_INLINE bool slab() const { return bool(getBits(extent_bits::slab)); }

    /// jemalloc: edata_nfree_get
    ALLOCATOR_ALWAYS_INLINE unsigned numFree() const
    {
        ALLOCATOR_ASSERT(slab());
        return unsigned(getBits(extent_bits::num_free));
    }

    /// jemalloc: edata_is_head_get
    ALLOCATOR_ALWAYS_INLINE bool isHead() const { return bool(getBits(extent_bits::is_head)); }

    /// jemalloc: edata_base_get
    ALLOCATOR_ALWAYS_INLINE void * base() const
    {
        ALLOCATOR_ASSERT(address == pageAddrToBase(address) || !slab());
        return pageAddrToBase(address);
    }

    /// jemalloc: edata_addr_get
    ALLOCATOR_ALWAYS_INLINE void * addr() const
    {
        ALLOCATOR_ASSERT(address == pageAddrToBase(address) || !slab());
        return address;
    }

    /// jemalloc: edata_size_get
    ALLOCATOR_ALWAYS_INLINE size_t size() const { return size_and_serial_number & EXTENT_SIZE_MASK; }

    /// jemalloc: edata_esn_get
    ALLOCATOR_ALWAYS_INLINE size_t structSerialNumber() const { return size_and_serial_number & EXTENT_STRUCT_SERIAL_NUMBER_MASK; }

    /// jemalloc: edata_bsize_get
    ALLOCATOR_ALWAYS_INLINE size_t baseSize() const { return base_size; }

    /// jemalloc: edata_ps_get
    ALLOCATOR_ALWAYS_INLINE void * pageSlab() const
    {
        ALLOCATOR_ASSERT(allocatorKind() == EXTENT_ALLOCATOR_HUGE_PAGE_ALLOCATOR);
        return unused_page_slab;
    }

    /// jemalloc: edata_before_get
    ALLOCATOR_ALWAYS_INLINE void * before() const { return static_cast<std::byte *>(base()) - PAGE; }

    /// jemalloc: edata_last_get
    ALLOCATOR_ALWAYS_INLINE void * last() const { return static_cast<std::byte *>(base()) + size() - PAGE; }

    /// jemalloc: edata_past_get
    ALLOCATOR_ALWAYS_INLINE void * past() const { return static_cast<std::byte *>(base()) + size(); }

    /// jemalloc: edata_slab_data_get
    ALLOCATOR_ALWAYS_INLINE SlabData * slabData()
    {
        ALLOCATOR_ASSERT(slab());
        return &slab_data;
    }

    /// jemalloc: edata_slab_data_get_const
    ALLOCATOR_ALWAYS_INLINE const SlabData * slabData() const
    {
        ALLOCATOR_ASSERT(slab());
        return &slab_data;
    }

    /// jemalloc: edata_prof_tctx_get
    ALLOCATOR_ALWAYS_INLINE ProfilingThreadContext * profilingThreadContext() const
    {
        return std::atomic_ref<ProfilingThreadContext *>(const_cast<ProfilingThreadContext *&>(profiling_info.thread_context))
            .load(std::memory_order_acquire);
    }

    /// jemalloc: edata_prof_alloc_time_get
    ALLOCATOR_ALWAYS_INLINE const Nanoseconds * profilingAllocTime() const { return &profiling_info.allocation_time; }

    /// jemalloc: edata_prof_alloc_size_get
    ALLOCATOR_ALWAYS_INLINE size_t profilingAllocSize() const { return profiling_info.allocation_size; }

    /// jemalloc: edata_prof_recent_alloc_get_dont_call_directly
    ALLOCATOR_ALWAYS_INLINE ProfilingRecent * profilingRecentAllocGetDontCallDirectly() const
    {
        return std::atomic_ref<ProfilingRecent *>(const_cast<ProfilingRecent *&>(profiling_info.recent_allocation))
            .load(std::memory_order_relaxed);
    }

    /// jemalloc: edata_prof_frag_tracked_get
    ALLOCATOR_ALWAYS_INLINE bool profilingFragmentationTracked() const { return profiling_info.fragmentation_tracked; }

    /// --- Setters ---------------------------------------------------------------------------------------------------

    /// jemalloc: edata_arena_ind_set
    ALLOCATOR_ALWAYS_INLINE void setArenaIdx(unsigned arena_idx) { setBits(extent_bits::arena, arena_idx); }

    /// The assertion assumes szind is set already.
    /// jemalloc: edata_binshard_set
    ALLOCATOR_ALWAYS_INLINE void setBinShard(unsigned bin_shard)
    {
        ALLOCATOR_ASSERT(bin_shard < bin_infos[sizeClassIdx()].num_shards);
        setBits(extent_bits::bin_shard, bin_shard);
    }

    /// jemalloc: edata_addr_set
    ALLOCATOR_ALWAYS_INLINE void setAddr(void * addr) { address = addr; }

    /// jemalloc: edata_size_set
    ALLOCATOR_ALWAYS_INLINE void setSize(size_t size)
    {
        ALLOCATOR_ASSERT((size & ~EXTENT_SIZE_MASK) == 0);
        size_and_serial_number = size | (size_and_serial_number & ~EXTENT_SIZE_MASK);
    }

    /// jemalloc: edata_esn_set
    ALLOCATOR_ALWAYS_INLINE void setStructSerialNumber(size_t struct_serial_number)
    {
        size_and_serial_number
            = (size_and_serial_number & ~EXTENT_STRUCT_SERIAL_NUMBER_MASK) | (struct_serial_number & EXTENT_STRUCT_SERIAL_NUMBER_MASK);
    }

    /// jemalloc: edata_bsize_set
    ALLOCATOR_ALWAYS_INLINE void setBaseSize(size_t value) { base_size = value; }

    /// jemalloc: edata_ps_set
    ALLOCATOR_ALWAYS_INLINE void setPageSlab(void * page_slab)
    {
        ALLOCATOR_ASSERT(allocatorKind() == EXTENT_ALLOCATOR_HUGE_PAGE_ALLOCATOR);
        unused_page_slab = page_slab;
    }

    /// SIZE_CLASS_NUM_SIZES means "invalid".
    /// jemalloc: edata_szind_set
    ALLOCATOR_ALWAYS_INLINE void setSizeClassIdx(SizeClassIdx size_class_idx)
    {
        ALLOCATOR_ASSERT(size_class_idx <= SIZE_CLASS_NUM_SIZES);
        setBits(extent_bits::size_class_idx, size_class_idx);
    }

    /// jemalloc: edata_nfree_set
    ALLOCATOR_ALWAYS_INLINE void setNumFree(unsigned num_free)
    {
        ALLOCATOR_ASSERT(slab());
        setBits(extent_bits::num_free, num_free);
    }

    /// The assertion assumes szind is set already.
    /// jemalloc: edata_nfree_binshard_set
    ALLOCATOR_ALWAYS_INLINE void setNumFreeBinShard(unsigned num_free, unsigned bin_shard)
    {
        ALLOCATOR_ASSERT(bin_shard < bin_infos[sizeClassIdx()].num_shards);
        packed_bits = (packed_bits & (~extent_bits::num_free.mask() & ~extent_bits::bin_shard.mask()))
            | (uint64_t(bin_shard) << extent_bits::bin_shard.shift) | (uint64_t(num_free) << extent_bits::num_free.shift);
    }

    /// jemalloc: edata_nfree_inc
    ALLOCATOR_ALWAYS_INLINE void numFreeIncrement()
    {
        ALLOCATOR_ASSERT(slab());
        packed_bits += uint64_t(1) << extent_bits::num_free.shift;
    }

    /// jemalloc: edata_nfree_dec
    ALLOCATOR_ALWAYS_INLINE void numFreeDecrement()
    {
        ALLOCATOR_ASSERT(slab());
        packed_bits -= uint64_t(1) << extent_bits::num_free.shift;
    }

    /// jemalloc: edata_nfree_sub
    ALLOCATOR_ALWAYS_INLINE void numFreeSub(uint64_t n)
    {
        ALLOCATOR_ASSERT(slab());
        packed_bits -= n << extent_bits::num_free.shift;
    }

    /// jemalloc: edata_sn_set
    ALLOCATOR_ALWAYS_INLINE void setSerialNumber(uint64_t value) { serial_number = value; }

    /// jemalloc: edata_state_set
    ALLOCATOR_ALWAYS_INLINE void setState(ExtentState state) { setBits(extent_bits::state, state); }

    /// jemalloc: edata_guarded_set
    ALLOCATOR_ALWAYS_INLINE void setGuarded(bool guarded) { setBits(extent_bits::guarded, guarded); }

    /// jemalloc: edata_zeroed_set
    ALLOCATOR_ALWAYS_INLINE void setZeroed(bool zeroed) { setBits(extent_bits::zeroed, zeroed); }

    /// jemalloc: edata_committed_set
    ALLOCATOR_ALWAYS_INLINE void setCommitted(bool committed) { setBits(extent_bits::committed, committed); }

    /// jemalloc: edata_pai_set
    ALLOCATOR_ALWAYS_INLINE void setAllocatorKind(ExtentAllocatorKind allocator_kind)
    {
        setBits(extent_bits::allocator_kind, allocator_kind);
    }

    /// jemalloc: edata_slab_set
    ALLOCATOR_ALWAYS_INLINE void setSlab(bool slab) { setBits(extent_bits::slab, slab); }

    /// jemalloc: edata_is_head_set
    ALLOCATOR_ALWAYS_INLINE void setIsHead(bool is_head) { setBits(extent_bits::is_head, is_head); }

    /// jemalloc: edata_prof_tctx_set
    ALLOCATOR_ALWAYS_INLINE void setProfilingThreadContext(ProfilingThreadContext * thread_context)
    {
        std::atomic_ref<ProfilingThreadContext *>(profiling_info.thread_context)
            .store(thread_context, std::memory_order_release);
    }

    /// jemalloc: edata_prof_alloc_time_set
    ALLOCATOR_ALWAYS_INLINE void setProfilingAllocTime(const Nanoseconds * t) { profiling_info.allocation_time.copy(*t); }

    /// jemalloc: edata_prof_alloc_size_set
    ALLOCATOR_ALWAYS_INLINE void setProfilingAllocSize(size_t size) { profiling_info.allocation_size = size; }

    /// jemalloc: edata_prof_recent_alloc_set_dont_call_directly
    ALLOCATOR_ALWAYS_INLINE void setProfilingRecentAllocDontCallDirectly(ProfilingRecent * recent_alloc)
    {
        std::atomic_ref<ProfilingRecent *>(profiling_info.recent_allocation).store(recent_alloc, std::memory_order_relaxed);
    }

    /// jemalloc: edata_prof_frag_tracked_set
    ALLOCATOR_ALWAYS_INLINE void setProfilingFragmentationTracked(bool tracked)
    {
        profiling_info.fragmentation_tracked = tracked;
    }

    /// --- Initialization --------------------------------------------------------------------------------------------

    /// Because this is implemented as a sequence of bitfield modifications, even though each individual bit is
    /// properly initialized, it technically reads uninitialized data. Most callers get their extents from zeroing
    /// sources; callers who make stack extents need to zero them manually.
    /// jemalloc: edata_init
    ALLOCATOR_ALWAYS_INLINE void init(
        unsigned arena_idx,
        void * addr,
        size_t size,
        bool slab,
        SizeClassIdx size_class_idx,
        uint64_t serial_number,
        ExtentState state,
        bool zeroed,
        bool committed,
        ExtentAllocatorKind allocator_kind,
        ExtentHeadState is_head)
    {
        ALLOCATOR_ASSERT(addr == pageAddrToBase(addr) || !slab);

        setArenaIdx(arena_idx);
        setAddr(addr);
        setSize(size);
        setSlab(slab);
        setSizeClassIdx(size_class_idx);
        setSerialNumber(serial_number);
        setState(state);
        setGuarded(false);
        setZeroed(zeroed);
        setCommitted(committed);
        setAllocatorKind(allocator_kind);
        setIsHead(is_head == EXTENT_IS_HEAD);
        if constexpr (config::profiling)
            setProfilingThreadContext(nullptr);
    }

    /// jemalloc: edata_binit
    ALLOCATOR_ALWAYS_INLINE void initBase(void * addr, size_t base_size, uint64_t serial_number, bool reused)
    {
        setArenaIdx((1U << MALLOCX_ARENA_BITS) - 1);
        setAddr(addr);
        setBaseSize(base_size);
        setSlab(false);
        setSizeClassIdx(SIZE_CLASS_NUM_SIZES);
        setSerialNumber(serial_number);
        setState(extent_state_active);
        /// See comments in `base_edata_is_reused`.
        setGuarded(reused);
        setZeroed(true);
        setCommitted(true);
        /// This isn't strictly true, but base allocated extents never get deallocated and can't be looked up in the
        /// emap, but no sense in wasting a state bit to encode this fact.
        setAllocatorKind(EXTENT_ALLOCATOR_PAGE_ALLOCATOR);
    }

    /// --- Comparators -----------------------------------------------------------------------------------------------

    /// jemalloc: edata_esn_comp
    static ALLOCATOR_ALWAYS_INLINE int compareStructSerialNumber(const Extent * a, const Extent * b)
    {
        size_t a_struct_serial_number = a->structSerialNumber();
        size_t b_struct_serial_number = b->structSerialNumber();
        return (a_struct_serial_number > b_struct_serial_number) - (a_struct_serial_number < b_struct_serial_number);
    }

    /// Compares the addresses of the `Extent` structures themselves.
    /// jemalloc: edata_ead_comp
    static ALLOCATOR_ALWAYS_INLINE int compareExtentAddress(const Extent * a, const Extent * b)
    {
        uintptr_t a_end_address = reinterpret_cast<uintptr_t>(a);
        uintptr_t b_end_address = reinterpret_cast<uintptr_t>(b);
        return (a_end_address > b_end_address) - (a_end_address < b_end_address);
    }

    /// jemalloc: edata_cmp_summary_get
    ALLOCATOR_ALWAYS_INLINE ExtentComparisonSummary comparisonSummary() const
    {
        ExtentComparisonSummary result;
        result.serial_number = serialNumber();
        result.addr = reinterpret_cast<uintptr_t>(addr());
        return result;
    }

    /// Lexicographic (sn, addr). Branchless: the sn comparison is multiplied by 2 so that, when non-zero, it dominates
    /// the addr comparison (the branches would be badly predicted; measurably faster).
    /// jemalloc: edata_cmp_summary_comp (the variant without JEMALLOC_HAVE_INT128)
    static ALLOCATOR_ALWAYS_INLINE int compareSummary(ExtentComparisonSummary a, ExtentComparisonSummary b)
    {
        return (2 * ((a.serial_number > b.serial_number) - (a.serial_number < b.serial_number))) + ((a.addr > b.addr) - (a.addr < b.addr));
    }

    /// jemalloc: edata_snad_comp
    static ALLOCATOR_ALWAYS_INLINE int compareSerialNumberAndAddress(const Extent * a, const Extent * b)
    {
        return compareSummary(a->comparisonSummary(), b->comparisonSummary());
    }

    /// Lexicographic (esn, address of the structure), branchless.
    /// jemalloc: edata_esnead_comp
    static ALLOCATOR_ALWAYS_INLINE int compareStructSerialNumberAndAddress(const Extent * a, const Extent * b)
    {
        return (2 * compareStructSerialNumber(a, b)) + compareExtentAddress(a, b);
    }

private:
    ALLOCATOR_ALWAYS_INLINE uint64_t getBits(ExtentBitField field) const { return (packed_bits & field.mask()) >> field.shift; }

    ALLOCATOR_ALWAYS_INLINE void setBits(ExtentBitField field, uint64_t value)
    {
        packed_bits = (packed_bits & ~field.mask()) | (value << field.shift);
    }
};

static_assert(std::is_trivial_v<Extent>);
static_assert(std::is_standard_layout_v<Extent>);
static_assert(sizeof(SlabData) >= sizeof(ExtentProfilingInfo));
static_assert(sizeof(ExtentProfilingInfo) == 56);
/// Measured from the C build (`stats.metadata` depends on it).
static_assert(sizeof(Extent) == (LOG2_PAGE == 12 ? 128 : (LOG2_PAGE == 14 ? 328 : 1112)));
static_assert(EXTENT_ALIGNMENT >= alignof(Extent));

struct ExtentSerialNumberAndAddressCompare
{
    ALLOCATOR_ALWAYS_INLINE int operator()(const Extent * a, const Extent * b) const { return Extent::compareSerialNumberAndAddress(a, b); }
};

struct ExtentStructSerialNumberAndAddressCompare
{
    ALLOCATOR_ALWAYS_INLINE int operator()(const Extent * a, const Extent * b) const
    {
        return Extent::compareStructSerialNumberAndAddress(a, b);
    }
};

/// The heap of extents ordered by (sn, addr): eset bins, base avail heaps, bin `slabs_non_full`.
/// jemalloc: edata_heap_t (ph_gen(, edata_heap, edata_t, heap_link, edata_snad_comp))
using ExtentHeap = PairingHeap<Extent, &Extent::heap_link, ExtentSerialNumberAndAddressCompare>;

/// The heap of unused `Extent` structures ordered by (esn, structure address): `ExtentPool`, base `extent_available`.
/// jemalloc: edata_avail_t (ph_gen(, edata_avail, edata_t, avail_link, edata_esnead_comp))
using ExtentAvailableHeap = PairingHeap<Extent, &Extent::available_link, ExtentStructSerialNumberAndAddressCompare>;

/// jemalloc: edata_heap_enumerate_helper_t, edata_avail_enumerate_helper_t
using ExtentHeapEnumerateHelper = ExtentHeap::EnumerateHelper<EXTENT_SET_ENUMERATE_MAX_NUM>;
using ExtentAvailableEnumerateHelper = ExtentAvailableHeap::EnumerateHelper<EXTENT_SET_ENUMERATE_MAX_NUM>;

/// jemalloc: edata_list_active_t (TYPED_LIST(edata_list_active, edata_t, ql_link_active))
using ExtentListActive = TypedList<Extent, &Extent::list_link_active>;

/// jemalloc: edata_list_inactive_t (TYPED_LIST(edata_list_inactive, edata_t, ql_link_inactive))
using ExtentListInactive = TypedList<Extent, &Extent::list_link_inactive>;

/// The list of live sampled allocations of a gctx, linked through `e_prof_info.e_prof_frag_link`.
/// A nested member cannot be named by a pointer-to-member, so this is a direct port of the `ql`/`TYPED_LIST`
/// operations with exactly the same semantics as `TypedList`.
/// jemalloc: edata_list_frag_t (TYPED_LIST(edata_list_frag, edata_t, e_prof_info.e_prof_frag_link))
class ExtentListFragmentation
{
public:
    constexpr ExtentListFragmentation() = default;

    ExtentListFragmentation(const ExtentListFragmentation &) = delete;
    ExtentListFragmentation & operator=(const ExtentListFragmentation &) = delete;

    /// jemalloc: edata_list_frag_init
    ALLOCATOR_ALWAYS_INLINE void init() { head = nullptr; }

    /// jemalloc: edata_list_frag_first
    ALLOCATOR_ALWAYS_INLINE Extent * first() const { return head; }

    /// jemalloc: edata_list_frag_last
    ALLOCATOR_ALWAYS_INLINE Extent * last() const { return empty() ? nullptr : link(head).prev; }

    /// jemalloc: edata_list_frag_next
    ALLOCATOR_ALWAYS_INLINE Extent * next(Extent * item) const { return (last() != item) ? link(item).next : nullptr; }

    /// jemalloc: edata_list_frag_empty
    ALLOCATOR_ALWAYS_INLINE bool empty() const { return head == nullptr; }

    /// jemalloc: edata_list_frag_append
    ALLOCATOR_ALWAYS_INLINE void append(Extent * item)
    {
        elementInit(item);
        if (!empty())
            meld(head, item);
        head = link(item).next;
    }

    /// jemalloc: edata_list_frag_prepend
    ALLOCATOR_ALWAYS_INLINE void prepend(Extent * item)
    {
        elementInit(item);
        if (!empty())
            meld(head, item);
        head = item;
    }

    /// jemalloc: edata_list_frag_remove
    ALLOCATOR_ALWAYS_INLINE void remove(Extent * item)
    {
        if (head == item)
            head = link(head).next;
        if (head != item)
            meld(link(item).next, item);
        else
            init();
    }

    /// jemalloc: edata_list_frag_concat
    ALLOCATOR_ALWAYS_INLINE void concat(ExtentListFragmentation & other)
    {
        if (empty())
        {
            head = other.head;
            other.init();
        }
        else if (!other.empty())
        {
            meld(head, other.head);
            other.init();
        }
    }

    /// Iterates from the head (ql_foreach order). The callback must not modify the list.
    template <typename F>
    ALLOCATOR_ALWAYS_INLINE void forEach(F && f) const
    {
        for (Extent * variable = head; variable != nullptr; variable = (link(variable).next != head) ? link(variable).next : nullptr)
            f(variable);
    }

private:
    Extent * head = nullptr;

    ALLOCATOR_ALWAYS_INLINE static RingLink<Extent> & link(Extent * e) { return e->profiling_info.fragmentation_link; }

    /// jemalloc: qr_new
    ALLOCATOR_ALWAYS_INLINE static void elementInit(Extent * e)
    {
        link(e).next = e;
        link(e).prev = e;
    }

    /// jemalloc: qr_meld
    ALLOCATOR_ALWAYS_INLINE static void meld(Extent * a, Extent * b)
    {
        link(link(b).prev).next = link(a).prev;
        link(a).prev = link(b).prev;
        link(b).prev = link(link(b).prev).next;
        link(link(a).prev).next = a;
        link(link(b).prev).next = b;
    }
};

}
