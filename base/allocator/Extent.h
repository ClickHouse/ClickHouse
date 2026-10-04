#pragma once

/// Extent metadata (jemalloc: `edata.h`, `edata.c`, `slab_data.h`).
///
/// `Extent` is jemalloc's `edata_t`: it describes a span of pages (or, for the base allocator, a span of bytes).
/// The layout is identical to the C struct (`sizeof` is observable through `stats.metadata`), including the unused
/// `e_ps` slot of the dropped HPA. Extents are carved out of zero-filled memory by `Base`, so the type is trivial
/// (atomic fields are accessed through `std::atomic_ref` with the same memory orders as jemalloc's `atomic_p_t`).

#include <allocator/Bitmap.h>
#include <allocator/Common.h>
#include <allocator/IntrusiveList.h>
#include <allocator/NsTime.h>
#include <allocator/PairingHeap.h>
#include <allocator/SizeClasses.h>

#include <atomic>
#include <type_traits>

namespace jemalloc
{

class ProfThreadContext; /// jemalloc: prof_tctx_t
class ProfRecent;        /// jemalloc: prof_recent_t

/// Every `Extent` handed out by `Base` is aligned to this, which frees the low pointer bits for the compact rtree
/// leaf encoding. jemalloc: EDATA_ALIGNMENT
inline constexpr size_t EDATA_ALIGNMENT = 128;

/// How many nodes are visited when enumerating an extent heap in search of a suitable extent; also the BFS queue size.
/// jemalloc: ESET_ENUMERATE_MAX_NUM
inline constexpr unsigned ESET_ENUMERATE_MAX_NUM = 32;

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
enum ExtentPai : unsigned
{
    EXTENT_PAI_PAC = 0,
    EXTENT_PAI_HPA = 1,
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
struct ExtentProfInfo
{
    /// Time when this was allocated.
    NsTime e_prof_alloc_time;
    /// Allocation request size.
    size_t e_prof_alloc_size;
    /// Atomic (acquire/release). jemalloc: atomic_p_t e_prof_tctx
    ProfThreadContext * e_prof_tctx;
    /// Atomic (relaxed); null means the recent allocation record no longer exists. Protected by
    /// `prof_recent_alloc_mtx`. jemalloc: atomic_p_t e_prof_recent_alloc
    ProfRecent * e_prof_recent_alloc;
    /// Linkage into the owning gctx's list of live sampled allocations (`gctx->frag_objs`). Both fields are protected
    /// by the owning gctx's lock; the gctx is reachable via `e_prof_tctx` while `e_prof_frag_tracked` is true.
    RingLink<Extent> e_prof_frag_link;
    bool e_prof_frag_tracked;
};

/// The information about an extent that lives in the emap (rtree leaf). jemalloc: edata_map_info_t
struct ExtentMapInfo
{
    bool slab;
    szind_t szind;
};

/// jemalloc: edata_cmp_summary_t
struct ExtentCmpSummary
{
    uint64_t sn;
    uintptr_t addr;
};

/// A field of `Extent::e_bits`. jemalloc: EDATA_BITS_*_WIDTH / _SHIFT / _MASK
struct ExtentBitField
{
    unsigned width;
    unsigned shift;

    constexpr uint64_t mask() const { return ((uint64_t(1) << width) - 1) << shift; }
    constexpr unsigned end() const { return width + shift; }
};

namespace extent_bits
{

/// 00000000 ... 0000ssss ssffffff ffffiiii iiiitttg zpcbaaaa aaaaaaaa (for LG_PAGE = 12)
inline constexpr ExtentBitField arena{MALLOCX_ARENA_BITS, 0};
inline constexpr ExtentBitField slab{1, arena.end()};
inline constexpr ExtentBitField committed{1, slab.end()};
inline constexpr ExtentBitField pai{1, committed.end()};
inline constexpr ExtentBitField zeroed{1, pai.end()};
inline constexpr ExtentBitField guarded{1, zeroed.end()};
inline constexpr ExtentBitField state{3, guarded.end()};
inline constexpr ExtentBitField szind{lgCeilConst(SC_NSIZES), state.end()};
inline constexpr ExtentBitField nfree{SC_LG_SLAB_MAXREGS + 1, szind.end()};
inline constexpr ExtentBitField binshard{6, nfree.end()};
inline constexpr ExtentBitField is_head{1, binshard.end()};

static_assert(is_head.end() <= 64);

}

/// jemalloc: EDATA_SIZE_MASK, EDATA_ESN_MASK
inline constexpr size_t EDATA_SIZE_MASK = ~(PAGE - 1);
inline constexpr size_t EDATA_ESN_MASK = PAGE - 1;

/// Extent (span of pages). jemalloc: edata_t
///
/// The fields are public and named as in jemalloc (they are accessed via the accessor methods everywhere except
/// in layout-sensitive code such as the containers' pointer-to-member links).
class Extent
{
public:
    /// a: arena_ind, b: slab, c: committed, p: pai, z: zeroed, g: guarded, t: state, i: szind, f: nfree,
    /// s: bin_shard, h: is_head. See `extent_bits`.
    uint64_t e_bits;

    /// Pointer to the extent that this structure is responsible for.
    void * e_addr;

    union
    {
        /// Extent size and serial number associated with the extent structure (different than the serial number
        /// for the extent at `e_addr`). ssssssss [...] ssssssss ssssnnnn nnnnnnnn
        size_t e_size_esn;
        /// Base extent size, which may not be a multiple of PAGE.
        size_t e_bsize;
    };

    /// HPA pageslab (`hpdata_t *`). HPA is dropped; the slot is kept for an identical size.
    void * e_ps;

    /// Serial number. These are not necessarily unique; splitting an extent results in two extents with the same
    /// serial number.
    uint64_t e_sn;

    union
    {
        /// List linkage used when the extent is active; either in the arena's large allocations or bin's `slabs_full`.
        RingLink<Extent> ql_link_active;
        /// Pairing heap linkage. Used whenever the extent is inactive (in the page allocators), or when it is active
        /// and in `slabs_nonfull`, or when the `Extent` is unassociated with an extent and sitting in an `ExtentPool`.
        PairingHeapLink<Extent> heap_link;
        PairingHeapLink<Extent> avail_link;
    };

    union
    {
        /// List linkage used when the extent is inactive: stashed dirty extents, ecache LRU.
        RingLink<Extent> ql_link_inactive;
        /// Small region slab metadata.
        SlabData e_slab_data;
        /// Profiling data, used for large objects.
        ExtentProfInfo e_prof_info;
    };

    /// --- Getters ---------------------------------------------------------------------------------------------------

    /// jemalloc: edata_arena_ind_get
    JE_ALWAYS_INLINE unsigned arenaInd() const
    {
        unsigned arena_ind = unsigned(getBits(extent_bits::arena));
        JE_ASSERT(arena_ind < MALLOCX_ARENA_LIMIT);
        return arena_ind;
    }

    /// jemalloc: edata_szind_get_maybe_invalid
    JE_ALWAYS_INLINE szind_t szindMaybeInvalid() const
    {
        szind_t szind = szind_t(getBits(extent_bits::szind));
        JE_ASSERT(szind <= SC_NSIZES);
        return szind;
    }

    /// jemalloc: edata_szind_get
    JE_ALWAYS_INLINE szind_t szind() const
    {
        szind_t szind = szindMaybeInvalid();
        JE_ASSERT(szind < SC_NSIZES); /// Never call when "invalid".
        return szind;
    }

    /// jemalloc: edata_usize_get
    JE_ALWAYS_INLINE size_t usize() const
    {
        /// When large size classes are disabled: if the usize from the index is not smaller than SC_LARGE_MINCLASS,
        /// the usize from the size is accurate; otherwise the usize from the index is accurate. When they are not
        /// disabled, the two are the same for usize >= SC_LARGE_MINCLASS. Sampled small allocations are promoted:
        /// their extent size is recorded in the size, while their szind reflects the true usize.
        szind_t ind = szind();
        if (!sz::largeSizeClassesDisabled() || ind < SC_NBINS)
        {
            size_t usize_from_ind = sz::indexToSize(ind);
            if constexpr (config::debug)
            {
                if (!sz::largeSizeClassesDisabled() && usize_from_ind >= SC_LARGE_MINCLASS)
                {
                    size_t size = (e_size_esn & EDATA_SIZE_MASK);
                    JE_ASSERT(size > sz_large_pad);
                    JE_ASSERT(usize_from_ind == size - sz_large_pad);
                }
            }
            return usize_from_ind;
        }

        size_t size = (e_size_esn & EDATA_SIZE_MASK);
        JE_ASSERT(size > sz_large_pad);
        size_t usize_from_size = size - sz_large_pad;
        /// No matter whether large size classes are disabled or not, the usize from the size is not accurate when
        /// smaller than SC_LARGE_MINCLASS.
        JE_ASSERT(usize_from_size >= SC_LARGE_MINCLASS);
        return usize_from_size;
    }

    /// jemalloc: edata_binshard_get
    JE_ALWAYS_INLINE unsigned binshard() const
    {
        unsigned binshard = unsigned(getBits(extent_bits::binshard));
        JE_ASSERT(binshard < bin_infos[szind()].n_shards);
        return binshard;
    }

    /// jemalloc: edata_sn_get
    JE_ALWAYS_INLINE uint64_t sn() const { return e_sn; }

    /// jemalloc: edata_state_get
    JE_ALWAYS_INLINE ExtentState state() const { return ExtentState(getBits(extent_bits::state)); }

    /// jemalloc: edata_guarded_get
    JE_ALWAYS_INLINE bool guarded() const { return bool(getBits(extent_bits::guarded)); }

    /// jemalloc: edata_zeroed_get
    JE_ALWAYS_INLINE bool zeroed() const { return bool(getBits(extent_bits::zeroed)); }

    /// jemalloc: edata_committed_get
    JE_ALWAYS_INLINE bool committed() const { return bool(getBits(extent_bits::committed)); }

    /// jemalloc: edata_pai_get
    JE_ALWAYS_INLINE ExtentPai pai() const { return ExtentPai(getBits(extent_bits::pai)); }

    /// jemalloc: edata_slab_get
    JE_ALWAYS_INLINE bool slab() const { return bool(getBits(extent_bits::slab)); }

    /// jemalloc: edata_nfree_get
    JE_ALWAYS_INLINE unsigned nfree() const
    {
        JE_ASSERT(slab());
        return unsigned(getBits(extent_bits::nfree));
    }

    /// jemalloc: edata_is_head_get
    JE_ALWAYS_INLINE bool isHead() const { return bool(getBits(extent_bits::is_head)); }

    /// jemalloc: edata_base_get
    JE_ALWAYS_INLINE void * base() const
    {
        JE_ASSERT(e_addr == pageAddrToBase(e_addr) || !slab());
        return pageAddrToBase(e_addr);
    }

    /// jemalloc: edata_addr_get
    JE_ALWAYS_INLINE void * addr() const
    {
        JE_ASSERT(e_addr == pageAddrToBase(e_addr) || !slab());
        return e_addr;
    }

    /// jemalloc: edata_size_get
    JE_ALWAYS_INLINE size_t size() const { return e_size_esn & EDATA_SIZE_MASK; }

    /// jemalloc: edata_esn_get
    JE_ALWAYS_INLINE size_t esn() const { return e_size_esn & EDATA_ESN_MASK; }

    /// jemalloc: edata_bsize_get
    JE_ALWAYS_INLINE size_t bsize() const { return e_bsize; }

    /// jemalloc: edata_ps_get
    JE_ALWAYS_INLINE void * ps() const
    {
        JE_ASSERT(pai() == EXTENT_PAI_HPA);
        return e_ps;
    }

    /// jemalloc: edata_before_get
    JE_ALWAYS_INLINE void * before() const { return static_cast<std::byte *>(base()) - PAGE; }

    /// jemalloc: edata_last_get
    JE_ALWAYS_INLINE void * last() const { return static_cast<std::byte *>(base()) + size() - PAGE; }

    /// jemalloc: edata_past_get
    JE_ALWAYS_INLINE void * past() const { return static_cast<std::byte *>(base()) + size(); }

    /// jemalloc: edata_slab_data_get
    JE_ALWAYS_INLINE SlabData * slabData()
    {
        JE_ASSERT(slab());
        return &e_slab_data;
    }

    /// jemalloc: edata_slab_data_get_const
    JE_ALWAYS_INLINE const SlabData * slabData() const
    {
        JE_ASSERT(slab());
        return &e_slab_data;
    }

    /// jemalloc: edata_prof_tctx_get
    JE_ALWAYS_INLINE ProfThreadContext * profTctx() const
    {
        return std::atomic_ref<ProfThreadContext *>(const_cast<ProfThreadContext *&>(e_prof_info.e_prof_tctx))
            .load(std::memory_order_acquire);
    }

    /// jemalloc: edata_prof_alloc_time_get
    JE_ALWAYS_INLINE const NsTime * profAllocTime() const { return &e_prof_info.e_prof_alloc_time; }

    /// jemalloc: edata_prof_alloc_size_get
    JE_ALWAYS_INLINE size_t profAllocSize() const { return e_prof_info.e_prof_alloc_size; }

    /// jemalloc: edata_prof_recent_alloc_get_dont_call_directly
    JE_ALWAYS_INLINE ProfRecent * profRecentAllocGetDontCallDirectly() const
    {
        return std::atomic_ref<ProfRecent *>(const_cast<ProfRecent *&>(e_prof_info.e_prof_recent_alloc))
            .load(std::memory_order_relaxed);
    }

    /// jemalloc: edata_prof_frag_tracked_get
    JE_ALWAYS_INLINE bool profFragTracked() const { return e_prof_info.e_prof_frag_tracked; }

    /// --- Setters ---------------------------------------------------------------------------------------------------

    /// jemalloc: edata_arena_ind_set
    JE_ALWAYS_INLINE void setArenaInd(unsigned arena_ind) { setBits(extent_bits::arena, arena_ind); }

    /// The assertion assumes szind is set already.
    /// jemalloc: edata_binshard_set
    JE_ALWAYS_INLINE void setBinshard(unsigned binshard)
    {
        JE_ASSERT(binshard < bin_infos[szind()].n_shards);
        setBits(extent_bits::binshard, binshard);
    }

    /// jemalloc: edata_addr_set
    JE_ALWAYS_INLINE void setAddr(void * addr) { e_addr = addr; }

    /// jemalloc: edata_size_set
    JE_ALWAYS_INLINE void setSize(size_t size)
    {
        JE_ASSERT((size & ~EDATA_SIZE_MASK) == 0);
        e_size_esn = size | (e_size_esn & ~EDATA_SIZE_MASK);
    }

    /// jemalloc: edata_esn_set
    JE_ALWAYS_INLINE void setEsn(size_t esn) { e_size_esn = (e_size_esn & ~EDATA_ESN_MASK) | (esn & EDATA_ESN_MASK); }

    /// jemalloc: edata_bsize_set
    JE_ALWAYS_INLINE void setBsize(size_t bsize) { e_bsize = bsize; }

    /// jemalloc: edata_ps_set
    JE_ALWAYS_INLINE void setPs(void * ps)
    {
        JE_ASSERT(pai() == EXTENT_PAI_HPA);
        e_ps = ps;
    }

    /// SC_NSIZES means "invalid".
    /// jemalloc: edata_szind_set
    JE_ALWAYS_INLINE void setSzind(szind_t szind)
    {
        JE_ASSERT(szind <= SC_NSIZES);
        setBits(extent_bits::szind, szind);
    }

    /// jemalloc: edata_nfree_set
    JE_ALWAYS_INLINE void setNfree(unsigned nfree)
    {
        JE_ASSERT(slab());
        setBits(extent_bits::nfree, nfree);
    }

    /// The assertion assumes szind is set already.
    /// jemalloc: edata_nfree_binshard_set
    JE_ALWAYS_INLINE void setNfreeBinshard(unsigned nfree, unsigned binshard)
    {
        JE_ASSERT(binshard < bin_infos[szind()].n_shards);
        e_bits = (e_bits & (~extent_bits::nfree.mask() & ~extent_bits::binshard.mask()))
            | (uint64_t(binshard) << extent_bits::binshard.shift) | (uint64_t(nfree) << extent_bits::nfree.shift);
    }

    /// jemalloc: edata_nfree_inc
    JE_ALWAYS_INLINE void nfreeInc()
    {
        JE_ASSERT(slab());
        e_bits += uint64_t(1) << extent_bits::nfree.shift;
    }

    /// jemalloc: edata_nfree_dec
    JE_ALWAYS_INLINE void nfreeDec()
    {
        JE_ASSERT(slab());
        e_bits -= uint64_t(1) << extent_bits::nfree.shift;
    }

    /// jemalloc: edata_nfree_sub
    JE_ALWAYS_INLINE void nfreeSub(uint64_t n)
    {
        JE_ASSERT(slab());
        e_bits -= n << extent_bits::nfree.shift;
    }

    /// jemalloc: edata_sn_set
    JE_ALWAYS_INLINE void setSn(uint64_t sn) { e_sn = sn; }

    /// jemalloc: edata_state_set
    JE_ALWAYS_INLINE void setState(ExtentState state) { setBits(extent_bits::state, state); }

    /// jemalloc: edata_guarded_set
    JE_ALWAYS_INLINE void setGuarded(bool guarded) { setBits(extent_bits::guarded, guarded); }

    /// jemalloc: edata_zeroed_set
    JE_ALWAYS_INLINE void setZeroed(bool zeroed) { setBits(extent_bits::zeroed, zeroed); }

    /// jemalloc: edata_committed_set
    JE_ALWAYS_INLINE void setCommitted(bool committed) { setBits(extent_bits::committed, committed); }

    /// jemalloc: edata_pai_set
    JE_ALWAYS_INLINE void setPai(ExtentPai pai) { setBits(extent_bits::pai, pai); }

    /// jemalloc: edata_slab_set
    JE_ALWAYS_INLINE void setSlab(bool slab) { setBits(extent_bits::slab, slab); }

    /// jemalloc: edata_is_head_set
    JE_ALWAYS_INLINE void setIsHead(bool is_head) { setBits(extent_bits::is_head, is_head); }

    /// jemalloc: edata_prof_tctx_set
    JE_ALWAYS_INLINE void setProfTctx(ProfThreadContext * tctx)
    {
        std::atomic_ref<ProfThreadContext *>(e_prof_info.e_prof_tctx).store(tctx, std::memory_order_release);
    }

    /// jemalloc: edata_prof_alloc_time_set
    JE_ALWAYS_INLINE void setProfAllocTime(const NsTime * t) { e_prof_info.e_prof_alloc_time.copy(*t); }

    /// jemalloc: edata_prof_alloc_size_set
    JE_ALWAYS_INLINE void setProfAllocSize(size_t size) { e_prof_info.e_prof_alloc_size = size; }

    /// jemalloc: edata_prof_recent_alloc_set_dont_call_directly
    JE_ALWAYS_INLINE void setProfRecentAllocDontCallDirectly(ProfRecent * recent_alloc)
    {
        std::atomic_ref<ProfRecent *>(e_prof_info.e_prof_recent_alloc).store(recent_alloc, std::memory_order_relaxed);
    }

    /// jemalloc: edata_prof_frag_tracked_set
    JE_ALWAYS_INLINE void setProfFragTracked(bool tracked) { e_prof_info.e_prof_frag_tracked = tracked; }

    /// --- Initialization --------------------------------------------------------------------------------------------

    /// Because this is implemented as a sequence of bitfield modifications, even though each individual bit is
    /// properly initialized, it technically reads uninitialized data. Most callers get their extents from zeroing
    /// sources; callers who make stack extents need to zero them manually.
    /// jemalloc: edata_init
    JE_ALWAYS_INLINE void init(
        unsigned arena_ind,
        void * addr,
        size_t size,
        bool slab,
        szind_t szind,
        uint64_t sn,
        ExtentState state,
        bool zeroed,
        bool committed,
        ExtentPai pai,
        ExtentHeadState is_head)
    {
        JE_ASSERT(addr == pageAddrToBase(addr) || !slab);

        setArenaInd(arena_ind);
        setAddr(addr);
        setSize(size);
        setSlab(slab);
        setSzind(szind);
        setSn(sn);
        setState(state);
        setGuarded(false);
        setZeroed(zeroed);
        setCommitted(committed);
        setPai(pai);
        setIsHead(is_head == EXTENT_IS_HEAD);
        if constexpr (config::prof)
            setProfTctx(nullptr);
    }

    /// jemalloc: edata_binit
    JE_ALWAYS_INLINE void initBase(void * addr, size_t bsize, uint64_t sn, bool reused)
    {
        setArenaInd((1U << MALLOCX_ARENA_BITS) - 1);
        setAddr(addr);
        setBsize(bsize);
        setSlab(false);
        setSzind(SC_NSIZES);
        setSn(sn);
        setState(extent_state_active);
        /// See comments in `base_edata_is_reused`.
        setGuarded(reused);
        setZeroed(true);
        setCommitted(true);
        /// This isn't strictly true, but base allocated extents never get deallocated and can't be looked up in the
        /// emap, but no sense in wasting a state bit to encode this fact.
        setPai(EXTENT_PAI_PAC);
    }

    /// --- Comparators -----------------------------------------------------------------------------------------------

    /// jemalloc: edata_esn_comp
    static JE_ALWAYS_INLINE int compareEsn(const Extent * a, const Extent * b)
    {
        size_t a_esn = a->esn();
        size_t b_esn = b->esn();
        return (a_esn > b_esn) - (a_esn < b_esn);
    }

    /// Compares the addresses of the `Extent` structures themselves.
    /// jemalloc: edata_ead_comp
    static JE_ALWAYS_INLINE int compareEad(const Extent * a, const Extent * b)
    {
        uintptr_t a_eaddr = reinterpret_cast<uintptr_t>(a);
        uintptr_t b_eaddr = reinterpret_cast<uintptr_t>(b);
        return (a_eaddr > b_eaddr) - (a_eaddr < b_eaddr);
    }

    /// jemalloc: edata_cmp_summary_get
    JE_ALWAYS_INLINE ExtentCmpSummary cmpSummary() const
    {
        ExtentCmpSummary result;
        result.sn = sn();
        result.addr = reinterpret_cast<uintptr_t>(addr());
        return result;
    }

    /// Lexicographic (sn, addr). Branchless: the sn comparison is multiplied by 2 so that, when non-zero, it dominates
    /// the addr comparison (the branches would be badly predicted; measurably faster).
    /// jemalloc: edata_cmp_summary_comp (the variant without JEMALLOC_HAVE_INT128)
    static JE_ALWAYS_INLINE int compareSummary(ExtentCmpSummary a, ExtentCmpSummary b)
    {
        return (2 * ((a.sn > b.sn) - (a.sn < b.sn))) + ((a.addr > b.addr) - (a.addr < b.addr));
    }

    /// jemalloc: edata_snad_comp
    static JE_ALWAYS_INLINE int compareSnad(const Extent * a, const Extent * b)
    {
        return compareSummary(a->cmpSummary(), b->cmpSummary());
    }

    /// Lexicographic (esn, address of the structure), branchless.
    /// jemalloc: edata_esnead_comp
    static JE_ALWAYS_INLINE int compareEsnead(const Extent * a, const Extent * b)
    {
        return (2 * compareEsn(a, b)) + compareEad(a, b);
    }

private:
    JE_ALWAYS_INLINE uint64_t getBits(ExtentBitField field) const { return (e_bits & field.mask()) >> field.shift; }

    JE_ALWAYS_INLINE void setBits(ExtentBitField field, uint64_t value)
    {
        e_bits = (e_bits & ~field.mask()) | (value << field.shift);
    }
};

static_assert(std::is_trivial_v<Extent>);
static_assert(std::is_standard_layout_v<Extent>);
static_assert(sizeof(SlabData) >= sizeof(ExtentProfInfo));
static_assert(sizeof(ExtentProfInfo) == 56);
/// Measured from the C build (`stats.metadata` depends on it).
static_assert(sizeof(Extent) == (LG_PAGE == 12 ? 128 : (LG_PAGE == 14 ? 328 : 1112)));
static_assert(EDATA_ALIGNMENT >= alignof(Extent));

struct ExtentSnadCompare
{
    JE_ALWAYS_INLINE int operator()(const Extent * a, const Extent * b) const { return Extent::compareSnad(a, b); }
};

struct ExtentEsneadCompare
{
    JE_ALWAYS_INLINE int operator()(const Extent * a, const Extent * b) const { return Extent::compareEsnead(a, b); }
};

/// The heap of extents ordered by (sn, addr): eset bins, base avail heaps, bin `slabs_nonfull`.
/// jemalloc: edata_heap_t (ph_gen(, edata_heap, edata_t, heap_link, edata_snad_comp))
using ExtentHeap = PairingHeap<Extent, &Extent::heap_link, ExtentSnadCompare>;

/// The heap of unused `Extent` structures ordered by (esn, structure address): `ExtentPool`, base `edata_avail`.
/// jemalloc: edata_avail_t (ph_gen(, edata_avail, edata_t, avail_link, edata_esnead_comp))
using ExtentAvailHeap = PairingHeap<Extent, &Extent::avail_link, ExtentEsneadCompare>;

/// jemalloc: edata_heap_enumerate_helper_t, edata_avail_enumerate_helper_t
using ExtentHeapEnumerateHelper = ExtentHeap::EnumerateHelper<ESET_ENUMERATE_MAX_NUM>;
using ExtentAvailEnumerateHelper = ExtentAvailHeap::EnumerateHelper<ESET_ENUMERATE_MAX_NUM>;

/// jemalloc: edata_list_active_t (TYPED_LIST(edata_list_active, edata_t, ql_link_active))
using ExtentListActive = TypedList<Extent, &Extent::ql_link_active>;

/// jemalloc: edata_list_inactive_t (TYPED_LIST(edata_list_inactive, edata_t, ql_link_inactive))
using ExtentListInactive = TypedList<Extent, &Extent::ql_link_inactive>;

/// The list of live sampled allocations of a gctx, linked through `e_prof_info.e_prof_frag_link`.
/// A nested member cannot be named by a pointer-to-member, so this is a direct port of the `ql`/`TYPED_LIST`
/// operations with exactly the same semantics as `TypedList`.
/// jemalloc: edata_list_frag_t (TYPED_LIST(edata_list_frag, edata_t, e_prof_info.e_prof_frag_link))
class ExtentListFrag
{
public:
    constexpr ExtentListFrag() = default;

    ExtentListFrag(const ExtentListFrag &) = delete;
    ExtentListFrag & operator=(const ExtentListFrag &) = delete;

    /// jemalloc: edata_list_frag_init
    JE_ALWAYS_INLINE void init() { head = nullptr; }

    /// jemalloc: edata_list_frag_first
    JE_ALWAYS_INLINE Extent * first() const { return head; }

    /// jemalloc: edata_list_frag_last
    JE_ALWAYS_INLINE Extent * last() const { return empty() ? nullptr : link(head).prev; }

    /// jemalloc: edata_list_frag_next
    JE_ALWAYS_INLINE Extent * next(Extent * item) const { return (last() != item) ? link(item).next : nullptr; }

    /// jemalloc: edata_list_frag_empty
    JE_ALWAYS_INLINE bool empty() const { return head == nullptr; }

    /// jemalloc: edata_list_frag_append
    JE_ALWAYS_INLINE void append(Extent * item)
    {
        elementInit(item);
        if (!empty())
            meld(head, item);
        head = link(item).next;
    }

    /// jemalloc: edata_list_frag_prepend
    JE_ALWAYS_INLINE void prepend(Extent * item)
    {
        elementInit(item);
        if (!empty())
            meld(head, item);
        head = item;
    }

    /// jemalloc: edata_list_frag_remove
    JE_ALWAYS_INLINE void remove(Extent * item)
    {
        if (head == item)
            head = link(head).next;
        if (head != item)
            meld(link(item).next, item);
        else
            init();
    }

    /// jemalloc: edata_list_frag_concat
    JE_ALWAYS_INLINE void concat(ExtentListFrag & other)
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
    JE_ALWAYS_INLINE void forEach(F && f) const
    {
        for (Extent * var = head; var != nullptr; var = (link(var).next != head) ? link(var).next : nullptr)
            f(var);
    }

private:
    Extent * head = nullptr;

    JE_ALWAYS_INLINE static RingLink<Extent> & link(Extent * e) { return e->e_prof_info.e_prof_frag_link; }

    /// jemalloc: qr_new
    JE_ALWAYS_INLINE static void elementInit(Extent * e)
    {
        link(e).next = e;
        link(e).prev = e;
    }

    /// jemalloc: qr_meld
    JE_ALWAYS_INLINE static void meld(Extent * a, Extent * b)
    {
        link(link(b).prev).next = link(a).prev;
        link(a).prev = link(b).prev;
        link(b).prev = link(link(b).prev).next;
        link(link(a).prev).next = a;
        link(link(b).prev).next = b;
    }
};

}
