/// Compares `RadixTree` with jemalloc's rtree (`radix_tree_oracle_ref.c`, linked with the reference `lib_jemalloc.a`):
/// the geometry and the layout constants (also those of `edata_t`), the key functions and the leaf encoding, and a
/// long randomized trace of tree operations on both trees, after each of which the results and the complete state of
/// the per-thread lookup cache (L1 and L2 leafkeys; leaf pointers up to a bijection) must be identical.

#include <allocator/Base.h>
#include <allocator/ExtentHooks.h>
#include <allocator/Pages.h>
#include <allocator/RadixTree.h>

#include "Test.h"

#include <cstddef>
#include <map>
#include <set>
#include <vector>

using namespace jemalloc;

extern "C"
{
size_t ref_constant(int which);
void ref_level(unsigned level, unsigned * bits, unsigned * cumbits);
uintptr_t ref_leafkey(uintptr_t key);
uintptr_t ref_subkey(uintptr_t key, unsigned level);
size_t ref_direct_map(uintptr_t key);
void ref_encode(uintptr_t edata, unsigned szind, unsigned state, int is_head, int slab, uintptr_t * out);
void ref_decode(uintptr_t bits, uintptr_t * out);
int ref_init();
void ref_ctx_get(uintptr_t * leafkeys, uintptr_t * leaves);
uintptr_t ref_lookup(uintptr_t key, int dependent, int init_missing);
int ref_write(uintptr_t key, uintptr_t edata, unsigned szind, unsigned state, int is_head, int slab);
void ref_read(uintptr_t key, uintptr_t * out);
int ref_read_independent(uintptr_t key, uintptr_t * out);
int ref_metadata_try_read_fast(uintptr_t key, uintptr_t * out);
void ref_clear(uintptr_t key);
void ref_write_range(uintptr_t base, uintptr_t end, uintptr_t edata, unsigned szind, unsigned state, int is_head, int slab);
void ref_clear_range(uintptr_t base, uintptr_t end);
void ref_state_update(uintptr_t key1, uintptr_t key2, unsigned state);

/// `rtree.o` pulls in the rest of the reference jemalloc, including the libunwind-based profiler backtrace, which is
/// never called here.
int unw_backtrace(void **, int)
{
    return 0;
}
}

namespace
{

struct Rng
{
    uint64_t state = 0x9e3779b97f4a7c15ULL;

    uint64_t next()
    {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        return state;
    }

    uint64_t below(uint64_t n) { return next() % n; }
};

RadixTreeContents makeContents(uintptr_t edata, unsigned szind, unsigned state, bool is_head, bool slab)
{
    return {reinterpret_cast<Extent *>(edata), {szind, ExtentState(state), is_head, slab}};
}

void splitContents(const RadixTreeContents & contents, uintptr_t * out)
{
    out[0] = reinterpret_cast<uintptr_t>(contents.edata);
    out[1] = contents.metadata.szind;
    out[2] = contents.metadata.state;
    out[3] = contents.metadata.is_head;
    out[4] = contents.metadata.slab;
}

/// A random 128-aligned fake edata address in the user half of the address space (or null).
uintptr_t randomEdata(Rng & rng)
{
    if (rng.below(8) == 0)
        return 0;
    return (rng.next() & ((uintptr_t(1) << (LG_VADDR - 1)) - 1)) & ~uintptr_t(EDATA_ALIGNMENT - 1);
}

}

TEST(RadixTree, Geometry)
{
    CHECK_EQ(size_t(RTREE_NHIB), ref_constant(0));
    CHECK_EQ(size_t(RTREE_NLIB), ref_constant(1));
    CHECK_EQ(size_t(RTREE_NSB), ref_constant(2));
    CHECK_EQ(size_t(RTREE_HEIGHT), ref_constant(3));
    CHECK_EQ(size_t(RTREE_LEAF_COMPACT), ref_constant(4));
    CHECK_EQ(size_t(rtreeLeafMaskbits()), ref_constant(5));
    CHECK_EQ(sizeof(RadixTreeContext), ref_constant(6));
    CHECK_EQ(sizeof(RadixTree), ref_constant(7));
    CHECK_EQ(sizeof(RadixTreeLeafElm), ref_constant(8));
    CHECK_EQ(sizeof(RadixTreeNodeElm), ref_constant(9));
    CHECK_EQ(size_t(RTREE_CTX_NCACHE), ref_constant(10));
    CHECK_EQ(size_t(RTREE_CTX_NCACHE_L2), ref_constant(11));
    CHECK_EQ(offsetof(RadixTree, root), ref_constant(12));
    CHECK_EQ(RadixTree::root_size, ref_constant(13));
    for (unsigned level = 0; level < RTREE_HEIGHT; ++level)
    {
        unsigned bits;
        unsigned cumbits;
        ref_level(level, &bits, &cumbits);
        CHECK_EQ(rtree_levels[level].bits, bits);
        CHECK_EQ(rtree_levels[level].cumbits, cumbits);
    }
}

TEST(RadixTree, ExtentLayout)
{
    CHECK_EQ(sizeof(Extent), ref_constant(20));
    CHECK_EQ(offsetof(Extent, e_addr), ref_constant(21));
    CHECK_EQ(offsetof(Extent, e_size_esn), ref_constant(22));
    CHECK_EQ(offsetof(Extent, e_ps), ref_constant(23));
    CHECK_EQ(offsetof(Extent, e_sn), ref_constant(24));
    CHECK_EQ(offsetof(Extent, ql_link_active), ref_constant(25));
    CHECK_EQ(offsetof(Extent, heap_link), ref_constant(26));
    CHECK_EQ(offsetof(Extent, ql_link_inactive), ref_constant(27));
    CHECK_EQ(offsetof(Extent, e_slab_data), ref_constant(28));
    CHECK_EQ(offsetof(Extent, e_prof_info), ref_constant(29));
    CHECK_EQ(sizeof(SlabData), ref_constant(30));
    CHECK_EQ(sizeof(ExtentProfInfo), ref_constant(31));
    CHECK_EQ(offsetof(ExtentProfInfo, e_prof_frag_link), ref_constant(32));
    CHECK_EQ(offsetof(ExtentProfInfo, e_prof_frag_tracked), ref_constant(33));
    CHECK_EQ(EDATA_ALIGNMENT, ref_constant(34));
    CHECK_EQ(size_t(ESET_ENUMERATE_MAX_NUM), ref_constant(35));

    const ExtentBitField fields[] = {extent_bits::arena, extent_bits::slab, extent_bits::committed, extent_bits::pai,
        extent_bits::zeroed, extent_bits::guarded, extent_bits::state, extent_bits::szind, extent_bits::nfree,
        extent_bits::binshard, extent_bits::is_head};
    for (int i = 0; i < 11; ++i)
        CHECK_EQ(size_t(fields[i].shift), ref_constant(40 + i));
    CHECK_EQ(size_t(extent_bits::szind.width), ref_constant(51));
    CHECK_EQ(size_t(extent_bits::nfree.width), ref_constant(52));
}

TEST(RadixTree, KeyFunctions)
{
    Rng rng;
    for (int i = 0; i < 200000; ++i)
    {
        uintptr_t key = rng.next();
        if (i % 2)
            key &= (uintptr_t(1) << LG_VADDR) - 1;
        if (key == 0)
            continue;
        CHECK_EQ(rtreeLeafkey(key), ref_leafkey(key));
        CHECK_EQ(rtreeCacheDirectMap(key), ref_direct_map(key));
        for (unsigned level = 0; level < RTREE_HEIGHT; ++level)
            CHECK_EQ(rtreeSubkey(key, level), ref_subkey(key, level));
    }
}

TEST(RadixTree, Encoding)
{
    Rng rng;
    for (int i = 0; i < 200000; ++i)
    {
        uintptr_t edata = randomEdata(rng);
        if (i % 3 == 0 && edata != 0)
            edata |= ~((uintptr_t(1) << (LG_VADDR - 1)) - 1); /// Kernel half: tests sign/zero extension.
        unsigned szind = unsigned(rng.below(SC_NSIZES + 1));
        unsigned state = unsigned(rng.below(extent_state_max + 1));
        bool is_head = rng.below(2);
        bool slab = rng.below(2);

        uintptr_t ref[5];
        ref_encode(edata, szind, state, is_head, slab, ref);
        RadixTreeEncoded encoded = rtreeContentsEncode(makeContents(edata, szind, state, is_head, slab));
        CHECK_EQ(encoded.bits, ref[0]);
        CHECK_EQ(uintptr_t(encoded.additional), ref[1]);

        if constexpr (RTREE_LEAF_COMPACT)
        {
            uintptr_t decoded_ref[5];
            uintptr_t decoded[5];
            ref_decode(encoded.bits, decoded_ref);
            splitContents(rtreeLeafElmBitsDecode(encoded.bits), decoded);
            for (int j = 0; j < 5; ++j)
                CHECK_EQ(decoded[j], decoded_ref[j]);
        }
    }

    CHECK_EQ(rtree_contents_cleared.metadata.szind, SC_NSIZES);
}

namespace
{

constinit RadixTree tree;
RadixTreeContext ctx;

/// C leaf pointer -> C++ leaf pointer.
std::map<uintptr_t, uintptr_t> leaf_bijection;
int mismatches = 0;

void checkLeaf(uintptr_t ref_leaf, uintptr_t leaf)
{
    if ((ref_leaf == 0) != (leaf == 0))
    {
        CHECK_EQ(ref_leaf == 0, leaf == 0);
        ++mismatches;
        return;
    }
    if (ref_leaf == 0)
        return;
    auto [it, inserted] = leaf_bijection.emplace(ref_leaf, leaf);
    if (!inserted && it->second != leaf)
    {
        CHECK_EQ(it->second, leaf);
        ++mismatches;
    }
}

void compareCache()
{
    uintptr_t ref_leafkeys[RTREE_CTX_NCACHE + RTREE_CTX_NCACHE_L2];
    uintptr_t ref_leaves[RTREE_CTX_NCACHE + RTREE_CTX_NCACHE_L2];
    ref_ctx_get(ref_leafkeys, ref_leaves);
    for (unsigned i = 0; i < RTREE_CTX_NCACHE + RTREE_CTX_NCACHE_L2; ++i)
    {
        const RadixTreeCacheElm & elm = i < RTREE_CTX_NCACHE ? ctx.cache[i] : ctx.l2_cache[i - RTREE_CTX_NCACHE];
        if (elm.leafkey != ref_leafkeys[i])
        {
            CHECK_EQ(elm.leafkey, ref_leafkeys[i]);
            ++mismatches;
        }
        checkLeaf(ref_leaves[i], reinterpret_cast<uintptr_t>(elm.leaf));
    }
}

/// The element pointer, as its leaf and index.
void compareElm(uintptr_t key, uintptr_t ref_elm, RadixTreeLeafElm * elm)
{
    uintptr_t offset = rtreeSubkey(key, RTREE_HEIGHT - 1) * sizeof(RadixTreeLeafElm);
    if ((ref_elm == 0) != (elm == nullptr))
    {
        CHECK_EQ(ref_elm == 0, elm == nullptr);
        ++mismatches;
        return;
    }
    if (ref_elm != 0)
        checkLeaf(ref_elm - offset, reinterpret_cast<uintptr_t>(elm) - offset);
}

void compareContents(const uintptr_t * ref, const RadixTreeContents & contents)
{
    uintptr_t mine[5];
    splitContents(contents, mine);
    for (int j = 0; j < 5; ++j)
    {
        if (mine[j] != ref[j])
        {
            CHECK_EQ(mine[j], ref[j]);
            ++mismatches;
        }
    }
}

}

TEST(RadixTree, RandomizedTrace)
{
    REQUIRE(!pages::boot());
    REQUIRE(ref_init() == 0);
    Base * base = Base::create(nullptr, 0, &ehooks_default_extent_hooks, true);
    REQUIRE(base != nullptr);
    REQUIRE(!tree.init(base, true));
    ctx.init();
    compareCache();

    /// A pool of leaves: many share an L1 slot, so that both cache levels are exercised.
    const unsigned maskbits = rtreeLeafMaskbits();
    const uintptr_t leaf_span = uintptr_t(1) << maskbits;
    const uintptr_t pages_per_leaf = leaf_span >> LG_PAGE;
    std::vector<uintptr_t> leaf_bases;
    for (uintptr_t i = 1; i <= 24; ++i)
        leaf_bases.push_back(i * leaf_span);
    for (uintptr_t i = 1; i <= 24; ++i)
        leaf_bases.push_back((i * RTREE_CTX_NCACHE + 5) * leaf_span);
    for (uintptr_t i = 1; i <= 8; ++i)
        leaf_bases.push_back(((uintptr_t(1) << (LG_VADDR - maskbits)) - i) * leaf_span);

    std::set<uintptr_t> existing_leaves; /// Leaf bases that were created by a write.
    std::map<uintptr_t, bool> nonnull; /// Page -> the element has a non-null edata.

    Rng rng;
    auto random_key_in = [&](uintptr_t leaf_base)
    { return leaf_base + rng.below(pages_per_leaf) * PAGE + (rng.below(4) == 0 ? rng.below(PAGE) : 0); };
    auto random_existing_leaf = [&]() -> uintptr_t
    {
        auto it = existing_leaves.begin();
        std::advance(it, rng.below(existing_leaves.size()));
        return *it;
    };

    const int steps = 200000;
    for (int step = 0; step < steps && mismatches < 20; ++step)
    {
        unsigned op = unsigned(rng.below(100));
        if (existing_leaves.empty())
            op = 0;

        if (op < 25)
        {
            /// write (init_missing)
            uintptr_t leaf_base = leaf_bases[rng.below(leaf_bases.size())];
            uintptr_t key = random_key_in(leaf_base);
            uintptr_t edata = randomEdata(rng);
            unsigned szind = unsigned(rng.below(SC_NSIZES + 1));
            unsigned state = unsigned(rng.below(extent_state_max + 1));
            bool is_head = rng.below(2);
            bool slab = rng.below(2);
            int ref_err = ref_write(key, edata, szind, state, is_head, slab);
            bool err = tree.write(nullptr, &ctx, key, makeContents(edata, szind, state, is_head, slab));
            CHECK_EQ(bool(ref_err), err);
            existing_leaves.insert(leaf_base);
            nonnull[pageFloor(key)] = edata != 0;
        }
        else if (op < 45)
        {
            /// dependent read in an existing leaf
            uintptr_t key = random_key_in(random_existing_leaf());
            uintptr_t ref[5];
            ref_read(key, ref);
            compareContents(ref, tree.read(nullptr, &ctx, key));
        }
        else if (op < 60)
        {
            /// independent read anywhere (also in leaves that don't exist)
            uintptr_t key = random_key_in(leaf_bases[rng.below(leaf_bases.size())]);
            uintptr_t ref[5];
            RadixTreeContents contents;
            int ref_err = ref_read_independent(key, ref);
            bool err = tree.readIndependent(nullptr, &ctx, key, &contents);
            CHECK_EQ(bool(ref_err), err);
            if (!ref_err && !err)
                compareContents(ref, contents);
        }
        else if (op < 70)
        {
            /// lookup with various flags (never dependent + init_missing)
            uintptr_t key = random_key_in(leaf_bases[rng.below(leaf_bases.size())]);
            bool dependent = false;
            bool init_missing = rng.below(2);
            uintptr_t ref_elm = ref_lookup(key, dependent, init_missing);
            RadixTreeLeafElm * elm = tree.leafElmLookup(nullptr, &ctx, key, dependent, init_missing);
            compareElm(key, ref_elm, elm);
            if (init_missing)
                existing_leaves.insert(rtreeLeafkey(key));
        }
        else if (op < 78)
        {
            /// fast metadata read (L1 only)
            uintptr_t key = random_key_in(leaf_bases[rng.below(leaf_bases.size())]);
            uintptr_t ref[4];
            RadixTreeMetadata metadata;
            int ref_miss = ref_metadata_try_read_fast(key, ref);
            bool miss = tree.metadataTryReadFast(nullptr, &ctx, key, &metadata);
            CHECK_EQ(bool(ref_miss), miss);
            if (!ref_miss && !miss)
            {
                CHECK_EQ(uintptr_t(metadata.szind), ref[0]);
                CHECK_EQ(uintptr_t(metadata.state), ref[1]);
                CHECK_EQ(uintptr_t(metadata.is_head), ref[2]);
                CHECK_EQ(uintptr_t(metadata.slab), ref[3]);
            }
        }
        else if (op < 84)
        {
            /// clear an element with non-null edata (pick a random tracked page, then the next non-null one)
            if (nonnull.empty())
                continue;
            auto it = nonnull.lower_bound(random_key_in(random_existing_leaf()));
            while (it != nonnull.end() && !it->second)
                ++it;
            if (it == nonnull.end())
                continue;
            uintptr_t key = it->first;
            ref_clear(key);
            tree.clear(nullptr, &ctx, key);
            it->second = false;
        }
        else if (op < 92)
        {
            /// write a range within one existing leaf, then maybe clear a subrange of it
            uintptr_t leaf_base = random_existing_leaf();
            uintptr_t first = rng.below(pages_per_leaf);
            uintptr_t count = 1 + rng.below(minOf<uintptr_t>(64, pages_per_leaf - first));
            uintptr_t range_base = leaf_base + first * PAGE;
            uintptr_t range_end = range_base + (count - 1) * PAGE;
            uintptr_t edata = randomEdata(rng) | EDATA_ALIGNMENT; /// Non-null.
            unsigned szind = unsigned(rng.below(SC_NSIZES));
            unsigned state = unsigned(rng.below(extent_state_max + 1));
            bool slab = rng.below(2);
            ref_write_range(range_base, range_end, edata, szind, state, false, slab);
            tree.writeRange(nullptr, &ctx, range_base, range_end, makeContents(edata, szind, state, false, slab));
            for (uintptr_t page = range_base; page <= range_end; page += PAGE)
                nonnull[page] = true;
            if (rng.below(2))
            {
                uintptr_t clear_first = rng.below(count);
                uintptr_t clear_count = 1 + rng.below(count - clear_first);
                uintptr_t clear_base = range_base + clear_first * PAGE;
                uintptr_t clear_end = clear_base + (clear_count - 1) * PAGE;
                ref_clear_range(clear_base, clear_end);
                tree.clearRange(nullptr, &ctx, clear_base, clear_end);
                for (uintptr_t page = clear_base; page <= clear_end; page += PAGE)
                    nonnull[page] = false;
            }
        }
        else
        {
            /// state update of one or two elements in existing leaves
            uintptr_t key1 = random_key_in(random_existing_leaf());
            uintptr_t key2 = rng.below(3) == 0 ? 0 : random_key_in(random_existing_leaf());
            unsigned state = unsigned(rng.below(extent_state_max + 1));
            ref_state_update(key1, key2, state);
            RadixTreeLeafElm * elm1 = tree.leafElmLookup(nullptr, &ctx, key1, true, false);
            RadixTreeLeafElm * elm2 = key2 == 0 ? nullptr : tree.leafElmLookup(nullptr, &ctx, key2, true, false);
            RadixTree::leafElmStateUpdate(nullptr, elm1, elm2, ExtentState(state));
            /// The compact encoding copies the whole word of `elm1` (including edata) to `elm2`; the non-compact one
            /// (LG_VADDR 64) copies only the metadata word.
            if (key2 != 0 && RTREE_LEAF_COMPACT)
                nonnull[pageFloor(key2)] = nonnull[pageFloor(key1)];
        }

        compareCache();
    }

    CHECK_EQ(mismatches, 0);
    CHECK_GE(existing_leaves.size(), size_t(40));

    /// Finally, every page touched has the same contents in both trees.
    for (uintptr_t leaf_base : existing_leaves)
    {
        for (int i = 0; i < 64; ++i)
        {
            uintptr_t key = random_key_in(leaf_base);
            uintptr_t ref[5];
            ref_read(key, ref);
            compareContents(ref, tree.read(nullptr, &ctx, key));
        }
    }
    for (auto [page, is_nonnull] : nonnull)
    {
        uintptr_t ref[5];
        ref_read(page, ref);
        compareContents(ref, tree.read(nullptr, &ctx, page));
        CHECK_EQ(ref[0] != 0, is_nonnull);
    }
    compareCache();
    CHECK_EQ(mismatches, 0);
}

TEST(RadixTree, FallbackContext)
{
    RadixTreeContext fallback;
    fallback.cache[3].leafkey = 12345;
    /// The null tsdn path initializes the fallback (the ThreadState accessor is tested in extent_map).
    fallback.init();
    for (const auto & elm : fallback.cache)
    {
        CHECK_EQ(elm.leafkey, RTREE_LEAFKEY_INVALID);
        CHECK(elm.leaf == nullptr);
    }
    for (const auto & elm : fallback.l2_cache)
    {
        CHECK_EQ(elm.leafkey, RTREE_LEAFKEY_INVALID);
        CHECK(elm.leaf == nullptr);
    }
    constexpr RadixTreeContext constant;
    static_assert(constant.cache[15].leafkey == RTREE_LEAFKEY_INVALID);
    static_assert(constant.l2_cache[7].leafkey == RTREE_LEAFKEY_INVALID);
}
