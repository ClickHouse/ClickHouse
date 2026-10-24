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

extern "C" {
size_t ref_constant(int which);
void ref_level(unsigned level, unsigned * bits, unsigned * cumulative_bits);
uintptr_t ref_leaf_key(uintptr_t key);
uintptr_t ref_subkey(uintptr_t key, unsigned level);
size_t ref_direct_map(uintptr_t key);
void ref_encode(uintptr_t extent, unsigned size_class_idx, unsigned state, int is_head, int slab, uintptr_t * out);
void ref_decode(uintptr_t bits, uintptr_t * out);
int ref_init();
void ref_context_get(uintptr_t * leaf_keys, uintptr_t * leaves);
uintptr_t ref_lookup(uintptr_t key, int dependent, int init_missing);
int ref_write(uintptr_t key, uintptr_t extent, unsigned size_class_idx, unsigned state, int is_head, int slab);
void ref_read(uintptr_t key, uintptr_t * out);
int ref_read_independent(uintptr_t key, uintptr_t * out);
int ref_metadata_try_read_fast(uintptr_t key, uintptr_t * out);
void ref_clear(uintptr_t key);
void ref_write_range(uintptr_t base, uintptr_t end, uintptr_t extent, unsigned size_class_idx, unsigned state, int is_head, int slab);
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

RadixTreeContents makeContents(uintptr_t extent, unsigned size_class_idx, unsigned state, bool is_head, bool slab)
{
    return {reinterpret_cast<Extent *>(extent), {size_class_idx, ExtentState(state), is_head, slab}};
}

void splitContents(const RadixTreeContents & contents, uintptr_t * out)
{
    out[0] = reinterpret_cast<uintptr_t>(contents.extent);
    out[1] = contents.metadata.size_class_idx;
    out[2] = contents.metadata.state;
    out[3] = contents.metadata.is_head;
    out[4] = contents.metadata.slab;
}

/// A random 128-aligned fake edata address in the user half of the address space (or null).
uintptr_t randomExtent(Rng & rng)
{
    if (rng.below(8) == 0)
        return 0;
    return (rng.next() & ((uintptr_t(1) << (LOG2_VIRTUAL_ADDRESS - 1)) - 1)) & ~uintptr_t(EXTENT_ALIGNMENT - 1);
}

}

TEST(RadixTree, Geometry)
{
    CHECK_EQ(size_t(RADIX_TREE_NUM_HIGH_INSIGNIFICANT_BITS), ref_constant(0));
    CHECK_EQ(size_t(RADIX_TREE_NUM_LOW_INSIGNIFICANT_BITS), ref_constant(1));
    CHECK_EQ(size_t(RADIX_TREE_NUM_SIGNIFICANT_BITS), ref_constant(2));
    CHECK_EQ(size_t(RADIX_TREE_HEIGHT), ref_constant(3));
    CHECK_EQ(size_t(RADIX_TREE_LEAF_COMPACT), ref_constant(4));
    CHECK_EQ(size_t(radixTreeLeafMaskBits()), ref_constant(5));
    CHECK_EQ(sizeof(RadixTreeContext), ref_constant(6));
    CHECK_EQ(sizeof(RadixTree), ref_constant(7));
    CHECK_EQ(sizeof(RadixTreeLeafElement), ref_constant(8));
    CHECK_EQ(sizeof(RadixTreeNodeElement), ref_constant(9));
    CHECK_EQ(size_t(RADIX_TREE_CONTEXT_NUM_CACHE), ref_constant(10));
    CHECK_EQ(size_t(RADIX_TREE_CONTEXT_NUM_CACHE_L2), ref_constant(11));
    CHECK_EQ(offsetof(RadixTree, root), ref_constant(12));
    CHECK_EQ(RadixTree::root_size, ref_constant(13));
    for (unsigned level = 0; level < RADIX_TREE_HEIGHT; ++level)
    {
        unsigned bits;
        unsigned cumulative_bits;
        ref_level(level, &bits, &cumulative_bits);
        CHECK_EQ(radix_tree_levels[level].bits, bits);
        CHECK_EQ(radix_tree_levels[level].cumulative_bits, cumulative_bits);
    }
}

TEST(RadixTree, ExtentLayout)
{
    CHECK_EQ(sizeof(Extent), ref_constant(20));
    CHECK_EQ(offsetof(Extent, address), ref_constant(21));
    CHECK_EQ(offsetof(Extent, size_and_serial_number), ref_constant(22));
    CHECK_EQ(offsetof(Extent, unused_page_slab), ref_constant(23));
    CHECK_EQ(offsetof(Extent, serial_number), ref_constant(24));
    CHECK_EQ(offsetof(Extent, list_link_active), ref_constant(25));
    CHECK_EQ(offsetof(Extent, heap_link), ref_constant(26));
    CHECK_EQ(offsetof(Extent, list_link_inactive), ref_constant(27));
    CHECK_EQ(offsetof(Extent, slab_data), ref_constant(28));
    CHECK_EQ(offsetof(Extent, profiling_info), ref_constant(29));
    CHECK_EQ(sizeof(SlabData), ref_constant(30));
    CHECK_EQ(sizeof(ExtentProfilingInfo), ref_constant(31));
    CHECK_EQ(offsetof(ExtentProfilingInfo, fragmentation_link), ref_constant(32));
    CHECK_EQ(offsetof(ExtentProfilingInfo, fragmentation_tracked), ref_constant(33));
    CHECK_EQ(EXTENT_ALIGNMENT, ref_constant(34));
    CHECK_EQ(size_t(EXTENT_SET_ENUMERATE_MAX_NUM), ref_constant(35));

    const ExtentBitField fields[]
        = {extent_bits::arena,
           extent_bits::slab,
           extent_bits::committed,
           extent_bits::allocator_kind,
           extent_bits::zeroed,
           extent_bits::guarded,
           extent_bits::state,
           extent_bits::size_class_idx,
           extent_bits::num_free,
           extent_bits::bin_shard,
           extent_bits::is_head};
    for (int i = 0; i < 11; ++i)
        CHECK_EQ(size_t(fields[i].shift), ref_constant(40 + i));
    CHECK_EQ(size_t(extent_bits::size_class_idx.width), ref_constant(51));
    CHECK_EQ(size_t(extent_bits::num_free.width), ref_constant(52));
}

TEST(RadixTree, KeyFunctions)
{
    Rng rng;
    for (int i = 0; i < 200000; ++i)
    {
        uintptr_t key = rng.next();
        if (i % 2)
            key &= (uintptr_t(1) << LOG2_VIRTUAL_ADDRESS) - 1;
        if (key == 0)
            continue;
        CHECK_EQ(radixTreeLeafKey(key), ref_leaf_key(key));
        CHECK_EQ(radixTreeCacheDirectMap(key), ref_direct_map(key));
        for (unsigned level = 0; level < RADIX_TREE_HEIGHT; ++level)
            CHECK_EQ(radixTreeSubkey(key, level), ref_subkey(key, level));
    }
}

TEST(RadixTree, Encoding)
{
    Rng rng;
    for (int i = 0; i < 200000; ++i)
    {
        uintptr_t extent = randomExtent(rng);
        if (i % 3 == 0 && extent != 0)
            extent |= ~((uintptr_t(1) << (LOG2_VIRTUAL_ADDRESS - 1)) - 1); /// Kernel half: tests sign/zero extension.
        unsigned size_class_idx = unsigned(rng.below(SIZE_CLASS_NUM_SIZES + 1));
        unsigned state = unsigned(rng.below(extent_state_max + 1));
        bool is_head = rng.below(2);
        bool slab = rng.below(2);

        uintptr_t ref[5];
        ref_encode(extent, size_class_idx, state, is_head, slab, ref);
        RadixTreeEncoded encoded = radixTreeContentsEncode(makeContents(extent, size_class_idx, state, is_head, slab));
        CHECK_EQ(encoded.bits, ref[0]);
        CHECK_EQ(uintptr_t(encoded.additional), ref[1]);

        if constexpr (RADIX_TREE_LEAF_COMPACT)
        {
            uintptr_t decoded_ref[5];
            uintptr_t decoded[5];
            ref_decode(encoded.bits, decoded_ref);
            splitContents(radixTreeLeafElementBitsDecode(encoded.bits), decoded);
            for (int j = 0; j < 5; ++j)
                CHECK_EQ(decoded[j], decoded_ref[j]);
        }
    }

    CHECK_EQ(radix_tree_contents_cleared.metadata.size_class_idx, SIZE_CLASS_NUM_SIZES);
}

namespace
{

constinit RadixTree tree;
RadixTreeContext context;

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
    uintptr_t ref_leaf_keys[RADIX_TREE_CONTEXT_NUM_CACHE + RADIX_TREE_CONTEXT_NUM_CACHE_L2];
    uintptr_t ref_leaves[RADIX_TREE_CONTEXT_NUM_CACHE + RADIX_TREE_CONTEXT_NUM_CACHE_L2];
    ref_context_get(ref_leaf_keys, ref_leaves);
    for (unsigned i = 0; i < RADIX_TREE_CONTEXT_NUM_CACHE + RADIX_TREE_CONTEXT_NUM_CACHE_L2; ++i)
    {
        const RadixTreeCacheElement & element
            = i < RADIX_TREE_CONTEXT_NUM_CACHE ? context.cache[i] : context.l2_cache[i - RADIX_TREE_CONTEXT_NUM_CACHE];
        if (element.leaf_key != ref_leaf_keys[i])
        {
            CHECK_EQ(element.leaf_key, ref_leaf_keys[i]);
            ++mismatches;
        }
        checkLeaf(ref_leaves[i], reinterpret_cast<uintptr_t>(element.leaf));
    }
}

/// The element pointer, as its leaf and index.
void compareElement(uintptr_t key, uintptr_t ref_element, RadixTreeLeafElement * element)
{
    uintptr_t offset = radixTreeSubkey(key, RADIX_TREE_HEIGHT - 1) * sizeof(RadixTreeLeafElement);
    if ((ref_element == 0) != (element == nullptr))
    {
        CHECK_EQ(ref_element == 0, element == nullptr);
        ++mismatches;
        return;
    }
    if (ref_element != 0)
        checkLeaf(ref_element - offset, reinterpret_cast<uintptr_t>(element) - offset);
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
    Base * base = Base::create(nullptr, 0, &extent_hooks_default_extent_hooks, true);
    REQUIRE(base != nullptr);
    REQUIRE(!tree.init(base, true));
    context.init();
    compareCache();

    /// A pool of leaves: many share an L1 slot, so that both cache levels are exercised.
    const unsigned mask_bits = radixTreeLeafMaskBits();
    const uintptr_t leaf_span = uintptr_t(1) << mask_bits;
    const uintptr_t pages_per_leaf = leaf_span >> LOG2_PAGE;
    std::vector<uintptr_t> leaf_bases;
    for (uintptr_t i = 1; i <= 24; ++i)
        leaf_bases.push_back(i * leaf_span);
    for (uintptr_t i = 1; i <= 24; ++i)
        leaf_bases.push_back((i * RADIX_TREE_CONTEXT_NUM_CACHE + 5) * leaf_span);
    for (uintptr_t i = 1; i <= 8; ++i)
        leaf_bases.push_back(((uintptr_t(1) << (LOG2_VIRTUAL_ADDRESS - mask_bits)) - i) * leaf_span);

    std::set<uintptr_t> existing_leaves; /// Leaf bases that were created by a write.
    std::map<uintptr_t, bool> non_null; /// Page -> the element has a non-null edata.

    Rng rng;
    auto random_key_in
        = [&](uintptr_t leaf_base) { return leaf_base + rng.below(pages_per_leaf) * PAGE + (rng.below(4) == 0 ? rng.below(PAGE) : 0); };
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
            uintptr_t extent = randomExtent(rng);
            unsigned size_class_idx = unsigned(rng.below(SIZE_CLASS_NUM_SIZES + 1));
            unsigned state = unsigned(rng.below(extent_state_max + 1));
            bool is_head = rng.below(2);
            bool slab = rng.below(2);
            int ref_error = ref_write(key, extent, size_class_idx, state, is_head, slab);
            bool error = tree.write(nullptr, &context, key, makeContents(extent, size_class_idx, state, is_head, slab));
            CHECK_EQ(bool(ref_error), error);
            existing_leaves.insert(leaf_base);
            non_null[pageFloor(key)] = extent != 0;
        }
        else if (op < 45)
        {
            /// dependent read in an existing leaf
            uintptr_t key = random_key_in(random_existing_leaf());
            uintptr_t ref[5];
            ref_read(key, ref);
            compareContents(ref, tree.read(nullptr, &context, key));
        }
        else if (op < 60)
        {
            /// independent read anywhere (also in leaves that don't exist)
            uintptr_t key = random_key_in(leaf_bases[rng.below(leaf_bases.size())]);
            uintptr_t ref[5];
            RadixTreeContents contents;
            int ref_error = ref_read_independent(key, ref);
            bool error = tree.readIndependent(nullptr, &context, key, &contents);
            CHECK_EQ(bool(ref_error), error);
            if (!ref_error && !error)
                compareContents(ref, contents);
        }
        else if (op < 70)
        {
            /// lookup with various flags (never dependent + init_missing)
            uintptr_t key = random_key_in(leaf_bases[rng.below(leaf_bases.size())]);
            bool dependent = false;
            bool init_missing = rng.below(2);
            uintptr_t ref_element = ref_lookup(key, dependent, init_missing);
            RadixTreeLeafElement * element = tree.leafElementLookup(nullptr, &context, key, dependent, init_missing);
            compareElement(key, ref_element, element);
            if (init_missing)
                existing_leaves.insert(radixTreeLeafKey(key));
        }
        else if (op < 78)
        {
            /// fast metadata read (L1 only)
            uintptr_t key = random_key_in(leaf_bases[rng.below(leaf_bases.size())]);
            uintptr_t ref[4];
            RadixTreeMetadata metadata;
            int ref_miss = ref_metadata_try_read_fast(key, ref);
            bool miss = tree.metadataTryReadFast(nullptr, &context, key, &metadata);
            CHECK_EQ(bool(ref_miss), miss);
            if (!ref_miss && !miss)
            {
                CHECK_EQ(uintptr_t(metadata.size_class_idx), ref[0]);
                CHECK_EQ(uintptr_t(metadata.state), ref[1]);
                CHECK_EQ(uintptr_t(metadata.is_head), ref[2]);
                CHECK_EQ(uintptr_t(metadata.slab), ref[3]);
            }
        }
        else if (op < 84)
        {
            /// clear an element with non-null edata (pick a random tracked page, then the next non-null one)
            if (non_null.empty())
                continue;
            auto it = non_null.lower_bound(random_key_in(random_existing_leaf()));
            while (it != non_null.end() && !it->second)
                ++it;
            if (it == non_null.end())
                continue;
            uintptr_t key = it->first;
            ref_clear(key);
            tree.clear(nullptr, &context, key);
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
            uintptr_t extent = randomExtent(rng) | EXTENT_ALIGNMENT; /// Non-null.
            unsigned size_class_idx = unsigned(rng.below(SIZE_CLASS_NUM_SIZES));
            unsigned state = unsigned(rng.below(extent_state_max + 1));
            bool slab = rng.below(2);
            ref_write_range(range_base, range_end, extent, size_class_idx, state, false, slab);
            tree.writeRange(nullptr, &context, range_base, range_end, makeContents(extent, size_class_idx, state, false, slab));
            for (uintptr_t page = range_base; page <= range_end; page += PAGE)
                non_null[page] = true;
            if (rng.below(2))
            {
                uintptr_t clear_first = rng.below(count);
                uintptr_t clear_count = 1 + rng.below(count - clear_first);
                uintptr_t clear_base = range_base + clear_first * PAGE;
                uintptr_t clear_end = clear_base + (clear_count - 1) * PAGE;
                ref_clear_range(clear_base, clear_end);
                tree.clearRange(nullptr, &context, clear_base, clear_end);
                for (uintptr_t page = clear_base; page <= clear_end; page += PAGE)
                    non_null[page] = false;
            }
        }
        else
        {
            /// state update of one or two elements in existing leaves
            uintptr_t key1 = random_key_in(random_existing_leaf());
            uintptr_t key2 = rng.below(3) == 0 ? 0 : random_key_in(random_existing_leaf());
            unsigned state = unsigned(rng.below(extent_state_max + 1));
            ref_state_update(key1, key2, state);
            RadixTreeLeafElement * element1 = tree.leafElementLookup(nullptr, &context, key1, true, false);
            RadixTreeLeafElement * element2 = key2 == 0 ? nullptr : tree.leafElementLookup(nullptr, &context, key2, true, false);
            RadixTree::leafElementStateUpdate(nullptr, element1, element2, ExtentState(state));
            /// The compact encoding copies the whole word of `element1` (including edata) to `element2`; the non-compact one
            /// (LOG2_VIRTUAL_ADDRESS 64) copies only the metadata word.
            if (key2 != 0 && RADIX_TREE_LEAF_COMPACT)
                non_null[pageFloor(key2)] = non_null[pageFloor(key1)];
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
            compareContents(ref, tree.read(nullptr, &context, key));
        }
    }
    for (auto [page, is_non_null] : non_null)
    {
        uintptr_t ref[5];
        ref_read(page, ref);
        compareContents(ref, tree.read(nullptr, &context, page));
        CHECK_EQ(ref[0] != 0, is_non_null);
    }
    compareCache();
    CHECK_EQ(mismatches, 0);
}

TEST(RadixTree, FallbackContext)
{
    RadixTreeContext fallback;
    fallback.cache[3].leaf_key = 12345;
    /// The null tsdn path initializes the fallback (the ThreadState accessor is tested in extent_map).
    fallback.init();
    for (const auto & element : fallback.cache)
    {
        CHECK_EQ(element.leaf_key, RADIX_TREE_LEAF_KEY_INVALID);
        CHECK(element.leaf == nullptr);
    }
    for (const auto & element : fallback.l2_cache)
    {
        CHECK_EQ(element.leaf_key, RADIX_TREE_LEAF_KEY_INVALID);
        CHECK(element.leaf == nullptr);
    }
    constexpr RadixTreeContext constant;
    static_assert(constant.cache[15].leaf_key == RADIX_TREE_LEAF_KEY_INVALID);
    static_assert(constant.l2_cache[7].leaf_key == RADIX_TREE_LEAF_KEY_INVALID);
}
