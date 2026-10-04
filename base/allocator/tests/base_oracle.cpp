/// Compares `Base` with jemalloc's `base.c` (linked from the reference `lib_jemalloc.a`): for a long scripted sequence
/// of `base_alloc` / `base_alloc_edata` / `base_alloc_rtree` calls (and tcache stack allocations from `b0`), every
/// returned address must be at the same offset in the same block (counted from the first block), the `esn` of every
/// `Extent` must be the same, and the block sizes and all stats must be identical after every call.

#include <allocator/Base.h>
#include <allocator/Pages.h>

#include "Test.h"

#include <cstring>
#include <random>
#include <vector>

extern "C"
{
int ref_boot(void);
size_t ref_sizeof_base(void);
size_t ref_sizeof_base_block(void);
void * ref_base_new(unsigned ind);
void ref_base_delete(void * base);
void * ref_base_alloc(void * base, size_t size, size_t alignment);
void * ref_base_alloc_edata(void * base, size_t * esn);
void * ref_base_alloc_rtree(void * base, size_t size);
void ref_base_stats(void * base, size_t * out);
int ref_base_blocks(void * base, uintptr_t * addrs, size_t * sizes, int max);
int ref_base_boot(void);
void * ref_b0(void);
void * ref_b0_alloc_tcache_stack(size_t size);
void ref_b0_dalloc_tcache_stack(void * p);

/// `base.o` pulls in the rest of the reference jemalloc, including the libunwind-based profiler backtrace, which is
/// never called here.
int unw_backtrace(void **, int)
{
    return 0;
}
}

using namespace jemalloc;

namespace
{

constexpr int max_blocks = 256;

struct Blocks
{
    int n = 0;
    uintptr_t addrs[max_blocks];
    size_t sizes[max_blocks];

    /// (index of the block counted from the oldest, offset in the block), or (-1, 0).
    std::pair<int, size_t> locate(const void * p) const
    {
        uintptr_t a = reinterpret_cast<uintptr_t>(p);
        for (int i = 0; i < n; ++i)
            if (a >= addrs[i] && a < addrs[i] + sizes[i])
                return {n - 1 - i, a - addrs[i]};
        return {-1, 0};
    }
};

Blocks refBlocks(void * base)
{
    Blocks b;
    b.n = ref_base_blocks(base, b.addrs, b.sizes, max_blocks);
    return b;
}

Blocks ourBlocks(const Base * base)
{
    Blocks b;
    for (const BaseBlock * block = base->blocksList(); block != nullptr && b.n < max_blocks; block = block->next, ++b.n)
    {
        b.addrs[b.n] = reinterpret_cast<uintptr_t>(block);
        b.sizes[b.n] = block->size;
    }
    return b;
}

void bootOnce()
{
    static bool booted = false;
    if (!booted)
    {
        REQUIRE(ref_boot() == 0);
        REQUIRE(!pages::boot());
        booted = true;
    }
}

/// Compares stats and the block list; returns false on mismatch.
bool compareState(void * ref, Base * our, int step)
{
    size_t ref_stats[6];
    size_t our_stats[6];
    ref_base_stats(ref, ref_stats);
    our->statsGet(nullptr, &our_stats[0], &our_stats[1], &our_stats[2], &our_stats[3], &our_stats[4], &our_stats[5]);
    bool ok = true;
    for (int i = 0; i < 6; ++i)
    {
        if (ref_stats[i] != our_stats[i])
        {
            std::fprintf(stderr, "step %d: stat %d: %zu vs %zu\n", step, i, ref_stats[i], our_stats[i]);
            ok = false;
        }
    }
    Blocks rb = refBlocks(ref);
    Blocks ob = ourBlocks(our);
    if (rb.n != ob.n)
    {
        std::fprintf(stderr, "step %d: %d blocks vs %d\n", step, rb.n, ob.n);
        ok = false;
    }
    else
    {
        for (int i = 0; i < rb.n; ++i)
            if (rb.sizes[i] != ob.sizes[i])
            {
                std::fprintf(stderr, "step %d: block %d size %zu vs %zu\n", step, i, rb.sizes[i], ob.sizes[i]);
                ok = false;
            }
    }
    return ok;
}

bool comparePointers(void * ref, void * ref_ptr, Base * our, void * our_ptr, int step)
{
    if ((ref_ptr == nullptr) != (our_ptr == nullptr))
    {
        std::fprintf(stderr, "step %d: null mismatch\n", step);
        return false;
    }
    if (ref_ptr == nullptr)
        return true;
    auto r = refBlocks(ref).locate(ref_ptr);
    auto o = ourBlocks(our).locate(our_ptr);
    if (r != o)
    {
        std::fprintf(stderr, "step %d: (block %d, offset %zu) vs (block %d, offset %zu)\n", step, r.first, r.second, o.first, o.second);
        return false;
    }
    return true;
}

size_t pickSize(std::mt19937_64 & rng)
{
    unsigned kind = rng() % 100;
    if (kind < 50)
        return 1 + rng() % 512;
    if (kind < 80)
        return 512 + rng() % 65536;
    if (kind < 97)
        return 65536 + rng() % (size_t(4) << 20);
    return (size_t(4) << 20) + rng() % (size_t(36) << 20);
}

size_t pickAlignment(std::mt19937_64 & rng)
{
    static const size_t alignments[] = {1, 8, 16, 32, 64, 128, 256, 4096, PAGE, 65536};
    if (rng() % 200 == 0)
        return size_t(2) << 20;
    return alignments[rng() % (sizeof(alignments) / sizeof(alignments[0]))];
}

}

TEST(BaseOracle, Layout)
{
    CHECK_EQ(ref_sizeof_base(), sizeof(Base));
    CHECK_EQ(ref_sizeof_base_block(), sizeof(BaseBlock));
}

TEST(BaseOracle, ScriptedSequence)
{
    bootOnce();
    for (unsigned seed = 0; seed < 4; ++seed)
    {
        unsigned ind = seed + 1;
        void * ref = ref_base_new(ind);
        Base * our = Base::create(nullptr, ind, &ehooks_default_extent_hooks, true);
        REQUIRE(ref != nullptr && our != nullptr);
        CHECK_EQ(our->indGet(), ind);
        REQUIRE(compareState(ref, our, -1));
        /// The `Base` itself is at the same offset of the first block.
        REQUIRE(comparePointers(ref, ref, our, our, -1));

        std::mt19937_64 rng(seed);
        int failures = 0;
        for (int step = 0; step < 3000 && failures < 5; ++step)
        {
            unsigned op = rng() % 100;
            bool ok = true;
            if (op < 45)
            {
                size_t size = pickSize(rng);
                size_t alignment = pickAlignment(rng);
                void * r = ref_base_alloc(ref, size, alignment);
                void * o = our->alloc(nullptr, size, alignment);
                ok = comparePointers(ref, r, our, o, step);
                if (o)
                    ok &= reinterpret_cast<uintptr_t>(o) % alignment == 0;
            }
            else if (op < 85)
            {
                size_t ref_esn = 0;
                void * r = ref_base_alloc_edata(ref, &ref_esn);
                Extent * o = our->allocExtent(nullptr);
                ok = comparePointers(ref, r, our, o, step);
                if (o)
                {
                    ok &= o->esn() == ref_esn;
                    ok &= reinterpret_cast<uintptr_t>(o) % EDATA_ALIGNMENT == 0;
                }
            }
            else
            {
                static const size_t rtree_sizes[] = {64, 192, 4096, 16384, 65536, size_t(1) << 20, 3 << 18};
                size_t size = rtree_sizes[rng() % (sizeof(rtree_sizes) / sizeof(rtree_sizes[0]))];
                void * r = ref_base_alloc_rtree(ref, size);
                void * o = our->allocRtree(nullptr, size);
                ok = comparePointers(ref, r, our, o, step);
            }
            ok &= compareState(ref, our, step);
            if (!ok)
            {
                ++failures;
                CHECK(ok);
            }
        }
        Blocks b = ourBlocks(our);
        std::fprintf(stderr, "seed %u: %d blocks, the newest is %zu bytes\n", seed, b.n, b.sizes[0]);

        ref_base_delete(ref);
        our->destroy(nullptr);
    }
}

TEST(BaseOracle, BlockSizeSeries)
{
    bootOnce();
    /// Force one new block per allocation (half of the block size) and compare the series of block sizes.
    void * ref = ref_base_new(100);
    Base * our = Base::create(nullptr, 100, &ehooks_default_extent_hooks, true);
    REQUIRE(ref != nullptr && our != nullptr);
    for (int i = 0; i < 24; ++i)
    {
        size_t size = refBlocks(ref).sizes[0];
        void * r = ref_base_alloc(ref, size, 64);
        void * o = our->alloc(nullptr, size, 64);
        CHECK(comparePointers(ref, r, our, o, i));
        CHECK(compareState(ref, our, i));
    }
    Blocks b = ourBlocks(our);
    std::fprintf(stderr, "block sizes (oldest first):");
    for (int i = b.n - 1; i >= 0; --i)
        std::fprintf(stderr, " %zu", b.sizes[i] >> 20);
    std::fprintf(stderr, " MiB\n");
    ref_base_delete(ref);
    our->destroy(nullptr);
}

TEST(BaseOracle, B0TcacheStacks)
{
    bootOnce();
    REQUIRE(ref_base_boot() == 0);
    REQUIRE(!baseBoot(nullptr));
    void * ref = ref_b0();
    Base * our = b0get();
    REQUIRE(ref != nullptr && our != nullptr);
    CHECK_EQ(our->indGet(), 0u);
    REQUIRE(compareState(ref, our, -1));

    std::mt19937_64 rng(42);
    std::vector<std::pair<void *, void *>> live;
    int failures = 0;
    for (int step = 0; step < 2000 && failures < 5; ++step)
    {
        unsigned op = rng() % 100;
        bool ok = true;
        if (op < 50 || live.empty())
        {
            static const size_t stack_sizes[] = {8 * 36, 8 * 200, 8 * 1000, 8 * 4000, 30000, 100000};
            size_t size = stack_sizes[rng() % (sizeof(stack_sizes) / sizeof(stack_sizes[0]))];
            void * r = ref_b0_alloc_tcache_stack(size);
            void * o = b0AllocTcacheStack(nullptr, size);
            ok = comparePointers(ref, r, our, o, step);
            if (r && o)
            {
                memset(o, 0xaa, size);
                memset(r, 0xaa, size);
                live.emplace_back(r, o);
            }
        }
        else if (op < 80)
        {
            size_t i = rng() % live.size();
            ref_b0_dalloc_tcache_stack(live[i].first);
            b0DallocTcacheStack(nullptr, live[i].second);
            live[i] = live.back();
            live.pop_back();
        }
        else if (op < 90)
        {
            size_t ref_esn = 0;
            void * r = ref_base_alloc_edata(ref, &ref_esn);
            Extent * o = our->allocExtent(nullptr);
            ok = comparePointers(ref, r, our, o, step) && o->esn() == ref_esn;
        }
        else
        {
            size_t size = pickSize(rng) % 100000 + 1;
            void * r = ref_base_alloc(ref, size, 16);
            void * o = our->alloc(nullptr, size, 16);
            ok = comparePointers(ref, r, our, o, step);
            if (o)
                ok &= reinterpret_cast<unsigned char *>(o)[0] == 0 && reinterpret_cast<unsigned char *>(o)[size - 1] == 0;
        }
        ok &= compareState(ref, our, step);
        if (!ok)
        {
            ++failures;
            CHECK(ok);
        }
    }
}
