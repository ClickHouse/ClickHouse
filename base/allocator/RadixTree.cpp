#include <allocator/RadixTree.h>

#include <allocator/Base.h>

namespace jemalloc
{

void RadixTreeContext::init()
{
    for (unsigned i = 0; i < RTREE_CTX_NCACHE; ++i)
    {
        RadixTreeCacheElm * elm = &cache[i];
        elm->leafkey = RTREE_LEAFKEY_INVALID;
        elm->leaf = nullptr;
    }
    for (unsigned i = 0; i < RTREE_CTX_NCACHE_L2; ++i)
    {
        RadixTreeCacheElm * elm = &l2_cache[i];
        elm->leafkey = RTREE_LEAFKEY_INVALID;
        elm->leaf = nullptr;
    }
}

bool RadixTree::init(Base * base_, [[maybe_unused]] bool zeroed)
{
    JE_ASSERT(zeroed);
    base = base_;

    if (init_lock.init("rtree", MutexRank::RTREE, MutexLockOrder::RankExclusive))
        return true;

    return false;
}

RadixTreeNodeElm * RadixTree::nodeAlloc(ThreadState * tsdn, size_t nelms)
{
    return static_cast<RadixTreeNodeElm *>(base->allocRtree(tsdn, nelms * sizeof(RadixTreeNodeElm)));
}

RadixTreeLeafElm * RadixTree::leafAlloc(ThreadState * tsdn, size_t nelms)
{
    return static_cast<RadixTreeLeafElm *>(base->allocRtree(tsdn, nelms * sizeof(RadixTreeLeafElm)));
}

RadixTreeNodeElm * RadixTree::nodeInit(ThreadState * tsdn, unsigned level, void ** elmp)
{
    init_lock.lock(tsdn);
    /// If `*elmp` is non-null, then it was initialized with the init lock held, so we can get by with 'relaxed' here.
    auto * node = static_cast<RadixTreeNodeElm *>(std::atomic_ref<void *>(*elmp).load(std::memory_order_relaxed));
    if (node == nullptr)
    {
        node = nodeAlloc(tsdn, size_t(1) << rtree_levels[level].bits);
        if (node == nullptr)
        {
            init_lock.unlock(tsdn);
            return nullptr;
        }
        /// Even though we hold the lock, a later reader might not; we need release semantics.
        std::atomic_ref<void *>(*elmp).store(node, std::memory_order_release);
    }
    init_lock.unlock(tsdn);

    return node;
}

RadixTreeLeafElm * RadixTree::leafInit(ThreadState * tsdn, void ** elmp)
{
    init_lock.lock(tsdn);
    /// If `*elmp` is non-null, then it was initialized with the init lock held, so we can get by with 'relaxed' here.
    auto * leaf = static_cast<RadixTreeLeafElm *>(std::atomic_ref<void *>(*elmp).load(std::memory_order_relaxed));
    if (leaf == nullptr)
    {
        leaf = leafAlloc(tsdn, size_t(1) << rtree_levels[RTREE_HEIGHT - 1].bits);
        if (leaf == nullptr)
        {
            init_lock.unlock(tsdn);
            return nullptr;
        }
        /// Even though we hold the lock, a later reader might not; we need release semantics.
        std::atomic_ref<void *>(*elmp).store(leaf, std::memory_order_release);
    }
    init_lock.unlock(tsdn);

    return leaf;
}

namespace
{

/// jemalloc: rtree_node_valid
bool nodeValid(RadixTreeNodeElm * node)
{
    return node != nullptr;
}

/// jemalloc: rtree_leaf_valid
bool leafValid(RadixTreeLeafElm * leaf)
{
    return leaf != nullptr;
}

/// jemalloc: rtree_child_node_tryread
JE_ALWAYS_INLINE RadixTreeNodeElm * childNodeTryRead(RadixTreeNodeElm * elm, bool dependent)
{
    RadixTreeNodeElm * node;
    if (dependent)
        node = static_cast<RadixTreeNodeElm *>(std::atomic_ref<void *>(elm->child).load(std::memory_order_relaxed));
    else
        node = static_cast<RadixTreeNodeElm *>(std::atomic_ref<void *>(elm->child).load(std::memory_order_acquire));

    JE_ASSERT(!dependent || node != nullptr);
    return node;
}

/// jemalloc: rtree_child_leaf_tryread
JE_ALWAYS_INLINE RadixTreeLeafElm * childLeafTryRead(RadixTreeNodeElm * elm, bool dependent)
{
    RadixTreeLeafElm * leaf;
    if (dependent)
        leaf = static_cast<RadixTreeLeafElm *>(std::atomic_ref<void *>(elm->child).load(std::memory_order_relaxed));
    else
        leaf = static_cast<RadixTreeLeafElm *>(std::atomic_ref<void *>(elm->child).load(std::memory_order_acquire));

    JE_ASSERT(!dependent || leaf != nullptr);
    return leaf;
}

}

RadixTreeNodeElm * RadixTree::childNodeRead(ThreadState * tsdn, RadixTreeNodeElm * elm, unsigned level, bool dependent)
{
    RadixTreeNodeElm * node = childNodeTryRead(elm, dependent);
    if (!dependent && JE_UNLIKELY(!nodeValid(node)))
        node = nodeInit(tsdn, level + 1, &elm->child);
    JE_ASSERT(!dependent || node != nullptr);
    return node;
}

RadixTreeLeafElm * RadixTree::childLeafRead(ThreadState * tsdn, RadixTreeNodeElm * elm, unsigned /*level*/, bool dependent)
{
    RadixTreeLeafElm * leaf = childLeafTryRead(elm, dependent);
    if (!dependent && JE_UNLIKELY(!leafValid(leaf)))
        leaf = leafInit(tsdn, &elm->child);
    JE_ASSERT(!dependent || leaf != nullptr);
    return leaf;
}

RadixTreeLeafElm *
RadixTree::leafElmLookupHard(ThreadState * tsdn, RadixTreeContext * rtree_ctx, uintptr_t key, bool dependent, bool init_missing)
{
    RadixTreeNodeElm * node = root;
    RadixTreeLeafElm * leaf = nullptr;

    if constexpr (config::debug)
    {
        uintptr_t leafkey = rtreeLeafkey(key);
        for (unsigned i = 0; i < RTREE_CTX_NCACHE; ++i)
            JE_ASSERT(rtree_ctx->cache[i].leafkey != leafkey);
        for (unsigned i = 0; i < RTREE_CTX_NCACHE_L2; ++i)
            JE_ASSERT(rtree_ctx->l2_cache[i].leafkey != leafkey);
    }

    /// jemalloc: RTREE_GET_CHILD(level). Returns false if the lookup fails.
    auto get_child = [&](unsigned level) __attribute__((always_inline)) -> bool
    {
        JE_ASSERT(level < RTREE_HEIGHT - 1);
        if (level != 0 && !dependent && JE_UNLIKELY(!nodeValid(node)))
            return false;
        uintptr_t subkey = rtreeSubkey(key, level);
        if (level + 2 < RTREE_HEIGHT)
            node = init_missing ? childNodeRead(tsdn, &node[subkey], level, dependent) : childNodeTryRead(&node[subkey], dependent);
        else
            leaf = init_missing ? childLeafRead(tsdn, &node[subkey], level, dependent) : childLeafTryRead(&node[subkey], dependent);
        return true;
    };

    if constexpr (RTREE_HEIGHT > 1)
    {
        if (!get_child(0))
            return nullptr;
    }
    if constexpr (RTREE_HEIGHT > 2)
    {
        if (!get_child(1))
            return nullptr;
    }

    /// jemalloc: RTREE_GET_LEAF(RTREE_HEIGHT - 1).
    /// Cache replacement upon hard lookup (i.e. L1 & L2 rtree cache miss): (1) evict the last entry in the L2 cache;
    /// (2) move the collision slot from the L1 cache down to L2; and (3) fill L1.
    constexpr unsigned level = RTREE_HEIGHT - 1;
    if (!dependent && JE_UNLIKELY(!leafValid(leaf)))
        return nullptr;
    if constexpr (RTREE_CTX_NCACHE_L2 > 1)
        memmove(&rtree_ctx->l2_cache[1], &rtree_ctx->l2_cache[0], sizeof(RadixTreeCacheElm) * (RTREE_CTX_NCACHE_L2 - 1));
    size_t slot = rtreeCacheDirectMap(key);
    rtree_ctx->l2_cache[0].leafkey = rtree_ctx->cache[slot].leafkey;
    rtree_ctx->l2_cache[0].leaf = rtree_ctx->cache[slot].leaf;
    uintptr_t leafkey = rtreeLeafkey(key);
    rtree_ctx->cache[slot].leafkey = leafkey;
    rtree_ctx->cache[slot].leaf = leaf;
    uintptr_t subkey = rtreeSubkey(key, level);
    return &leaf[subkey];
}

}
