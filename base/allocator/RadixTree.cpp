#include <allocator/RadixTree.h>

#include <allocator/Base.h>

namespace jemalloc
{

void RadixTreeContext::init()
{
    for (unsigned i = 0; i < RADIX_TREE_CONTEXT_NUM_CACHE; ++i)
    {
        RadixTreeCacheElement * element = &cache[i];
        element->leaf_key = RADIX_TREE_LEAF_KEY_INVALID;
        element->leaf = nullptr;
    }
    for (unsigned i = 0; i < RADIX_TREE_CONTEXT_NUM_CACHE_L2; ++i)
    {
        RadixTreeCacheElement * element = &l2_cache[i];
        element->leaf_key = RADIX_TREE_LEAF_KEY_INVALID;
        element->leaf = nullptr;
    }
}

bool RadixTree::init(Base * base_, [[maybe_unused]] bool zeroed)
{
    ALLOCATOR_ASSERT(zeroed);
    base = base_;

    if (init_lock.init("rtree", MutexRank::RADIX_TREE, MutexLockOrder::RankExclusive))
        return true;

    return false;
}

RadixTreeNodeElement * RadixTree::nodeAlloc(ThreadState * thread_state, size_t num_elements)
{
    return static_cast<RadixTreeNodeElement *>(base->allocRadixTree(thread_state, num_elements * sizeof(RadixTreeNodeElement)));
}

RadixTreeLeafElement * RadixTree::leafAlloc(ThreadState * thread_state, size_t num_elements)
{
    return static_cast<RadixTreeLeafElement *>(base->allocRadixTree(thread_state, num_elements * sizeof(RadixTreeLeafElement)));
}

RadixTreeNodeElement * RadixTree::nodeInit(ThreadState * thread_state, unsigned level, void ** element_ptr)
{
    init_lock.lock(thread_state);
    /// If `*element_ptr` is non-null, then it was initialized with the init lock held, so we can get by with 'relaxed' here.
    auto * node = static_cast<RadixTreeNodeElement *>(std::atomic_ref<void *>(*element_ptr).load(std::memory_order_relaxed));
    if (node == nullptr)
    {
        node = nodeAlloc(thread_state, size_t(1) << radix_tree_levels[level].bits);
        if (node == nullptr)
        {
            init_lock.unlock(thread_state);
            return nullptr;
        }
        /// Even though we hold the lock, a later reader might not; we need release semantics.
        std::atomic_ref<void *>(*element_ptr).store(node, std::memory_order_release);
    }
    init_lock.unlock(thread_state);

    return node;
}

RadixTreeLeafElement * RadixTree::leafInit(ThreadState * thread_state, void ** element_ptr)
{
    init_lock.lock(thread_state);
    /// If `*element_ptr` is non-null, then it was initialized with the init lock held, so we can get by with 'relaxed' here.
    auto * leaf = static_cast<RadixTreeLeafElement *>(std::atomic_ref<void *>(*element_ptr).load(std::memory_order_relaxed));
    if (leaf == nullptr)
    {
        leaf = leafAlloc(thread_state, size_t(1) << radix_tree_levels[RADIX_TREE_HEIGHT - 1].bits);
        if (leaf == nullptr)
        {
            init_lock.unlock(thread_state);
            return nullptr;
        }
        /// Even though we hold the lock, a later reader might not; we need release semantics.
        std::atomic_ref<void *>(*element_ptr).store(leaf, std::memory_order_release);
    }
    init_lock.unlock(thread_state);

    return leaf;
}

namespace
{

/// jemalloc: rtree_node_valid
bool nodeValid(RadixTreeNodeElement * node)
{
    return node != nullptr;
}

/// jemalloc: rtree_leaf_valid
bool leafValid(RadixTreeLeafElement * leaf)
{
    return leaf != nullptr;
}

/// jemalloc: rtree_child_node_tryread
ALLOCATOR_ALWAYS_INLINE RadixTreeNodeElement * childNodeTryRead(RadixTreeNodeElement * element, bool dependent)
{
    RadixTreeNodeElement * node;
    if (dependent)
        node = static_cast<RadixTreeNodeElement *>(std::atomic_ref<void *>(element->child).load(std::memory_order_relaxed));
    else
        node = static_cast<RadixTreeNodeElement *>(std::atomic_ref<void *>(element->child).load(std::memory_order_acquire));

    ALLOCATOR_ASSERT(!dependent || node != nullptr);
    return node;
}

/// jemalloc: rtree_child_leaf_tryread
ALLOCATOR_ALWAYS_INLINE RadixTreeLeafElement * childLeafTryRead(RadixTreeNodeElement * element, bool dependent)
{
    RadixTreeLeafElement * leaf;
    if (dependent)
        leaf = static_cast<RadixTreeLeafElement *>(std::atomic_ref<void *>(element->child).load(std::memory_order_relaxed));
    else
        leaf = static_cast<RadixTreeLeafElement *>(std::atomic_ref<void *>(element->child).load(std::memory_order_acquire));

    ALLOCATOR_ASSERT(!dependent || leaf != nullptr);
    return leaf;
}

}

RadixTreeNodeElement * RadixTree::childNodeRead(ThreadState * thread_state, RadixTreeNodeElement * element, unsigned level, bool dependent)
{
    RadixTreeNodeElement * node = childNodeTryRead(element, dependent);
    if (!dependent && ALLOCATOR_UNLIKELY(!nodeValid(node)))
        node = nodeInit(thread_state, level + 1, &element->child);
    ALLOCATOR_ASSERT(!dependent || node != nullptr);
    return node;
}

RadixTreeLeafElement *
RadixTree::childLeafRead(ThreadState * thread_state, RadixTreeNodeElement * element, unsigned /*level*/, bool dependent)
{
    RadixTreeLeafElement * leaf = childLeafTryRead(element, dependent);
    if (!dependent && ALLOCATOR_UNLIKELY(!leafValid(leaf)))
        leaf = leafInit(thread_state, &element->child);
    ALLOCATOR_ASSERT(!dependent || leaf != nullptr);
    return leaf;
}

RadixTreeLeafElement * RadixTree::leafElementLookupHard(
    ThreadState * thread_state, RadixTreeContext * radix_tree_context, uintptr_t key, bool dependent, bool init_missing)
{
    RadixTreeNodeElement * node = root;
    RadixTreeLeafElement * leaf = nullptr;

    if constexpr (config::debug)
    {
        uintptr_t leaf_key = radixTreeLeafKey(key);
        for (unsigned i = 0; i < RADIX_TREE_CONTEXT_NUM_CACHE; ++i)
            ALLOCATOR_ASSERT(radix_tree_context->cache[i].leaf_key != leaf_key);
        for (unsigned i = 0; i < RADIX_TREE_CONTEXT_NUM_CACHE_L2; ++i)
            ALLOCATOR_ASSERT(radix_tree_context->l2_cache[i].leaf_key != leaf_key);
    }

    /// jemalloc: RTREE_GET_CHILD(level). Returns false if the lookup fails.
    auto get_child = [&](unsigned level) __attribute__((always_inline)) -> bool
    {
        ALLOCATOR_ASSERT(level < RADIX_TREE_HEIGHT - 1);
        if (level != 0 && !dependent && ALLOCATOR_UNLIKELY(!nodeValid(node)))
            return false;
        uintptr_t subkey = radixTreeSubkey(key, level);
        if (level + 2 < RADIX_TREE_HEIGHT)
            node = init_missing ? childNodeRead(thread_state, &node[subkey], level, dependent) : childNodeTryRead(&node[subkey], dependent);
        else
            leaf = init_missing ? childLeafRead(thread_state, &node[subkey], level, dependent) : childLeafTryRead(&node[subkey], dependent);
        return true;
    };

    if constexpr (RADIX_TREE_HEIGHT > 1)
    {
        if (!get_child(0))
            return nullptr;
    }
    if constexpr (RADIX_TREE_HEIGHT > 2)
    {
        if (!get_child(1))
            return nullptr;
    }

    /// jemalloc: RTREE_GET_LEAF(RTREE_HEIGHT - 1).
    /// Cache replacement upon hard lookup (i.e. L1 & L2 rtree cache miss): (1) evict the last entry in the L2 cache;
    /// (2) move the collision slot from the L1 cache down to L2; and (3) fill L1.
    constexpr unsigned level = RADIX_TREE_HEIGHT - 1;
    if (!dependent && ALLOCATOR_UNLIKELY(!leafValid(leaf)))
        return nullptr;
    if constexpr (RADIX_TREE_CONTEXT_NUM_CACHE_L2 > 1)
        memmove(
            &radix_tree_context->l2_cache[1],
            &radix_tree_context->l2_cache[0],
            sizeof(RadixTreeCacheElement) * (RADIX_TREE_CONTEXT_NUM_CACHE_L2 - 1));
    size_t slot = radixTreeCacheDirectMap(key);
    radix_tree_context->l2_cache[0].leaf_key = radix_tree_context->cache[slot].leaf_key;
    radix_tree_context->l2_cache[0].leaf = radix_tree_context->cache[slot].leaf;
    uintptr_t leaf_key = radixTreeLeafKey(key);
    radix_tree_context->cache[slot].leaf_key = leaf_key;
    radix_tree_context->cache[slot].leaf = leaf;
    uintptr_t subkey = radixTreeSubkey(key, level);
    return &leaf[subkey];
}

}
