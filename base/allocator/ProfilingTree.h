#pragma once

/// An intrusive ordered set used by the profiler for the trees of `rb.h` (`prof_tctx_tree_t`, `prof_gctx_tree_t`,
/// `prof_tdata_tree_t`).
///
/// The profiler only observes the trees through their in-order traversal (`*_tree_iter`, `*_tree_first`,
/// `*_tree_next`) with unique keys, so any balanced binary search tree with the same comparator gives identical
/// results. The link has the size of jemalloc's `rb_node` (two pointers) and the tree the size of `rb_tree` (one
/// pointer), so that the profiling structures have the same sizes (and size classes) as in jemalloc.
///
/// The implementation is a treap whose priorities are a hash of the node address (no extra storage). The operations
/// are recursive; the expected depth is logarithmic.

#include <allocator/Common.h>

#include <cstdint>

namespace jemalloc
{

/// jemalloc: rb_node(a_type)
template <typename T>
struct ProfilingTreeLink
{
    T * left;
    T * right;
};

static_assert(sizeof(ProfilingTreeLink<int>) == 16);

/// jemalloc: rb_tree(a_type) with `rb_gen(..., a_field, a_cmp)`.
template <typename T, ProfilingTreeLink<T> T::* link, int (*compare)(const T *, const T *)>
class ProfilingTree
{
public:
    constexpr ProfilingTree() = default;

    ProfilingTree(const ProfilingTree &) = delete;
    ProfilingTree & operator=(const ProfilingTree &) = delete;

    /// jemalloc: *_tree_new
    void init() { root = nullptr; }

    /// jemalloc: *_tree_empty
    bool empty() const { return root == nullptr; }

    /// jemalloc: *_tree_first
    T * first() const
    {
        T * node = root;
        if (node == nullptr)
            return nullptr;
        while (links(node).left != nullptr)
            node = links(node).left;
        return node;
    }

    /// The successor of `node` (which is in the tree). jemalloc: *_tree_next
    T * next(const T * node) const
    {
        T * successor = nullptr;
        T * current = root;
        while (current != nullptr)
        {
            if (compare(node, current) < 0)
            {
                successor = current;
                current = links(current).left;
            }
            else
            {
                current = links(current).right;
            }
        }
        return successor;
    }

    /// jemalloc: *_tree_insert
    void insert(T * node) { root = insertImpl(root, node); }

    /// jemalloc: *_tree_remove
    void remove(T * node) { root = removeImpl(root, node); }

    /// Visits the nodes in order, starting at `start` (inclusive; the first node if null), until `callback` returns
    /// non-null; returns that value (or null). The callback must not modify the tree.
    /// jemalloc: *_tree_iter
    template <typename F>
    T * iterate(const T * start, F && callback) const
    {
        return iterateImpl(root, start, callback);
    }

private:
    T * root = nullptr;

    static ProfilingTreeLink<T> & links(T * node) { return node->*link; }
    static const ProfilingTreeLink<T> & links(const T * node) { return node->*link; }

    static uint64_t priority(const T * node)
    {
        uint64_t x = static_cast<uint64_t>(reinterpret_cast<uintptr_t>(node));
        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 33;
        x *= 0xc4ceb9fe1a85ec53ULL;
        x ^= x >> 33;
        return x;
    }

    /// Splits `tree` into the nodes less than `key` and the rest.
    static void split(T * tree, const T * key, T ** less, T ** greater)
    {
        if (tree == nullptr)
        {
            *less = nullptr;
            *greater = nullptr;
            return;
        }
        if (compare(tree, key) < 0)
        {
            split(links(tree).right, key, &links(tree).right, greater);
            *less = tree;
        }
        else
        {
            split(links(tree).left, key, less, &links(tree).left);
            *greater = tree;
        }
    }

    /// All nodes of `a` are less than all nodes of `b`.
    static T * merge(T * a, T * b)
    {
        if (a == nullptr)
            return b;
        if (b == nullptr)
            return a;
        if (priority(a) > priority(b))
        {
            links(a).right = merge(links(a).right, b);
            return a;
        }
        links(b).left = merge(a, links(b).left);
        return b;
    }

    static T * insertImpl(T * tree, T * node)
    {
        if (tree == nullptr)
        {
            links(node).left = nullptr;
            links(node).right = nullptr;
            return node;
        }
        if (priority(node) > priority(tree))
        {
            split(tree, node, &links(node).left, &links(node).right);
            return node;
        }
        ALLOCATOR_ASSERT(compare(node, tree) != 0);
        if (compare(node, tree) < 0)
            links(tree).left = insertImpl(links(tree).left, node);
        else
            links(tree).right = insertImpl(links(tree).right, node);
        return tree;
    }

    static T * removeImpl(T * tree, T * node)
    {
        ALLOCATOR_ASSERT(tree != nullptr);
        if (tree == node)
            return merge(links(node).left, links(node).right);
        if (compare(node, tree) < 0)
            links(tree).left = removeImpl(links(tree).left, node);
        else
            links(tree).right = removeImpl(links(tree).right, node);
        return tree;
    }

    template <typename F>
    static T * iterateImpl(T * tree, const T * start, F & callback)
    {
        if (tree == nullptr)
            return nullptr;
        if (start != nullptr && compare(tree, start) < 0)
            return iterateImpl(links(tree).right, start, callback);
        if (T * result = iterateImpl(links(tree).left, start, callback))
            return result;
        if (T * result = callback(tree))
            return result;
        return iterateImpl(links(tree).right, start, callback);
    }
};

}
