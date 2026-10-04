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
struct ProfTreeLink
{
    T * left;
    T * right;
};

static_assert(sizeof(ProfTreeLink<int>) == 16);

/// jemalloc: rb_tree(a_type) with `rb_gen(..., a_field, a_cmp)`.
template <typename T, ProfTreeLink<T> T::*link, int (*compare)(const T *, const T *)>
class ProfTree
{
public:
    constexpr ProfTree() = default;

    ProfTree(const ProfTree &) = delete;
    ProfTree & operator=(const ProfTree &) = delete;

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
        while (lnk(node).left != nullptr)
            node = lnk(node).left;
        return node;
    }

    /// The successor of `node` (which is in the tree). jemalloc: *_tree_next
    T * next(const T * node) const
    {
        T * successor = nullptr;
        T * cur = root;
        while (cur != nullptr)
        {
            if (compare(node, cur) < 0)
            {
                successor = cur;
                cur = lnk(cur).left;
            }
            else
            {
                cur = lnk(cur).right;
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
    T * iter(const T * start, F && callback) const
    {
        return iterImpl(root, start, callback);
    }

private:
    T * root = nullptr;

    static ProfTreeLink<T> & lnk(T * node) { return node->*link; }
    static const ProfTreeLink<T> & lnk(const T * node) { return node->*link; }

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
            split(lnk(tree).right, key, &lnk(tree).right, greater);
            *less = tree;
        }
        else
        {
            split(lnk(tree).left, key, less, &lnk(tree).left);
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
            lnk(a).right = merge(lnk(a).right, b);
            return a;
        }
        lnk(b).left = merge(a, lnk(b).left);
        return b;
    }

    static T * insertImpl(T * tree, T * node)
    {
        if (tree == nullptr)
        {
            lnk(node).left = nullptr;
            lnk(node).right = nullptr;
            return node;
        }
        if (priority(node) > priority(tree))
        {
            split(tree, node, &lnk(node).left, &lnk(node).right);
            return node;
        }
        JE_ASSERT(compare(node, tree) != 0);
        if (compare(node, tree) < 0)
            lnk(tree).left = insertImpl(lnk(tree).left, node);
        else
            lnk(tree).right = insertImpl(lnk(tree).right, node);
        return tree;
    }

    static T * removeImpl(T * tree, T * node)
    {
        JE_ASSERT(tree != nullptr);
        if (tree == node)
            return merge(lnk(node).left, lnk(node).right);
        if (compare(node, tree) < 0)
            lnk(tree).left = removeImpl(lnk(tree).left, node);
        else
            lnk(tree).right = removeImpl(lnk(tree).right, node);
        return tree;
    }

    template <typename F>
    static T * iterImpl(T * tree, const T * start, F & callback)
    {
        if (tree == nullptr)
            return nullptr;
        if (start != nullptr && compare(tree, start) < 0)
            return iterImpl(lnk(tree).right, start, callback);
        if (T * ret = iterImpl(lnk(tree).left, start, callback))
            return ret;
        if (T * ret = callback(tree))
            return ret;
        return iterImpl(lnk(tree).right, start, callback);
    }
};

}
