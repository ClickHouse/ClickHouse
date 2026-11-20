#pragma once

#include <Interpreters/ActionsDAG.h>

#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace DB
{

/// Traversals over the nodes of an `ActionsDAG`. A node can have several parents, so each helper
/// remembers what it has seen and visits a node at most once, not once per path.

enum class VisitResult : uint8_t
{
    Continue,       /// Walk into the children.
    SkipChildren,   /// Do not walk into the children of this node; they may still be reached through another parent.
    Stop,           /// End the whole walk.
};

/// Calls `visit(node)` once for every node reachable from `roots`, in no particular order. `visit` returns
/// a `VisitResult`. Returns false if `visit` stopped the walk.
template <typename Visit>
bool visitReachableNodes(const ActionsDAG::NodeRawConstPtrs & roots, Visit && visit)
{
    std::unordered_set<const ActionsDAG::Node *> visited;
    std::vector<const ActionsDAG::Node *> stack;
    for (const auto * root : roots)
        if (visited.insert(root).second)
            stack.push_back(root);

    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();

        switch (visit(node))
        {
            case VisitResult::Stop:
                return false;
            case VisitResult::SkipChildren:
                continue;
            case VisitResult::Continue:
                break;
        }

        for (const auto * child : node->children)
            if (visited.insert(child).second)
                stack.push_back(child);
    }
    return true;
}

/// Whether `predicate` holds for some node reachable from `roots`. Stops at the first such node.
template <typename Predicate>
bool anyReachableNode(const ActionsDAG::NodeRawConstPtrs & roots, Predicate && predicate)
{
    return !visitReachableNodes(roots, [&](const ActionsDAG::Node * node)
    {
        return predicate(node) ? VisitResult::Stop : VisitResult::Continue;
    });
}

/// Whether `predicate` holds for every node reachable from `roots`. Stops at the first node where it does not.
template <typename Predicate>
bool allReachableNodes(const ActionsDAG::NodeRawConstPtrs & roots, Predicate && predicate)
{
    return !anyReachableNode(roots, [&](const ActionsDAG::Node * node) { return !predicate(node); });
}

/// All nodes reachable from `roots`, including the roots themselves. `roots` may hold duplicates.
/// A node for which `is_barrier` returns true is included, children not entered.
std::unordered_set<const ActionsDAG::Node *> findReachableNodes(
    const ActionsDAG::NodeRawConstPtrs & roots, const std::function<bool(const ActionsDAG::Node *)> & is_barrier = {});

/// Calls `visit(node)` once for every node reachable from `roots`, after it has been called for all the
/// node's children: a topological order, children first. A node for which `skip(node)` is true is neither
/// visited nor entered.
template <typename Visit, typename Skip>
void visitNodesPostOrder(const ActionsDAG::NodeRawConstPtrs & roots, Visit && visit, Skip && skip)
{
    std::unordered_set<const ActionsDAG::Node *> visited;
    std::vector<std::pair<const ActionsDAG::Node *, size_t>> stack;
    for (const auto * root : roots)
    {
        if (skip(root) || !visited.insert(root).second)
            continue;

        stack.emplace_back(root, 0);
        while (!stack.empty())
        {
            auto & [node, next_child] = stack.back();
            if (next_child < node->children.size())
            {
                const auto * child = node->children[next_child++];
                if (!skip(child) && visited.insert(child).second)
                    stack.emplace_back(child, 0); /// Invalidates `node` and `next_child`, which are not used again in this iteration.
                continue;
            }

            visit(node);
            stack.pop_back();
        }
    }
}

template <typename Visit>
void visitNodesPostOrder(const ActionsDAG::NodeRawConstPtrs & roots, Visit && visit)
{
    visitNodesPostOrder(roots, std::forward<Visit>(visit), [](const ActionsDAG::Node *) { return false; });
}

/// Computes `values[node] = compute(node)` for every node reachable from `roots` that has no value yet,
/// children first, so `compute` can read `values.at(child)` for each child. Each node is computed once,
/// however many paths lead to it.
template <typename T, typename Compute>
void computeForNodes(
    const ActionsDAG::NodeRawConstPtrs & roots, std::unordered_map<const ActionsDAG::Node *, T> & values, Compute && compute)
{
    visitNodesPostOrder(
        roots,
        [&](const ActionsDAG::Node * node) { values.emplace(node, compute(node)); },
        [&](const ActionsDAG::Node * node) { return values.contains(node); });
}

}
