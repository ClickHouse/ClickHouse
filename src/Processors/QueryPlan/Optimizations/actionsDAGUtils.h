#pragma once

#include <Interpreters/ActionsDAG.h>

namespace DB
{

/// This structure stores a node mapping from one DAG to another.
/// The rule is following:
/// * Input nodes are mapped by name.
/// * Function is mapped to function if all children are mapped and function names are same.
/// * Alias is mapped to it's children mapping.
/// * Monotonic function can be mapped to it's children mapping if direct mapping does not exist.
///   In this case, information about monotonicity is filled.
/// * Mapped node is nullptr if there is no mapping found.
///
/// Overall, directly mapped nodes represent equal calculations.
/// Notes:
/// * Mapped DAG can contain many nodes which represent the same calculation.
///   In this case mapping is ambiguous and only one node is mapped.
/// * Aliases for mapped DAG are not supported.
/// DAG for PK does not contain aliases and ambiguous nodes.
struct MatchedTrees
{
    /// Monotonicity is calculated for monotonic functions chain.
    /// Chain is not strict if there is any non-strict monotonic function.
    struct Monotonicity
    {
        int direction = 1;
        bool strict = true;
    };

    struct Match
    {
        const ActionsDAG::Node * node = nullptr;
        std::optional<Monotonicity> monotonicity;
    };

    using Matches = std::unordered_map<const ActionsDAG::Node *, Match>;
};

MatchedTrees::Matches matchTrees(const ActionsDAG & inner_dag, const ActionsDAG & outer_dag, bool check_monotonicity = true);
}
