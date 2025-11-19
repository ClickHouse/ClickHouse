#pragma once

#include <type_traits>

namespace DB
{

/** Shared CNF reduction algorithms that work with any CNF representation.
  *
  * These template functions are used by both QueryTree CNF (Analyzer::CNF)
  * and ActionsDAG CNF (ActionsDAGCNF) implementations.
  */

/// Reduces CNF groups by removing mutually exclusive atoms found across groups,
/// in case other atoms are identical.
/// Might require multiple passes to complete reduction.
///
/// Example:
/// (x OR y) AND (x OR !y) -> x
template <typename TAndGroup>
TAndGroup reduceOnceCNFStatements(const TAndGroup & groups)
{
    TAndGroup result;
    for (const auto & group : groups)
    {
        using GroupType = std::decay_t<decltype(group)>;
        GroupType copy(group);
        bool inserted = false;
        for (const auto & atom : group)
        {
            using AtomType = std::decay_t<decltype(atom)>;
            AtomType negative_atom(atom);
            negative_atom.negative = !atom.negative;

            // Skip erase-insert for mutually exclusive atoms within
            // single group, since it won't insert negative atom, which
            // will break the logic of this rule
            if (copy.contains(negative_atom))
                continue;

            copy.erase(atom);
            copy.insert(negative_atom);

            if (groups.contains(copy))
            {
                copy.erase(negative_atom);
                result.insert(copy);
                inserted = true;
                break;
            }

            copy.erase(negative_atom);
            copy.insert(atom);
        }
        if (!inserted)
            result.insert(group);
    }
    return result;
}

/// Check if left is a subset of right
template <typename TOrGroup>
bool isCNFGroupSubset(const TOrGroup & left, const TOrGroup & right)
{
    if (left.size() > right.size())
        return false;
    for (const auto & elem : left)
        if (!right.contains(elem))
            return false;
    return true;
}

/// Removes CNF groups if a subset group is found in CNF.
///
/// Example:
/// (x OR y) AND (x) -> x
template <typename TAndGroup>
TAndGroup filterCNFSubsets(const TAndGroup & groups)
{
    TAndGroup result;
    for (const auto & group : groups)
    {
        bool insert = true;

        for (const auto & other_group : groups)
        {
            if (isCNFGroupSubset(other_group, group) && group != other_group)
            {
                insert = false;
                break;
            }
        }

        if (insert)
            result.insert(group);
    }
    return result;
}

}
