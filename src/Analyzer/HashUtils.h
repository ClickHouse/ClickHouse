#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>

#include <city.h>

namespace DB
{

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

/** This structure holds query tree node ptr and its hash. It can be used as hash map key to avoid unnecessary hash
  * recalculations.
  *
  * The `global` parameter selects the comparison semantics; there is intentionally no default, so
  * every use site states which one it needs:
  * - global = true (getTreeHashGlobal/isEqualGlobal): canonical across query trees, column
  *   references are compared by the content of their column sources. Use it when keys may come from
  *   different query trees, or when two structurally equal column sources must be treated as equal
  *   regardless of which instance they are.
  * - global = false (getTreeHashLocal/isEqualLocal): column references are compared by their column
  *   source ids, so keys are meaningful only within one query tree and its clones, and distinct
  *   source instances (e.g. the two sides of a self join) are distinguished.
  *
  * Example of usage:
  * std::unordered_map<QueryTreeNodeConstRawPtrWithGlobalHash, std::string> map;
  */
template <typename QueryTreeNodePtrType, bool compare_aliases, bool global>
struct QueryTreeNodeWithHash
{
    QueryTreeNodeWithHash(QueryTreeNodePtrType node_) /// NOLINT
        : node(std::move(node_))
        , hash(global ? node->getTreeHashGlobal({.compare_aliases = compare_aliases})
                      : node->getTreeHashLocal({.compare_aliases = compare_aliases}))
    {}

    QueryTreeNodePtrType node = nullptr;
    CityHash_v1_0_2::uint128 hash;
};

template <typename T, bool compare_aliases, bool global>
inline bool operator==(const QueryTreeNodeWithHash<T, compare_aliases, global> & lhs, const QueryTreeNodeWithHash<T, compare_aliases, global> & rhs)
{
    if (lhs.hash != rhs.hash)
        return false;

    if constexpr (global)
        return lhs.node->isEqualGlobal(*rhs.node, {.compare_aliases = compare_aliases});
    else
        return lhs.node->isEqualLocal(*rhs.node, {.compare_aliases = compare_aliases});
}

template <typename T, bool compare_aliases, bool global>
inline bool operator!=(const QueryTreeNodeWithHash<T, compare_aliases, global> & lhs, const QueryTreeNodeWithHash<T, compare_aliases, global> & rhs)
{
    return !(lhs == rhs);
}

/// Global hash: canonical across query trees (column sources compared by content).
using QueryTreeNodePtrWithGlobalHash = QueryTreeNodeWithHash<QueryTreeNodePtr, /*compare_aliases*/ true, /*global*/ true>;
using QueryTreeNodePtrWithGlobalHashIgnoreAliases = QueryTreeNodeWithHash<QueryTreeNodePtr, /*compare_aliases*/ false, /*global*/ true>;
using QueryTreeNodeRawPtrWithGlobalHash = QueryTreeNodeWithHash<IQueryTreeNode *, /*compare_aliases*/ true, /*global*/ true>;
using QueryTreeNodeConstRawPtrWithGlobalHash = QueryTreeNodeWithHash<const IQueryTreeNode *, /*compare_aliases*/ true, /*global*/ true>;

using QueryTreeNodePtrWithGlobalHashSet = std::unordered_set<QueryTreeNodePtrWithGlobalHash>;
using QueryTreeNodePtrWithGlobalHashIgnoreAliasesSet = std::unordered_set<QueryTreeNodePtrWithGlobalHashIgnoreAliases>;
using QueryTreeNodeConstRawPtrWithGlobalHashSet = std::unordered_set<QueryTreeNodeConstRawPtrWithGlobalHash>;

template <typename Value>
using QueryTreeNodePtrWithGlobalHashMap = std::unordered_map<QueryTreeNodePtrWithGlobalHash, Value>;

template <typename Value>
using QueryTreeNodePtrWithGlobalHashIgnoreAliasesMap = std::unordered_map<QueryTreeNodePtrWithGlobalHashIgnoreAliases, Value>;

/// Local hash: valid only within one query tree (column sources compared by id, self-join sides distinguished).
using QueryTreeNodePtrWithLocalHash = QueryTreeNodeWithHash<QueryTreeNodePtr, /*compare_aliases*/ true, /*global*/ false>;
using QueryTreeNodePtrWithLocalHashIgnoreAliases = QueryTreeNodeWithHash<QueryTreeNodePtr, /*compare_aliases*/ false, /*global*/ false>;
using QueryTreeNodeRawPtrWithLocalHash = QueryTreeNodeWithHash<IQueryTreeNode *, /*compare_aliases*/ true, /*global*/ false>;
using QueryTreeNodeConstRawPtrWithLocalHash = QueryTreeNodeWithHash<const IQueryTreeNode *, /*compare_aliases*/ true, /*global*/ false>;

using QueryTreeNodePtrWithLocalHashSet = std::unordered_set<QueryTreeNodePtrWithLocalHash>;
using QueryTreeNodePtrWithLocalHashIgnoreAliasesSet = std::unordered_set<QueryTreeNodePtrWithLocalHashIgnoreAliases>;
using QueryTreeNodeConstRawPtrWithLocalHashSet = std::unordered_set<QueryTreeNodeConstRawPtrWithLocalHash>;

template <typename Value>
using QueryTreeNodePtrWithLocalHashMap = std::unordered_map<QueryTreeNodePtrWithLocalHash, Value>;

template <typename Value>
using QueryTreeNodePtrWithLocalHashIgnoreAliasesMap = std::unordered_map<QueryTreeNodePtrWithLocalHashIgnoreAliases, Value>;

class ColumnNode;
using ColumnNodePtr = std::shared_ptr<ColumnNode>;
using ColumnNodePtrWithGlobalHash = QueryTreeNodeWithHash<ColumnNodePtr, /*compare_aliases*/ true, /*global*/ true>;
using ColumnNodePtrWithGlobalHashSet = std::unordered_set<ColumnNodePtrWithGlobalHash>;
using ColumnNodePtrWithLocalHash = QueryTreeNodeWithHash<ColumnNodePtr, /*compare_aliases*/ true, /*global*/ false>;
using ColumnNodePtrWithLocalHashSet = std::unordered_set<ColumnNodePtrWithLocalHash>;

}

template <typename T, bool compare_aliases, bool global>
struct std::hash<DB::QueryTreeNodeWithHash<T, compare_aliases, global>>
{
    size_t operator()(const DB::QueryTreeNodeWithHash<T, compare_aliases, global> & node_with_hash) const
    {
        return node_with_hash.hash.low64;
    }
};
