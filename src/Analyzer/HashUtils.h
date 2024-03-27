#pragma once

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

/** This structure holds query tree node ptr and its hash. It can be used as hash map key to avoid unnecessary hash
  * recalculations.
  *
  * Example of usage:
  * std::unordered_map<QueryTreeNodeConstRawPtrWithHash, std::string> map;
  */
template <typename QueryTreeNodePtrType, bool compare_aliases = true>
struct QueryTreeNodeWithHash
{
    QueryTreeNodeWithHash(QueryTreeNodePtrType node_) /// NOLINT
        : node(std::move(node_))
        , hash(node->getTreeHash({.compare_aliases = compare_aliases}))
    {}

    QueryTreeNodePtrType node = nullptr;
    CityHash_v1_0_2::uint128 hash;
};

template <typename T, bool compare_aliases>
inline bool operator==(const QueryTreeNodeWithHash<T, compare_aliases> & lhs, const QueryTreeNodeWithHash<T, compare_aliases> & rhs)
{
    return lhs.hash == rhs.hash && lhs.node->isEqual(*rhs.node, {.compare_aliases = compare_aliases});
}

template <typename T, bool compare_aliases>
inline bool operator!=(const QueryTreeNodeWithHash<T, compare_aliases> & lhs, const QueryTreeNodeWithHash<T, compare_aliases> & rhs)
{
    return !(lhs == rhs);
}

using QueryTreeNodePtrWithHash = QueryTreeNodeWithHash<QueryTreeNodePtr>;
using QueryTreeNodePtrWithHashWithoutAlias = QueryTreeNodeWithHash<QueryTreeNodePtr, /*compare_aliases*/ false>;
using QueryTreeNodeRawPtrWithHash = QueryTreeNodeWithHash<IQueryTreeNode *>;
using QueryTreeNodeConstRawPtrWithHash = QueryTreeNodeWithHash<const IQueryTreeNode *>;

using QueryTreeNodePtrWithHashSet = std::unordered_set<QueryTreeNodePtrWithHash>;
using QueryTreeNodePtrWithHashWithoutAliasSet = std::unordered_set<QueryTreeNodePtrWithHashWithoutAlias>;
using QueryTreeNodeConstRawPtrWithHashSet = std::unordered_set<QueryTreeNodeConstRawPtrWithHash>;

template <typename Value>
using QueryTreeNodePtrWithHashMap = std::unordered_map<QueryTreeNodePtrWithHash, Value>;

template <typename Value>
using QueryTreeNodeConstRawPtrWithHashMap = std::unordered_map<QueryTreeNodeConstRawPtrWithHash, Value>;

}

template <typename T, bool compare_aliases>
struct std::hash<DB::QueryTreeNodeWithHash<T, compare_aliases>>
{
    size_t operator()(const DB::QueryTreeNodeWithHash<T> & node_with_hash) const
    {
        return node_with_hash.hash.low64;
    }
};
