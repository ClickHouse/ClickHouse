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
template <typename QueryTreeNodePtrType>
struct QueryTreeNodeWithHash
{
    QueryTreeNodeWithHash(QueryTreeNodePtrType node_) /// NOLINT
        : node(std::move(node_))
        , hash(node->getTreeHash())
    {}

    QueryTreeNodePtrType node = nullptr;
    CityHash_v1_0_2::uint128 hash;
};

template <typename T>
inline bool operator==(const QueryTreeNodeWithHash<T> & lhs, const QueryTreeNodeWithHash<T> & rhs)
{
    return lhs.hash == rhs.hash && lhs.node->isEqual(*rhs.node);
}

template <typename T>
inline bool operator!=(const QueryTreeNodeWithHash<T> & lhs, const QueryTreeNodeWithHash<T> & rhs)
{
    return !(lhs == rhs);
}

using QueryTreeNodePtrWithHash = QueryTreeNodeWithHash<QueryTreeNodePtr>;
using QueryTreeNodeRawPtrWithHash = QueryTreeNodeWithHash<IQueryTreeNode *>;
using QueryTreeNodeConstRawPtrWithHash = QueryTreeNodeWithHash<const IQueryTreeNode *>;

using QueryTreeNodePtrWithHashSet = std::unordered_set<QueryTreeNodePtrWithHash>;
using QueryTreeNodeConstRawPtrWithHashSet = std::unordered_set<QueryTreeNodeConstRawPtrWithHash>;

template <typename Value>
using QueryTreeNodePtrWithHashMap = std::unordered_map<QueryTreeNodePtrWithHash, Value>;

template <typename Value>
using QueryTreeNodeConstRawPtrWithHashMap = std::unordered_map<QueryTreeNodeConstRawPtrWithHash, Value>;

}

template <typename T>
struct std::hash<DB::QueryTreeNodeWithHash<T>>
{
    size_t operator()(const DB::QueryTreeNodeWithHash<T> & node_with_hash) const
    {
        return node_with_hash.hash.low64;
    }
};
