#pragma once

#include <memory>
#include <unordered_map>

#include <city.h>

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ValidationUtils.h>

namespace DB
{

/** Hash map key that identifies a GROUP BY key node the same way the aggregation layer does.
  *
  * Unlike QueryTreeNodePtrWithHashIgnoreAliases this also compares column SOURCES (via
  * compareGroupByKeys), so `l.number` and `r.number` of a self-join are distinct keys. The tree hash
  * ignores aliases but does include each column's source, so it stays consistent with the comparison.
  */
struct GroupByKeyComparator
{
    GroupByKeyComparator(QueryTreeNodePtr node_) /// NOLINT
        : node(std::move(node_))
        , hash(node->getTreeHash({.compare_aliases = false}))
    {}

    bool operator==(const GroupByKeyComparator & other) const { return hash == other.hash && compareGroupByKeys(node, other.node); }

    bool operator!=(const GroupByKeyComparator & other) const { return !(*this == other); }

    struct Hasher { size_t operator()(const GroupByKeyComparator & key) const { return key.hash.low64; } };

    QueryTreeNodePtr node = nullptr;
    CityHash_v1_0_2::uint128 hash;
};

template <typename Value>
using AggredationKeyNodeMap = std::unordered_map<GroupByKeyComparator, Value, GroupByKeyComparator::Hasher>;

}
