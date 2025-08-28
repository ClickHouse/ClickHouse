#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <IO/WriteBuffer.h>
#include <Core/Names.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <base/types.h>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


/// JOIN graph is used to enumerate all allowed combinations of JOINs from the query based on the predicates.
/// This enumeration is more efficient than JOIN Associativity transformation rule and replaces it.
class JoinGraph
{
public:
    using RelationId = size_t;
    using Predicate = std::pair<String, String>;  /// {from_column_name, to_column_name}

    struct PredicateHash
    {
        size_t operator()(const Predicate & predicate) const
        {
            return std::hash<String>()(predicate.first) ^ (std::hash<String>()(predicate.second) << 1);
        }
    };

    using PredicatesSet = std::unordered_set<Predicate, PredicateHash>;

    void addRelation(String name, QueryPlan::Node & node);
    void addEqualityPredicate(const String & lhs_column_name, const String & rhs_column_name);

    size_t size() const { return relations.size(); }

    RelationId getRelationId(const String & name) const { return relation_name_to_id.at(name); }

    RelationId getColumnSourceRelationId(const String & column_name) const
    {
        auto column_source_it = column_source_relation.find(column_name);
        if (column_source_it == column_source_relation.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown column {}", column_name);
        return column_source_it->second;
    }

    PredicatesSet getPredicates(RelationId from, RelationId to) const
    {
        const auto & edges_from = edges.at(from);
        auto edges_from_to_it = edges_from.find(to);
        if (edges_from_to_it == edges_from.end())
            return {};
        return edges_from_to_it->second;
    }

    QueryPlan::Node & getRelationNode(RelationId relation_id) const
    {
        if (relation_id >= relations.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid relation id {}, have {} relations in total", relation_id, relations.size());
        return *relations[relation_id].node;
    }

    void dump(WriteBuffer & out);
    String dump();

private:
    struct RelationInfo
    {
        String name;
        QueryPlan::Node * node;
        Names columns;
    };

    std::vector<RelationInfo> relations;
    std::unordered_map<String, RelationId> relation_name_to_id;
    std::vector<std::unordered_map<RelationId, PredicatesSet>> edges;
    std::unordered_map<String, RelationId> column_source_relation;
};

}
