#include <Processors/QueryPlan/Optimizations/Cascades/JoinGraph.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

void JoinGraph::addRelation(String name, QueryPlan::Node & node)
{
    if (name.empty())
        name = "__rel_" + toString(relations.size());

    if (relation_name_to_id.contains(name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Relation '{}' already exists in the grapsh", name);

    RelationId new_relation_id = relations.size();

    auto column_names = node.step->getOutputHeader()->getNames();
    for (const auto & column_name : column_names)
    {
        if (column_source_relation.contains(column_name))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column '{}' already added from '{}'", column_name, column_source_relation.at(column_name));
        column_source_relation[column_name] = new_relation_id;
    }

    relations.push_back({name, &node, column_names});
    edges.push_back({});
    relation_name_to_id[name] = new_relation_id;
}


void JoinGraph::addEqualityPredicate(const String & lhs_column_name, const String & rhs_column_name)
{
    auto lhs_relation_id = getColumnSourceRelationId(lhs_column_name);
    auto rhs_relation_id = getColumnSourceRelationId(rhs_column_name);
    edges[lhs_relation_id][rhs_relation_id].insert({lhs_column_name, rhs_column_name});
    edges[rhs_relation_id][lhs_relation_id].insert({rhs_column_name, lhs_column_name});
}


void JoinGraph::dump(WriteBuffer & out)
{
    out << "Relations:\n";
    for (RelationId relation_id = 0; relation_id < relations.size(); ++relation_id)
        out << relation_id << ": " << relations[relation_id].name << "\n";

    out << "Edges:\n";
    for (RelationId from_relation_id = 0; from_relation_id < relations.size(); ++from_relation_id)
    {
        for (const auto & edge : edges[from_relation_id])
        {
            out << from_relation_id << " -> " << edge.first;
            for (const auto & predicate : edge.second)
                out << " (" << predicate.first << " = " << predicate.second << ")";
            out << "\n";
        }
    }
}

String JoinGraph::dump()
{
    WriteBufferFromOwnString out;
    dump(out);
    return out.str();
}

}
