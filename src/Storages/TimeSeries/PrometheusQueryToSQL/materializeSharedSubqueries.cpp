#include <Storages/TimeSeries/PrometheusQueryToSQL/materializeSharedSubqueries.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTWithElement.h>
#include <base/types.h>

#include <unordered_map>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Counts how many times each single-part table name occurs as a table expression (i.e. in FROM or JOIN).
    /// Qualified column references like `prometheus_query_step_1.values` are ordinary compound identifiers,
    /// not table identifiers, and don't cause a re-evaluation of the subquery, so they're not counted.
    void countTableReferences(const IAST & node, std::unordered_map<String, size_t> & ref_count)
    {
        if (const auto * table_identifier = node.as<ASTTableIdentifier>())
        {
            if (!table_identifier->compound())
                ++ref_count[table_identifier->shortName()];
        }

        for (const auto & child : node.children)
            countTableReferences(*child, ref_count);
    }
}


void materializeSharedSubqueries(const ASTPtr & final_query)
{
    const auto * select_with_union = final_query->as<ASTSelectWithUnionQuery>();
    if (!select_with_union || !select_with_union->list_of_selects)
        return;

    std::unordered_map<String, size_t> ref_count;
    countTableReferences(*final_query, ref_count);

    /// All named subqueries are collected in the WITH clauses of the top-level SELECTs
    /// (see SelectQueryBuilder::getSelectQuery() and finalizeSQL()).
    for (const auto & select : select_with_union->list_of_selects->children)
    {
        const auto * select_query = select->as<ASTSelectQuery>();
        if (!select_query)
            continue;

        auto with = select_query->with();
        if (!with)
            continue;

        for (const auto & with_child : with->children)
        {
            /// Scalar subqueries (`WITH (SELECT ...) AS name`) are not ASTWithElement and are skipped here;
            /// they're evaluated once regardless of how many times they're referenced.
            auto * with_element = with_child->as<ASTWithElement>();
            if (with_element && (ref_count[with_element->name] > 1))
                with_element->is_materialized = true;
        }
    }
}

}
