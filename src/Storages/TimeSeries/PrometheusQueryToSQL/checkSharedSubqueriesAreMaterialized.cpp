#include <Storages/TimeSeries/PrometheusQueryToSQL/checkSharedSubqueriesAreMaterialized.h>

#include <base/defines.h>

#ifdef DEBUG_OR_SANITIZER_BUILD

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTWithElement.h>

#include <fmt/format.h>

#include <unordered_map>

#endif


namespace DB::PrometheusQueryToSQL
{

#ifdef DEBUG_OR_SANITIZER_BUILD

namespace
{
    /// Counts how many times each table name from `ref_count` occurs as a table expression (i.e. in FROM or JOIN);
    /// names not present in `ref_count` are ignored. Qualified column references like `prometheus_query_step_1.values`
    /// are ordinary compound identifiers, not table identifiers, and don't cause a re-evaluation of the subquery,
    /// so they're not counted.
    void countTableReferences(const IAST & node, std::unordered_map<String, size_t> & ref_count)
    {
        if (const auto * table_identifier = node.as<ASTTableIdentifier>())
        {
            if (!table_identifier->compound())
            {
                auto it = ref_count.find(table_identifier->shortName());
                if (it != ref_count.end())
                    ++it->second;
            }
        }

        for (const auto & child : node.children)
            countTableReferences(*child, ref_count);
    }
}

void checkSharedSubqueriesAreMaterialized(const ASTPtr & final_query)
{
    const auto * select_with_union = final_query->as<ASTSelectWithUnionQuery>();
    if (!select_with_union || !select_with_union->list_of_selects)
        return;

    /// All named subqueries are collected in the WITH clauses of the top-level SELECTs
    /// (see SelectQueryBuilder::getSelectQuery() and finalizeSQL()).
    std::unordered_map<String, size_t> ref_count;
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
            const auto * with_element = with_child->as<ASTWithElement>();
            if (with_element && !with_element->is_materialized)
                ref_count[with_element->name] = 0;
        }
    }

    if (ref_count.empty())
        return;

    countTableReferences(*final_query, ref_count);

    for (const auto & [name, count] : ref_count)
    {
        chassert(count <= 1,
                 fmt::format("Named subquery {} is referenced multiple times ({}) but was not added as a materialized CTE",
                             name, count));
    }
}

#else

void checkSharedSubqueriesAreMaterialized(const ASTPtr &)
{
}

#endif

}
