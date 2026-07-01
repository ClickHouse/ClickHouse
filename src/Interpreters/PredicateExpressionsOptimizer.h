#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

namespace DB
{

class ASTSelectQuery;
struct Settings;

/** Predicate optimization based on rewriting ast rules
 *  For more details : https://github.com/ClickHouse/ClickHouse/pull/2015#issuecomment-374283452
 *  The optimizer does two different optimizations
 *      - Move predicates from having to where
 *      - Push the predicate down from the current query to the having of the subquery
 */
class PredicateExpressionsOptimizer : WithContext
{
public:
    PredicateExpressionsOptimizer(ContextPtr context_, const TablesWithColumns & tables_with_columns_, const Settings & settings_);

    bool optimize(ASTSelectQuery & select_query);

    /// Move conjuncts of HAVING that reference only GROUP BY keys (no aggregate/window functions)
    /// into WHERE. This is a semantics-preserving optimization for plain GROUP BY that enables
    /// pre-aggregation filtering (partition/index pruning). The caller must ensure the query has no
    /// WITH CUBE/ROLLUP/TOTALS/GROUPING SETS, where a grouping key can be NULL in super-aggregate
    /// rows and moving the predicate to WHERE would change results.
    static bool tryMovePredicatesFromHavingToWhere(ASTSelectQuery & select_query, ContextPtr context);

private:
    const bool enable_optimize_predicate_expression;
    const bool enable_optimize_predicate_expression_to_final_subquery;
    const bool allow_push_predicate_when_subquery_contains_with;
    const TablesWithColumns & tables_with_columns;

    std::vector<ASTs> extractTablesPredicates(const ASTPtr & where, const ASTPtr & prewhere);

    bool tryRewritePredicatesToTables(ASTs & tables_element, const std::vector<ASTs> & tables_predicates);

    bool tryRewritePredicatesToTable(
        ASTPtr & table_element, const ASTs & table_predicates, const TableWithColumnNamesAndTypes & table_columns) const;

    bool tryMovePredicatesFromHavingToWhere(ASTSelectQuery & select_query);
};

}
