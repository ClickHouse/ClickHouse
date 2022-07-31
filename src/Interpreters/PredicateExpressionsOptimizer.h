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

private:
    const bool enable_optimize_predicate_expression;
    const bool enable_optimize_predicate_expression_to_final_subquery;
    const bool allow_push_predicate_when_subquery_contains_with;
    const TablesWithColumns & tables_with_columns;

    std::vector<ASTList> extractTablesPredicates(const ASTPtr & where, const ASTPtr & prewhere);

    bool tryRewritePredicatesToTables(ASTList & tables_element, const std::vector<ASTList> & tables_predicates);

    bool tryRewritePredicatesToTable(
        ASTPtr & table_element, const ASTList & table_predicates, const TableWithColumnNamesAndTypes & table_columns) const;

    bool tryMovePredicatesFromHavingToWhere(ASTSelectQuery & select_query);
};

}
