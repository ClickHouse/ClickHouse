#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

namespace DB
{

class Context;
struct Settings;

/** Predicate optimization based on rewriting ast rules
 *  For more details : https://github.com/ClickHouse/ClickHouse/pull/2015#issuecomment-374283452
 *  The optimizer does two different optimizations
 *      - Move predicates from having to where
 *      - Push the predicate down from the current query to the having of the subquery
 */
class PredicateExpressionsOptimizer
{
public:
    PredicateExpressionsOptimizer(const Context & context_, const TablesWithColumns & tables_with_columns_, const Settings & settings_);

    bool optimize(ASTSelectQuery & select_query);

private:
    const bool enable_optimize_predicate_expression;
    const bool enable_optimize_predicate_expression_to_final_subquery;
    const Context & context;
    const TablesWithColumns & tables_with_columns;

    std::vector<ASTs> extractTablesPredicates(const ASTPtr & where, const ASTPtr & prewhere);

    bool tryRewritePredicatesToTables(ASTs & tables_element, const std::vector<ASTs> & tables_predicates);

    bool tryRewritePredicatesToTable(ASTPtr & table_element, const ASTs & table_predicates, Names && table_columns) const;

    bool tryMovePredicatesFromHavingToWhere(ASTSelectQuery & select_query);
};

}
