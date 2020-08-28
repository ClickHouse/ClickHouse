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
    PredicateExpressionsOptimizer(const Context & context_, const TablesWithColumnNames & tables_with_columns_, const Settings & settings_);

    bool optimize(ASTSelectQuery & select_query);

private:
    /// Extracts settings, mostly to show which are used and which are not.
    struct ExtractedSettings
    {
        const bool enable_optimize_predicate_expression;
        const bool enable_optimize_predicate_expression_to_final_subquery;
        const bool allow_push_predicate_when_subquery_contains_with;

        template<typename T>
        ExtractedSettings(const T & settings_)
            :   enable_optimize_predicate_expression(settings_.enable_optimize_predicate_expression),
                enable_optimize_predicate_expression_to_final_subquery(settings_.enable_optimize_predicate_expression_to_final_subquery),
                allow_push_predicate_when_subquery_contains_with(settings_.allow_push_predicate_when_subquery_contains_with)
        {}
    };

    const Context & context;
    const std::vector<TableWithColumnNames> & tables_with_columns;

    const ExtractedSettings settings;

    std::vector<ASTs> extractTablesPredicates(const ASTPtr & where, const ASTPtr & prewhere);

    bool tryRewritePredicatesToTables(ASTs & tables_element, const std::vector<ASTs> & tables_predicates);

    bool tryRewritePredicatesToTable(ASTPtr & table_element, const ASTs & table_predicates, const Names & table_column) const;

    bool tryMovePredicatesFromHavingToWhere(ASTSelectQuery & select_query);
};

}
