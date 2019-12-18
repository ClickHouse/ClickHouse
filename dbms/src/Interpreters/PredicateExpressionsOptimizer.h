#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>

namespace DB
{

class Context;
class Settings;

/** This class provides functions for Push-Down predicate expressions
 *
 *  The Example:
 *      - Query before optimization :
 *          SELECT id_1, name_1 FROM (SELECT id_1, name_1 FROM table_a UNION ALL SELECT id_2, name_2 FROM table_b)
 *              WHERE id_1 = 1
 *      - Query after optimization :
 *          SELECT id_1, name_1 FROM (SELECT id_1, name_1 FROM table_a WHERE id_1 = 1 UNION ALL SELECT id_2, name_2 FROM table_b WHERE id_2 = 1)
 *              WHERE id_1 = 1
 *  For more details : https://github.com/ClickHouse/ClickHouse/pull/2015#issuecomment-374283452
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

        template<typename T>
        ExtractedSettings(const T & settings_)
            :   enable_optimize_predicate_expression(settings_.enable_optimize_predicate_expression),
                enable_optimize_predicate_expression_to_final_subquery(settings_.enable_optimize_predicate_expression_to_final_subquery)
        {}
    };

    const Context & context;
    const std::vector<TableWithColumnNames> & tables_with_columns;

    const ExtractedSettings settings;

    std::vector<ASTs> extractTablesPredicates(const ASTPtr & where, const ASTPtr & prewhere);

    bool tryRewritePredicatesToTables(ASTs & tables_element, const std::vector<ASTs> & tables_predicates);

    bool tryRewritePredicatesToTable(ASTPtr & table_element, const ASTs & table_predicates, const Names & table_column) const;
};

}
