#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Interpreters/evaluateQualified.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}

using PredicateExpressions = std::vector<ASTPtr>;
using ProjectionWithAlias = std::pair<ASTPtr, String>;
using ProjectionsWithAliases = std::vector<ProjectionWithAlias>;
using SubqueriesProjectionColumns = std::map<IAST *, ProjectionsWithAliases>;
using IdentifierWithQualifiedName = std::pair<ASTIdentifier *, String>;
using IdentifiersWithQualifiedNameSet = std::vector<IdentifierWithQualifiedName>;


/** This class provides functions for Push-Down predicate expressions
 *
 *  The Example:
 *      - Query before optimization :
 *          SELECT id_1, name_1 FROM (SELECT id_1, name_1 FROM table_a UNION ALL SELECT id_2, name_2 FROM table_b)
 *              WHERE id_1 = 1
 *      - Query after optimization :
 *          SELECT id_1, name_1 FROM (SELECT id_1, name_1 FROM table_a WHERE id_1 = 1 UNION ALL SELECT id_2, name_2 FROM table_b WHERE id_2 = 1)
 *              WHERE id_1 = 1
 *  For more details : https://github.com/yandex/ClickHouse/pull/2015#issuecomment-374283452
 */
class PredicateExpressionsOptimizer
{
public:
    PredicateExpressionsOptimizer(ASTSelectQuery * ast_select_, const Settings & settings_, const Context & context_);

    bool optimize();

private:
    ASTSelectQuery * ast_select;
    const Settings & settings;
    const Context & context;

    enum OptimizeKind
    {
        NONE,
        PUSH_TO_PREWHERE,
        PUSH_TO_WHERE,
        PUSH_TO_HAVING,
    };

    bool isAggregateFunction(ASTPtr & node);

    PredicateExpressions splitConjunctionPredicate(ASTPtr & predicate_expression);

    void getDependenciesAndQualifiedOfExpression(const ASTPtr & expression, IdentifiersWithQualifiedNameSet & dependencies_and_qualified,
                                                 std::vector<DatabaseAndTableWithAlias> & tables_with_aliases);

    bool optimizeExpression(const ASTPtr & outer_expression, ASTPtr & subquery_expression, ASTSelectQuery * subquery);

    bool optimizeImpl(ASTPtr & outer_expression, SubqueriesProjectionColumns & subqueries_projection_columns, bool is_prewhere);

    bool cannotPushDownOuterPredicate(const ProjectionsWithAliases & subquery_projection_columns, ASTSelectQuery * subquery,
        IdentifiersWithQualifiedNameSet & outer_predicate_dependencies, bool & is_prewhere, OptimizeKind & optimize_kind);

    void cloneOuterPredicateForInnerPredicate(const ASTPtr & outer_predicate, const ProjectionsWithAliases & projection_columns,
                                              std::vector<DatabaseAndTableWithAlias> & tables, ASTPtr & inner_predicate);

    void getAllSubqueryProjectionColumns(SubqueriesProjectionColumns & all_subquery_projection_columns);

    void getSubqueryProjectionColumns(SubqueriesProjectionColumns & all_subquery_projection_columns,
                                      String & qualified_name_prefix, const ASTPtr & subquery);

    ASTs getSelectQueryProjectionColumns(ASTPtr & ast);

    std::vector<ASTTableExpression *> getSelectTablesExpression(ASTSelectQuery * select_query);

    ASTs evaluateAsterisk(ASTSelectQuery * select_query, const ASTPtr & asterisk);
};

}
