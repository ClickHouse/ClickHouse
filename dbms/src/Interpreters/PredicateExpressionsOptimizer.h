#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>

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
using SubQueriesProjectionColumns = std::map<IAST *, ProjectionsWithAliases>;


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
    PredicateExpressionsOptimizer(ASTSelectQuery * ast_select_, const Settings & settings_);

    bool optimize();

private:
    ASTSelectQuery * ast_select;
    const Settings & settings;

    enum OptimizeKind
    {
        NONE,
        PUSH_TO_PREWHERE,
        PUSH_TO_WHERE,
        PUSH_TO_HAVING,
    };

    bool isAggregateFunction(ASTPtr & node);

    PredicateExpressions splitConjunctionPredicate(ASTPtr & predicate_expression);

    void getExpressionDependentColumns(const ASTPtr & expression, ASTs & expression_dependent_columns);

    bool optimizeExpression(const ASTPtr & outer_expression, ASTPtr & subquery_expression, ASTSelectQuery * subquery);

    bool optimizeImpl(ASTPtr & outer_expression, SubQueriesProjectionColumns & sub_queries_projection_columns, bool is_prewhere);

    bool cannotPushDownOuterPredicate(
        const ProjectionsWithAliases & subquery_projection_columns, ASTSelectQuery * subquery,
        ASTs & expression_dependent_columns, bool & is_prewhere, OptimizeKind & optimize_kind);

    void cloneOuterPredicateForInnerPredicate(
        const ASTPtr & outer_predicate, const ProjectionsWithAliases & projection_columns, ASTs & predicate_dependent_columns,
        ASTPtr & inner_predicate);


    void getAllSubqueryProjectionColumns(IAST * node, SubQueriesProjectionColumns & all_subquery_projection_columns);

    void getSubqueryProjectionColumns(IAST * subquery, SubQueriesProjectionColumns & all_subquery_projection_columns, ASTs & output_projections);
};

}
