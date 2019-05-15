#pragma once

#include "DatabaseAndTableWithAlias.h"
#include "ExpressionAnalyzer.h"
#include <Parsers/ASTSelectQuery.h>
#include <map>

namespace DB
{

class ASTIdentifier;
class ASTSubquery;
class Context;

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
    using ProjectionWithAlias = std::pair<ASTPtr, String>;
    using SubqueriesProjectionColumns = std::map<ASTSelectQuery *, std::vector<ProjectionWithAlias>>;
    using IdentifierWithQualifier = std::pair<ASTIdentifier *, String>;

    /// Extracts settings, mostly to show which are used and which are not.
    struct ExtractedSettings
    {
        /// QueryNormalizer settings
        const UInt64 max_ast_depth;
        const UInt64 max_expanded_ast_elements;
        const String count_distinct_implementation;

        /// for PredicateExpressionsOptimizer
        const bool enable_optimize_predicate_expression;
        const bool join_use_nulls;

        template<typename T>
        ExtractedSettings(const T & settings)
        :   max_ast_depth(settings.max_ast_depth),
            max_expanded_ast_elements(settings.max_expanded_ast_elements),
            count_distinct_implementation(settings.count_distinct_implementation),
            enable_optimize_predicate_expression(settings.enable_optimize_predicate_expression),
            join_use_nulls(settings.join_use_nulls)
        {}
    };

public:
    PredicateExpressionsOptimizer(ASTSelectQuery * ast_select_, ExtractedSettings && settings_, const Context & context_);

    bool optimize();

private:
    ASTSelectQuery * ast_select;
    const ExtractedSettings settings;
    const Context & context;

    enum OptimizeKind
    {
        NONE,
        PUSH_TO_PREWHERE,
        PUSH_TO_WHERE,
        PUSH_TO_HAVING,
    };

    bool isArrayJoinFunction(const ASTPtr & node);

    std::vector<ASTPtr> splitConjunctionPredicate(const ASTPtr & predicate_expression);

    std::vector<IdentifierWithQualifier> getDependenciesAndQualifiers(ASTPtr & expression,
                                                                      std::vector<TableWithColumnNames> & tables_with_aliases);

    bool optimizeExpression(const ASTPtr & outer_expression, ASTSelectQuery * subquery, ASTSelectQuery::Expression expr);

    bool optimizeImpl(const ASTPtr & outer_expression, const SubqueriesProjectionColumns & subqueries_projection_columns, OptimizeKind optimize_kind);

    bool allowPushDown(
        const ASTSelectQuery * subquery,
        const ASTPtr & outer_predicate,
        const std::vector<ProjectionWithAlias> & subquery_projection_columns,
        const std::vector<IdentifierWithQualifier> & outer_predicate_dependencies,
        OptimizeKind & optimize_kind);

    bool checkDependencies(
        const std::vector<ProjectionWithAlias> & projection_columns,
        const std::vector<IdentifierWithQualifier> & dependencies,
        OptimizeKind & optimize_kind);

    void setNewAliasesForInnerPredicate(const std::vector<ProjectionWithAlias> & projection_columns,
                                        const std::vector<IdentifierWithQualifier> & inner_predicate_dependencies);

    SubqueriesProjectionColumns getAllSubqueryProjectionColumns();

    void getSubqueryProjectionColumns(const ASTPtr & subquery, SubqueriesProjectionColumns & all_subquery_projection_columns);

    ASTs getSelectQueryProjectionColumns(ASTPtr & ast);

    ASTs evaluateAsterisk(ASTSelectQuery * select_query, const ASTPtr & asterisk);

    void cleanExpressionAlias(ASTPtr & expression);
};

}
