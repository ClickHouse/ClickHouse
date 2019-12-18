#include <Interpreters/PredicateRewriteVisitor.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/ExtractExpressionInfoVisitor.h>


namespace DB
{

PredicateRewriteVisitorData::PredicateRewriteVisitorData(
    const Context & context_, const ASTs & predicates_, const Names & colunm_names_, bool optimize_final_)
    : context(context_), predicates(predicates_), column_names(colunm_names_), optimize_final(optimize_final_)
{
}

void PredicateRewriteVisitorData::visit(ASTSelectWithUnionQuery & union_select_query, ASTPtr &)
{
    auto & internal_select_list = union_select_query.list_of_selects->children;

    if (internal_select_list.size() > 0)
        visitFirstInternalSelect(*internal_select_list[0]->as<ASTSelectQuery>(), internal_select_list[0]);

    for (size_t index = 1; index < internal_select_list.size(); ++index)
        visitOtherInternalSelect(*internal_select_list[index]->as<ASTSelectQuery>(), internal_select_list[index]);
}

void PredicateRewriteVisitorData::visitFirstInternalSelect(ASTSelectQuery & select_query, ASTPtr &)
{
    is_rewrite |= rewriteSubquery(select_query, column_names, column_names);
}

void PredicateRewriteVisitorData::visitOtherInternalSelect(ASTSelectQuery & select_query, ASTPtr &)
{
    /// For non first select, its alias has no more significance, so we can set a temporary alias for them
    ASTPtr temp_internal_select = select_query.clone();
    ASTSelectQuery * temp_select_query = temp_internal_select->as<ASTSelectQuery>();

    size_t alias_index = 0;
    for (const auto ref_select : temp_select_query->refSelect()->children)
    {
        if (!ref_select->as<ASTAsterisk>() && !ref_select->as<ASTQualifiedAsterisk>() && !ref_select->as<ASTColumnsMatcher>())
        {
            if (const auto & alias = ref_select->tryGetAlias(); alias.empty())
                ref_select->setAlias("--predicate_optimizer_" + toString(alias_index++));
        }
    }

    const Names & internal_columns = InterpreterSelectQuery(
        temp_internal_select, context, SelectQueryOptions().analyze()).getSampleBlock().getNames();

    if ((is_rewrite |= rewriteSubquery(*temp_select_query, column_names, internal_columns)))
    {
        select_query.setExpression(ASTSelectQuery::Expression::SELECT, std::move(temp_select_query->refSelect()));

        if (temp_select_query->where())
            select_query.setExpression(ASTSelectQuery::Expression::WHERE, std::move(temp_select_query->refWhere()));

        if (temp_select_query->having())
            select_query.setExpression(ASTSelectQuery::Expression::HAVING, std::move(temp_select_query->refHaving()));
    }
}

static void cleanAliasAndCollectIdentifiers(ASTPtr & predicate, std::vector<ASTIdentifier *> & identifiers)
{
    for (auto & children : predicate->children)
        cleanAliasAndCollectIdentifiers(children, identifiers);

    if (const auto alias = predicate->tryGetAlias(); !alias.empty())
        predicate->setAlias("");

    if (ASTIdentifier * identifier = predicate->as<ASTIdentifier>())
        identifiers.emplace_back(identifier);
}

bool PredicateRewriteVisitorData::allowPushDown(const ASTSelectQuery &subquery, NameSet & aggregate_column)
{
    if ((!optimize_final && subquery.final())
        || subquery.limitBy() || subquery.limitLength()
        || subquery.with() || subquery.withFill())
        return false;

    for (const auto & select_expression : subquery.select()->children)
    {
        ExpressionInfoVisitor::Data expression_info{.context = context, .tables = {}};
        ExpressionInfoVisitor(expression_info).visit(select_expression);

        if (expression_info.is_stateful_function)
            return false;
        else if (expression_info.is_aggregate_function)
            aggregate_column.emplace(select_expression->getAliasOrColumnName());
    }

    return true;
}

bool PredicateRewriteVisitorData::rewriteSubquery(ASTSelectQuery & subquery, const Names & outer_columns, const Names & inner_columns)
{
    NameSet aggregate_columns;

    if (!allowPushDown(subquery, aggregate_columns))
        return false;

    for (const auto & predicate : predicates)
    {
        std::vector<ASTIdentifier *> identifiers;
        ASTPtr optimize_predicate = predicate->clone();
        cleanAliasAndCollectIdentifiers(optimize_predicate, identifiers);

        ASTSelectQuery::Expression rewrite_to = ASTSelectQuery::Expression::WHERE;

        for (size_t index = 0; index < identifiers.size(); ++index)
        {
            const auto & column_name = IdentifierSemantic::getColumnName(*identifiers[index]);

            const auto & iterator = std::find(outer_columns.begin(), outer_columns.end(), column_name);

            if (iterator == outer_columns.end())
                throw Exception("", ErrorCodes::LOGICAL_ERROR);

            if (aggregate_columns.count(*column_name))
                rewrite_to = ASTSelectQuery::Expression::HAVING;

            identifiers[index]->setShortName(inner_columns[iterator - outer_columns.begin()]);
        }

        ASTPtr optimize_expression = subquery.getExpression(rewrite_to, false);
        subquery.setExpression(rewrite_to,
            optimize_expression ? makeASTFunction("and", optimize_predicate, optimize_expression) : optimize_predicate);
    }
    return true;
}

}
