#include <Interpreters/PredicateRewriteVisitor.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/ExtractExpressionInfoVisitor.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>


namespace DB
{

PredicateRewriteVisitorData::PredicateRewriteVisitorData(
    ContextPtr context_,
    const ASTs & predicates_,
    const TableWithColumnNamesAndTypes & table_columns_,
    bool optimize_final_,
    bool optimize_with_)
    : WithContext(context_)
    , predicates(predicates_)
    , table_columns(table_columns_)
    , optimize_final(optimize_final_)
    , optimize_with(optimize_with_)
{
}

void PredicateRewriteVisitorData::visit(ASTSelectWithUnionQuery & union_select_query, ASTPtr &)
{
    auto & internal_select_list = union_select_query.list_of_selects->children;

    for (size_t index = 0; index < internal_select_list.size(); ++index)
    {
        if (auto * child_union = internal_select_list[index]->as<ASTSelectWithUnionQuery>())
            visit(*child_union, internal_select_list[index]);
        else
        {
            if (index == 0)
                visitFirstInternalSelect(*internal_select_list[0]->as<ASTSelectQuery>(), internal_select_list[0]);
            else
                visitOtherInternalSelect(*internal_select_list[index]->as<ASTSelectQuery>(), internal_select_list[index]);
        }
    }
}

void PredicateRewriteVisitorData::visitFirstInternalSelect(ASTSelectQuery & select_query, ASTPtr &)
{
    /// In this case inner_columns same as outer_columns from table_columns
    is_rewrite |= rewriteSubquery(select_query);
}

void PredicateRewriteVisitorData::visitOtherInternalSelect(ASTSelectQuery & select_query, ASTPtr &)
{
    /// For non first select, its alias has no more significance, so we can set a temporary alias for them
    ASTPtr temp_internal_select = select_query.clone();
    ASTSelectQuery * temp_select_query = temp_internal_select->as<ASTSelectQuery>();

    size_t alias_index = 0;
    for (auto & ref_select : temp_select_query->refSelect()->children)
    {
        if (!ref_select->as<ASTAsterisk>() && !ref_select->as<ASTQualifiedAsterisk>() && !ref_select->as<ASTColumnsMatcher>() &&
            !ref_select->as<ASTIdentifier>())
        {
            if (const auto & alias = ref_select->tryGetAlias(); alias.empty())
                ref_select->setAlias("--predicate_optimizer_" + toString(alias_index++));
        }
    }

    if (rewriteSubquery(*temp_select_query))
    {
        is_rewrite |= true;
        select_query.setExpression(ASTSelectQuery::Expression::SELECT, std::move(temp_select_query->refSelect()));
        select_query.setExpression(ASTSelectQuery::Expression::HAVING, std::move(temp_select_query->refHaving()));
    }
}

static void cleanAliasAndCollectIdentifiers(ASTPtr & predicate, std::vector<ASTIdentifier *> & identifiers)
{
    /// Skip WHERE x in (SELECT ...)
    if (!predicate->as<ASTSubquery>())
    {
        for (auto & children : predicate->children)
            cleanAliasAndCollectIdentifiers(children, identifiers);
    }

    if (const auto alias = predicate->tryGetAlias(); !alias.empty())
        predicate->setAlias({});

    if (ASTIdentifier * identifier = predicate->as<ASTIdentifier>())
        identifiers.emplace_back(identifier);
}

bool PredicateRewriteVisitorData::rewriteSubquery(ASTSelectQuery & subquery)
{
    if ((!optimize_final && subquery.final())
        || (!optimize_with && subquery.with())
        || subquery.withFill()
        || subquery.limitBy() || subquery.limitLength()
        || hasNonRewritableFunction(subquery.select(), getContext()))
        return false;

    for (const auto & predicate : predicates)
    {
        std::vector<ASTIdentifier *> identifiers;
        ASTPtr optimize_predicate = predicate->clone();
        cleanAliasAndCollectIdentifiers(optimize_predicate, identifiers);

        for (const auto & identifier : identifiers)
            IdentifierSemantic::setColumnShortName(*identifier, table_columns.table);

        /// We only need to push all the predicates to subquery having
        /// The subquery optimizer will move the appropriate predicates from having to where
        subquery.setExpression(ASTSelectQuery::Expression::HAVING,
            subquery.having() ? makeASTFunction("and", optimize_predicate, subquery.having()) : optimize_predicate);
    }

    return true;
}

}
