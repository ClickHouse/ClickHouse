#include <Interpreters/PredicateRewriteVisitor.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
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
        {
            visit(*child_union, internal_select_list[index]);
        }
        else if (auto * child_select = internal_select_list[index]->as<ASTSelectQuery>())
        {
            visitInternalSelect(index, *child_select, internal_select_list[index]);
        }
        else if (auto * child_intersect_except = internal_select_list[index]->as<ASTSelectIntersectExceptQuery>())
        {
            visit(*child_intersect_except, internal_select_list[index]);
        }
    }
}

void PredicateRewriteVisitorData::visitInternalSelect(size_t index, ASTSelectQuery & select_node, ASTPtr & node)
{
    if (index == 0)
        visitFirstInternalSelect(select_node, node);
    else
        visitOtherInternalSelect(select_node, node);
}

void PredicateRewriteVisitorData::visit(ASTSelectIntersectExceptQuery & intersect_except_query, ASTPtr &)
{
    auto internal_select_list = intersect_except_query.getListOfSelects();
    for (size_t index = 0; index < internal_select_list.size(); ++index)
    {
        if (auto * union_node = internal_select_list[index]->as<ASTSelectWithUnionQuery>())
        {
            visit(*union_node, internal_select_list[index]);
        }
        else if (auto * select_node = internal_select_list[index]->as<ASTSelectQuery>())
        {
            visitInternalSelect(index, *select_node, internal_select_list[index]);
        }
        else if (auto * intersect_node = internal_select_list[index]->as<ASTSelectIntersectExceptQuery>())
        {
            visit(*intersect_node, internal_select_list[index]);
        }
    }
}

void PredicateRewriteVisitorData::visitFirstInternalSelect(ASTSelectQuery & select_query, ASTPtr &)
{
    /// In this case inner_columns same as outer_columns from table_columns
    is_rewrite |= rewriteSubquery(select_query, table_columns.columns.getNames());
}

void PredicateRewriteVisitorData::visitOtherInternalSelect(ASTSelectQuery & select_query, ASTPtr &)
{
    /// For non first select, its alias has no more significance, so we can set a temporary alias for them
    ASTPtr temp_internal_select = select_query.clone();
    ASTSelectQuery * temp_select_query = temp_internal_select->as<ASTSelectQuery>();

    size_t alias_index = 0;
    for (auto & ref_select : temp_select_query->refSelect()->children)
    {
        if (!ref_select->as<ASTAsterisk>() && !ref_select->as<ASTQualifiedAsterisk>() && !ref_select->as<ASTColumnsListMatcher>()
            && !ref_select->as<ASTColumnsRegexpMatcher>() && !ref_select->as<ASTIdentifier>())
        {
            if (const auto & alias = ref_select->tryGetAlias(); alias.empty())
                ref_select->setAlias("--predicate_optimizer_" + toString(alias_index++));
        }
    }

    const Names & internal_columns = InterpreterSelectQuery(
        temp_internal_select,
        const_pointer_cast<Context>(getContext()),
        SelectQueryOptions().analyze()).getSampleBlock().getNames();

    if (rewriteSubquery(*temp_select_query, internal_columns))
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


/// Clean aliases and use aliased name
/// Transforms `(a = b as c) AND (x = y)` to `(a = c) AND (x = y)`
static void useAliasInsteadOfIdentifier(const ASTPtr & predicate)
{
    if (!predicate->as<ASTSubquery>())
    {
        for (auto & children : predicate->children)
            useAliasInsteadOfIdentifier(children);
    }

    if (const auto alias = predicate->tryGetAlias(); !alias.empty())
    {
        if (ASTIdentifier * identifier = predicate->as<ASTIdentifier>())
            identifier->setShortName(alias);
        predicate->setAlias({});
    }
}

static void getConjunctionHashesFrom(const ASTPtr & ast, std::set<IAST::Hash> & hashes)
{
    for (const auto & pred : splitConjunctionsAst(ast))
    {
        /// Clone not to modify `ast`
        ASTPtr pred_copy = pred->clone();
        useAliasInsteadOfIdentifier(pred_copy);
        hashes.emplace(pred_copy->getTreeHash(/*ignore_aliases=*/ true));
    }
}

bool PredicateRewriteVisitorData::rewriteSubquery(ASTSelectQuery & subquery, const Names & inner_columns)
{
    if ((!optimize_final && subquery.final())
        || (subquery.with() && (!optimize_with || hasNonRewritableFunction(subquery.with(), getContext())))
        || subquery.withFill()
        || subquery.limitBy() || subquery.limitLength() || subquery.limitByLength() || subquery.limitByOffset()
        || hasNonRewritableFunction(subquery.select(), getContext())
        || (subquery.orderBy() && subquery.limitOffset()))
        return false;

    Names outer_columns = table_columns.columns.getNames();

    /// Do not add same conditions twice to avoid extra rewrites with exponential blowup
    /// (e.g. in case of deep complex query with lots of JOINs)
    std::set<IAST::Hash> hashes;
    getConjunctionHashesFrom(subquery.where(), hashes);
    getConjunctionHashesFrom(subquery.having(), hashes);

    bool is_changed = false;
    for (const auto & predicate : predicates)
    {
        std::vector<ASTIdentifier *> identifiers;
        ASTPtr optimize_predicate = predicate->clone();
        cleanAliasAndCollectIdentifiers(optimize_predicate, identifiers);

        auto predicate_hash = optimize_predicate->getTreeHash(/*ignore_aliases=*/ true);
        if (hashes.contains(predicate_hash))
            continue;

        hashes.emplace(predicate_hash);
        is_changed = true;

        for (const auto & identifier : identifiers)
        {
            IdentifierSemantic::setColumnShortName(*identifier, table_columns.table);
            const auto & column_name = identifier->name();

            /// For lambda functions, we can't always find them in the list of columns
            /// For example: SELECT * FROM system.one WHERE arrayMap(x -> x, [dummy]) = [0]
            const auto & outer_column_iterator = std::find(outer_columns.begin(), outer_columns.end(), column_name);
            if (outer_column_iterator != outer_columns.end())
            {
                identifier->setShortName(inner_columns[outer_column_iterator - outer_columns.begin()]);
            }
        }

        /// We only need to push all the predicates to subquery having
        /// The subquery optimizer will move the appropriate predicates from having to where
        subquery.setExpression(ASTSelectQuery::Expression::HAVING,
            subquery.having() ? makeASTFunction("and", optimize_predicate, subquery.having()) : optimize_predicate);
    }

    return is_changed;
}

}
