#include <IO/Operators.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTProjectionSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


ASTPtr ASTProjectionSelectQuery::clone() const
{
    auto res = std::make_shared<ASTProjectionSelectQuery>(*this);
    res->children.clear();
    res->positions.clear();

#define CLONE(expr) res->setExpression(expr, getExpression(expr, true))

    /** NOTE Members must clone exactly in the same order,
        *  in which they were inserted into `children` in ParserSelectQuery.
        * This is important because of the children's names the identifier (getTreeHash) is compiled,
        *  which can be used for column identifiers in the case of subqueries in the IN statement.
        * For distributed query processing, in case one of the servers is localhost and the other one is not,
        *  localhost query is executed within the process and is cloned,
        *  and the request is sent to the remote server in text form via TCP.
        * And if the cloning order does not match the parsing order,
        *  then different servers will get different identifiers.
        */
    CLONE(Expression::WITH);
    CLONE(Expression::SELECT);
    CLONE(Expression::GROUP_BY);
    CLONE(Expression::ORDER_BY);

#undef CLONE

    return res;
}


void ASTProjectionSelectQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    frame.current_select = this;
    frame.need_parens = false;
    std::string indent_str = s.one_line ? "" : std::string(4 * frame.indent, ' ');

    if (with())
    {
        ostr << (s.hilite ? hilite_keyword : "") << indent_str << "WITH " << (s.hilite ? hilite_none : "");
        s.one_line ? with()->formatImpl(ostr, s, state, frame) : with()->as<ASTExpressionList &>().formatImplMultiline(ostr, s, state, frame);
        ostr << s.nl_or_ws;
    }

    ostr << (s.hilite ? hilite_keyword : "") << indent_str << "SELECT " << (s.hilite ? hilite_none : "");

    s.one_line ? select()->formatImpl(ostr, s, state, frame) : select()->as<ASTExpressionList &>().formatImplMultiline(ostr, s, state, frame);

    if (groupBy())
    {
        ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "GROUP BY " << (s.hilite ? hilite_none : "");
        s.one_line ? groupBy()->formatImpl(ostr, s, state, frame) : groupBy()->as<ASTExpressionList &>().formatImplMultiline(ostr, s, state, frame);
    }

    if (orderBy())
    {
        /// Let's convert tuple ASTFunction into ASTExpressionList, which generates consistent format
        /// between GROUP BY and ORDER BY projection definition.
        ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "ORDER BY " << (s.hilite ? hilite_none : "");
        ASTPtr order_by;
        if (auto * func = orderBy()->as<ASTFunction>(); func && func->name == "tuple")
            order_by = func->arguments;
        else
        {
            order_by = std::make_shared<ASTExpressionList>();
            order_by->children.push_back(orderBy());
        }
        s.one_line ? order_by->formatImpl(ostr, s, state, frame) : order_by->as<ASTExpressionList &>().formatImplMultiline(ostr, s, state, frame);
    }
}

void ASTProjectionSelectQuery::setExpression(Expression expr, ASTPtr && ast)
{
    if (ast)
    {
        auto it = positions.find(expr);
        if (it == positions.end())
        {
            positions[expr] = children.size();
            children.emplace_back(ast);
        }
        else
            children[it->second] = ast;
    }
    else if (positions.contains(expr))
    {
        size_t pos = positions[expr];
        children.erase(children.begin() + pos);
        positions.erase(expr);
        for (auto & pr : positions)
            if (pr.second > pos)
                --pr.second;
    }
}

ASTPtr & ASTProjectionSelectQuery::getExpression(Expression expr)
{
    if (!positions.contains(expr))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Get expression before set");
    return children[positions[expr]];
}

ASTPtr ASTProjectionSelectQuery::cloneToASTSelect() const
{
    auto select_query = std::make_shared<ASTSelectQuery>();
    ASTPtr node = select_query;
    if (with())
        select_query->setExpression(ASTSelectQuery::Expression::WITH, with()->clone());
    if (select())
    {
        ASTPtr select_list = select()->clone();
        if (orderBy())
        {
            /// Add ORDER BY list to SELECT for simplicity. It is Ok because we only uses this to find all required columns.
            auto * expressions = select_list->as<ASTExpressionList>();
            if (!expressions)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Unexpected structure of SELECT clause in projection definition {}; Expression list expected",
                    select_list->dumpTree(0));
            expressions->children.emplace_back(orderBy()->clone());
        }
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list));
    }
    if (groupBy())
        select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, groupBy()->clone());

    /// Attach settings to prevent AST transformations. We already have ignored AST optimizations
    /// for projection queries. Only remaining settings need to be added here.
    ///
    /// NOTE: `count_distinct_implementation` has already been selected during the creation of the
    /// projection, so there will be no countDistinct(...) to rewrite in projection queries.
    /// Ideally, we should aim for a unique and normalized query representation that remains
    /// unchanged after the AST rewrite. For instance, we can add -OrEmpty, realIn as the default
    /// behavior w.r.t -OrNull, nullIn.
    auto settings_query = std::make_shared<ASTSetQuery>();
    SettingsChanges settings_changes;
    settings_changes.insertSetting("aggregate_functions_null_for_empty", false);
    settings_changes.insertSetting("transform_null_in", false);
    settings_changes.insertSetting("legacy_column_name_of_tuple_literal", false);
    settings_query->changes = std::move(settings_changes);
    settings_query->is_standalone = false;
    select_query->setExpression(ASTSelectQuery::Expression::SETTINGS, std::move(settings_query));
    return node;
}

}
