#include <Interpreters/StorageID.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTProjectionSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
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
    CLONE(Expression::WHERE);
    CLONE(Expression::GROUP_BY);
    CLONE(Expression::ORDER_BY);

#undef CLONE

    return res;
}


void ASTProjectionSelectQuery::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(distinct);
    IAST::updateTreeHashImpl(hash_state);
}


void ASTProjectionSelectQuery::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    frame.current_select = this;
    frame.need_parens = false;
    std::string indent_str = s.one_line ? "" : std::string(4 * frame.indent, ' ');

    if (with())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << indent_str << "WITH " << (s.hilite ? hilite_none : "");
        s.one_line ? with()->formatImpl(s, state, frame) : with()->as<ASTExpressionList &>().formatImplMultiline(s, state, frame);
        s.ostr << s.nl_or_ws;
    }

    s.ostr << (s.hilite ? hilite_keyword : "") << indent_str << "SELECT " << (distinct ? "DISTINCT " : "") << (s.hilite ? hilite_none : "");

    s.one_line ? select()->formatImpl(s, state, frame) : select()->as<ASTExpressionList &>().formatImplMultiline(s, state, frame);

    if (where())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "WHERE " << (s.hilite ? hilite_none : "");
        where()->formatImpl(s, state, frame);
    }

    if (groupBy())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "GROUP BY " << (s.hilite ? hilite_none : "");
        s.one_line ? groupBy()->formatImpl(s, state, frame) : groupBy()->as<ASTExpressionList &>().formatImplMultiline(s, state, frame);
    }

    if (orderBy())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "ORDER BY " << (s.hilite ? hilite_none : "");
        orderBy()->formatImpl(s, state, frame);
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
    else if (positions.count(expr))
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
    if (!positions.count(expr))
        throw Exception("Get expression before set", ErrorCodes::LOGICAL_ERROR);
    return children[positions[expr]];
}

ASTPtr ASTProjectionSelectQuery::cloneToASTSelect() const
{
    auto select_query = std::make_shared<ASTSelectQuery>();
    ASTPtr node = select_query;
    if (with())
        select_query->setExpression(ASTSelectQuery::Expression::WITH, with()->clone());
    if (select())
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select()->clone());
    if (where())
        select_query->setExpression(ASTSelectQuery::Expression::WHERE, where()->clone());
    if (groupBy())
        select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, groupBy()->clone());
    // Get rid of orderBy. It's used for projection definition only
    if (orderBy())
        select_query->setExpression(ASTSelectQuery::Expression::ORDER_BY, orderBy()->clone());
    return node;
}

}
