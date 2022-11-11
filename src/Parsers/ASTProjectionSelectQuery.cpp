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

    s.ostr << (s.hilite ? hilite_keyword : "") << indent_str << "SELECT " << (s.hilite ? hilite_none : "");

    s.one_line ? select()->formatImpl(s, state, frame) : select()->as<ASTExpressionList &>().formatImplMultiline(s, state, frame);

    if (groupBy())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "GROUP BY " << (s.hilite ? hilite_none : "");
        s.one_line ? groupBy()->formatImpl(s, state, frame) : groupBy()->as<ASTExpressionList &>().formatImplMultiline(s, state, frame);
    }

    if (orderBy())
    {
        /// Let's convert the ASTFunction into ASTExpressionList, which generates consistent format
        /// between GROUP BY and ORDER BY projection definition.
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "ORDER BY " << (s.hilite ? hilite_none : "");
        ASTPtr order_by;
        if (auto * func = orderBy()->as<ASTFunction>())
            order_by = func->arguments;
        else
        {
            order_by = std::make_shared<ASTExpressionList>();
            order_by->children.push_back(orderBy());
        }
        s.one_line ? order_by->formatImpl(s, state, frame) : order_by->as<ASTExpressionList &>().formatImplMultiline(s, state, frame);
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
        throw Exception("Get expression before set", ErrorCodes::LOGICAL_ERROR);
    return children[positions[expr]];
}

namespace
{

ASTPtr wrapAsOrderByElement(const ASTPtr & expr)
{
    auto elem = std::make_shared<ASTOrderByElement>();

    elem->direction = 1; /// Currently it cannot be specified in projection definition so it is ASC default
    elem->nulls_direction = elem->direction;
    elem->nulls_direction_was_explicitly_specified = false;
    elem->with_fill = false;
    elem->children.push_back(expr);

    return elem;
}

/// Currently the ORDER BY clause in projection definition is parsed differently from ORDER BY of a standalone SELECT query.
/// This function converts this simplified AST into AST compatible with SELECT query.
/// If ORDER BY clause has single element then simple_order_by represents this element,
/// otherwise simple_order_by is a function "tuple" of all elements.
ASTPtr cloneToOrderByASTForSelect(const ASTPtr & simple_order_by)
{
    ASTPtr expression_list = std::make_shared<ASTExpressionList>();
    if (simple_order_by->children.empty())
    {
        expression_list->children.emplace_back(wrapAsOrderByElement(simple_order_by->clone()));
    }
    else
    {
        auto * func = simple_order_by->as<ASTFunction>();
        if (!func || func->name != "tuple")
        {
            expression_list->children.emplace_back(wrapAsOrderByElement(func->clone()));
        }
        else
        {
            if (func->children.size() != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Unexpected structure of ORDER BY clause in projection definition {}",
                    func->dumpTree(0));

            auto * params = func->children[0]->as<ASTExpressionList>();
            if (!params)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Unexpected structure of ORDER BY clause in projection definition {}; Expression list expected",
                    func->dumpTree(0));

            for (auto & child : params->children)
            {
                expression_list->children.emplace_back(wrapAsOrderByElement(child->clone()));
            }
        }
    }
    return expression_list;
}

}

ASTPtr ASTProjectionSelectQuery::cloneToASTSelect() const
{
    auto select_query = std::make_shared<ASTSelectQuery>();
    ASTPtr node = select_query;
    if (with())
        select_query->setExpression(ASTSelectQuery::Expression::WITH, with()->clone());
    if (select())
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select()->clone());
    if (groupBy())
        select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, groupBy()->clone());
    if (orderBy())
        select_query->setExpression(ASTSelectQuery::Expression::ORDER_BY, cloneToOrderByASTForSelect(orderBy()));
    return node;
}

}
