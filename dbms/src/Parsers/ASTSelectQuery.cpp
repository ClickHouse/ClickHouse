#include <Common/typeid_cast.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


ASTPtr ASTSelectQuery::clone() const
{
    auto res = std::make_shared<ASTSelectQuery>(*this);
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
    CLONE(Expression::TABLES);
    CLONE(Expression::PREWHERE);
    CLONE(Expression::WHERE);
    CLONE(Expression::GROUP_BY);
    CLONE(Expression::HAVING);
    CLONE(Expression::ORDER_BY);
    CLONE(Expression::LIMIT_BY_OFFSET);
    CLONE(Expression::LIMIT_BY_LENGTH);
    CLONE(Expression::LIMIT_BY);
    CLONE(Expression::LIMIT_OFFSET);
    CLONE(Expression::LIMIT_LENGTH);
    CLONE(Expression::SETTINGS);

#undef CLONE

    return res;
}


void ASTSelectQuery::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    frame.current_select = this;
    frame.need_parens = false;
    std::string indent_str = s.one_line ? "" : std::string(4 * frame.indent, ' ');

    if (with())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << indent_str << "WITH " << (s.hilite ? hilite_none : "");
        s.one_line
            ? with()->formatImpl(s, state, frame)
            : with()->as<ASTExpressionList &>().formatImplMultiline(s, state, frame);
        s.ostr << s.nl_or_ws;
    }

    s.ostr << (s.hilite ? hilite_keyword : "") << indent_str << "SELECT " << (distinct ? "DISTINCT " : "") << (s.hilite ? hilite_none : "");

    s.one_line
        ? select()->formatImpl(s, state, frame)
        : select()->as<ASTExpressionList &>().formatImplMultiline(s, state, frame);

    if (tables())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "FROM " << (s.hilite ? hilite_none : "");
        tables()->formatImpl(s, state, frame);
    }

    if (prewhere())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "PREWHERE " << (s.hilite ? hilite_none : "");
        prewhere()->formatImpl(s, state, frame);
    }

    if (where())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "WHERE " << (s.hilite ? hilite_none : "");
        where()->formatImpl(s, state, frame);
    }

    if (groupBy())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "GROUP BY " << (s.hilite ? hilite_none : "");
        s.one_line
            ? groupBy()->formatImpl(s, state, frame)
            : groupBy()->as<ASTExpressionList &>().formatImplMultiline(s, state, frame);
    }

    if (group_by_with_rollup)
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << (s.one_line ? "" : "    ") << "WITH ROLLUP" << (s.hilite ? hilite_none : "");

    if (group_by_with_cube)
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << (s.one_line ? "" : "    ") << "WITH CUBE" << (s.hilite ? hilite_none : "");

    if (group_by_with_totals)
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << (s.one_line ? "" : "    ") << "WITH TOTALS" << (s.hilite ? hilite_none : "");

    if (having())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "HAVING " << (s.hilite ? hilite_none : "");
        having()->formatImpl(s, state, frame);
    }

    if (orderBy())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "ORDER BY " << (s.hilite ? hilite_none : "");
        s.one_line
            ? orderBy()->formatImpl(s, state, frame)
            : orderBy()->as<ASTExpressionList &>().formatImplMultiline(s, state, frame);
    }

    if (limitByLength())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "LIMIT " << (s.hilite ? hilite_none : "");
        if (limitByOffset())
        {
            limitByOffset()->formatImpl(s, state, frame);
            s.ostr << ", ";
        }
        limitByLength()->formatImpl(s, state, frame);
        s.ostr << (s.hilite ? hilite_keyword : "") << " BY " << (s.hilite ? hilite_none : "");
        s.one_line
            ? limitBy()->formatImpl(s, state, frame)
            : limitBy()->as<ASTExpressionList &>().formatImplMultiline(s, state, frame);
    }

    if (limitLength())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "LIMIT " << (s.hilite ? hilite_none : "");
        if (limitOffset())
        {
            limitOffset()->formatImpl(s, state, frame);
            s.ostr << ", ";
        }
        limitLength()->formatImpl(s, state, frame);
    }

    if (settings())
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "SETTINGS " << (s.hilite ? hilite_none : "");
        settings()->formatImpl(s, state, frame);
    }
}


/// Compatibility functions. TODO Remove.


static const ASTTableExpression * getFirstTableExpression(const ASTSelectQuery & select)
{
    if (!select.tables())
        return {};

    const auto & tables_in_select_query = select.tables()->as<ASTTablesInSelectQuery &>();
    if (tables_in_select_query.children.empty())
        return {};

    const auto & tables_element = tables_in_select_query.children[0]->as<ASTTablesInSelectQueryElement &>();
    if (!tables_element.table_expression)
        return {};

    return tables_element.table_expression->as<ASTTableExpression>();
}

static ASTTableExpression * getFirstTableExpression(ASTSelectQuery & select)
{
    if (!select.tables())
        return {};

    auto & tables_in_select_query = select.tables()->as<ASTTablesInSelectQuery &>();
    if (tables_in_select_query.children.empty())
        return {};

    auto & tables_element = tables_in_select_query.children[0]->as<ASTTablesInSelectQueryElement &>();
    if (!tables_element.table_expression)
        return {};

    return tables_element.table_expression->as<ASTTableExpression>();
}

static const ASTArrayJoin * getFirstArrayJoin(const ASTSelectQuery & select)
{
    if (!select.tables())
        return {};

    const auto & tables_in_select_query = select.tables()->as<ASTTablesInSelectQuery &>();
    if (tables_in_select_query.children.empty())
        return {};

    const ASTArrayJoin * array_join = nullptr;
    for (const auto & child : tables_in_select_query.children)
    {
        const auto & tables_element = child->as<ASTTablesInSelectQueryElement &>();
        if (tables_element.array_join)
        {
            if (!array_join)
                array_join = tables_element.array_join->as<ASTArrayJoin>();
            else
                throw Exception("Support for more than one ARRAY JOIN in query is not implemented", ErrorCodes::NOT_IMPLEMENTED);
        }
    }

    return array_join;
}

static const ASTTablesInSelectQueryElement * getFirstTableJoin(const ASTSelectQuery & select)
{
    if (!select.tables())
        return nullptr;

    const auto & tables_in_select_query = select.tables()->as<ASTTablesInSelectQuery &>();
    if (tables_in_select_query.children.empty())
        return nullptr;

    const ASTTablesInSelectQueryElement * joined_table = nullptr;
    for (const auto & child : tables_in_select_query.children)
    {
        const auto & tables_element = child->as<ASTTablesInSelectQueryElement &>();
        if (tables_element.table_join)
        {
            if (!joined_table)
                joined_table = &tables_element;
            else
                throw Exception("Multiple JOIN disabled or does not support the query. "
                                "'set allow_experimental_multiple_joins_emulation' to enable.", ErrorCodes::NOT_IMPLEMENTED);
        }
    }

    return joined_table;
}


ASTPtr ASTSelectQuery::sample_size() const
{
    const ASTTableExpression * table_expression = getFirstTableExpression(*this);
    if (!table_expression)
        return {};

    return table_expression->sample_size;
}


ASTPtr ASTSelectQuery::sample_offset() const
{
    const ASTTableExpression * table_expression = getFirstTableExpression(*this);
    if (!table_expression)
        return {};

    return table_expression->sample_offset;
}


bool ASTSelectQuery::final() const
{
    const ASTTableExpression * table_expression = getFirstTableExpression(*this);
    if (!table_expression)
        return {};

    return table_expression->final;
}


ASTPtr ASTSelectQuery::array_join_expression_list(bool & is_left) const
{
    const ASTArrayJoin * array_join = getFirstArrayJoin(*this);
    if (!array_join)
        return {};

    is_left = (array_join->kind == ASTArrayJoin::Kind::Left);
    return array_join->expression_list;
}


ASTPtr ASTSelectQuery::array_join_expression_list() const
{
    bool is_left;
    return array_join_expression_list(is_left);
}


const ASTTablesInSelectQueryElement * ASTSelectQuery::join() const
{
    return getFirstTableJoin(*this);
}

static String getTableExpressionAlias(const ASTTableExpression * table_expression)
{
    if (table_expression->subquery)
        return table_expression->subquery->tryGetAlias();
    else if (table_expression->table_function)
        return table_expression->table_function->tryGetAlias();
    else if (table_expression->database_and_table_name)
        return table_expression->database_and_table_name->tryGetAlias();

    return String();
}

void ASTSelectQuery::replaceDatabaseAndTable(const String & database_name, const String & table_name)
{
    ASTTableExpression * table_expression = getFirstTableExpression(*this);

    if (!table_expression)
    {
        setExpression(Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
        auto element = std::make_shared<ASTTablesInSelectQueryElement>();
        auto table_expr = std::make_shared<ASTTableExpression>();
        element->table_expression = table_expr;
        element->children.emplace_back(table_expr);
        tables()->children.emplace_back(element);
        table_expression = table_expr.get();
    }

    String table_alias = getTableExpressionAlias(table_expression);
    table_expression->database_and_table_name = createTableIdentifier(database_name, table_name);

    if (!table_alias.empty())
        table_expression->database_and_table_name->setAlias(table_alias);
}


void ASTSelectQuery::addTableFunction(ASTPtr & table_function_ptr)
{
    ASTTableExpression * table_expression = getFirstTableExpression(*this);

    if (!table_expression)
    {
        setExpression(Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
        auto element = std::make_shared<ASTTablesInSelectQueryElement>();
        auto table_expr = std::make_shared<ASTTableExpression>();
        element->table_expression = table_expr;
        element->children.emplace_back(table_expr);
        tables()->children.emplace_back(element);
        table_expression = table_expr.get();
    }

    String table_alias = getTableExpressionAlias(table_expression);
    /// Maybe need to modify the alias, so we should clone new table_function node
    table_expression->table_function = table_function_ptr->clone();
    table_expression->database_and_table_name = nullptr;

    if (table_alias.empty())
        table_expression->table_function->setAlias(table_alias);
}

void ASTSelectQuery::setExpression(Expression expr, ASTPtr && ast)
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

ASTPtr & ASTSelectQuery::getExpression(Expression expr)
{
    if (!positions.count(expr))
        throw Exception("Get expression before set", ErrorCodes::LOGICAL_ERROR);
    return children[positions[expr]];
}

}
