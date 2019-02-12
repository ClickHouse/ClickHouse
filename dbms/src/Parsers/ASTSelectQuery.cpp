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
    extern const int NOT_IMPLEMENTED;
}


ASTPtr ASTSelectQuery::clone() const
{
    auto res = std::make_shared<ASTSelectQuery>(*this);
    res->children.clear();

#define CLONE(member) if (member) { res->member = member->clone(); res->children.push_back(res->member); }

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
    CLONE(with_expression_list)
    CLONE(select_expression_list)
    CLONE(tables)
    CLONE(prewhere_expression)
    CLONE(where_expression)
    CLONE(group_expression_list)
    CLONE(having_expression)
    CLONE(order_expression_list)
    CLONE(limit_by_value)
    CLONE(limit_by_expression_list)
    CLONE(limit_offset)
    CLONE(limit_length)
    CLONE(settings)

#undef CLONE

    return res;
}


void ASTSelectQuery::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    frame.current_select = this;
    frame.need_parens = false;
    std::string indent_str = s.one_line ? "" : std::string(4 * frame.indent, ' ');

    if (with_expression_list)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << indent_str << "WITH " << (s.hilite ? hilite_none : "");
        s.one_line
            ? with_expression_list->formatImpl(s, state, frame)
            : typeid_cast<const ASTExpressionList &>(*with_expression_list).formatImplMultiline(s, state, frame);
        s.ostr << s.nl_or_ws;
    }

    s.ostr << (s.hilite ? hilite_keyword : "") << indent_str << "SELECT " << (distinct ? "DISTINCT " : "") << (s.hilite ? hilite_none : "");

    s.one_line
        ? select_expression_list->formatImpl(s, state, frame)
        : typeid_cast<const ASTExpressionList &>(*select_expression_list).formatImplMultiline(s, state, frame);

    if (tables)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "FROM " << (s.hilite ? hilite_none : "");
        tables->formatImpl(s, state, frame);
    }

    if (prewhere_expression)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "PREWHERE " << (s.hilite ? hilite_none : "");
        prewhere_expression->formatImpl(s, state, frame);
    }

    if (where_expression)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "WHERE " << (s.hilite ? hilite_none : "");
        where_expression->formatImpl(s, state, frame);
    }

    if (group_expression_list)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "GROUP BY " << (s.hilite ? hilite_none : "");
        s.one_line
            ? group_expression_list->formatImpl(s, state, frame)
            : typeid_cast<const ASTExpressionList &>(*group_expression_list).formatImplMultiline(s, state, frame);
    }

    if (group_by_with_rollup)
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << (s.one_line ? "" : "    ") << "WITH ROLLUP" << (s.hilite ? hilite_none : "");

    if (group_by_with_cube)
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << (s.one_line ? "" : "    ") << "WITH CUBE" << (s.hilite ? hilite_none : "");

    if (group_by_with_totals)
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << (s.one_line ? "" : "    ") << "WITH TOTALS" << (s.hilite ? hilite_none : "");

    if (having_expression)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "HAVING " << (s.hilite ? hilite_none : "");
        having_expression->formatImpl(s, state, frame);
    }

    if (order_expression_list)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "ORDER BY " << (s.hilite ? hilite_none : "");
        s.one_line
            ? order_expression_list->formatImpl(s, state, frame)
            : typeid_cast<const ASTExpressionList &>(*order_expression_list).formatImplMultiline(s, state, frame);
    }

    if (limit_by_value)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "LIMIT " << (s.hilite ? hilite_none : "");
        limit_by_value->formatImpl(s, state, frame);
        s.ostr << (s.hilite ? hilite_keyword : "") << " BY " << (s.hilite ? hilite_none : "");
        s.one_line
            ? limit_by_expression_list->formatImpl(s, state, frame)
            : typeid_cast<const ASTExpressionList &>(*limit_by_expression_list).formatImplMultiline(s, state, frame);
    }

    if (limit_length)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "LIMIT " << (s.hilite ? hilite_none : "");
        if (limit_offset)
        {
            limit_offset->formatImpl(s, state, frame);
            s.ostr << ", ";
        }
        limit_length->formatImpl(s, state, frame);
    }

    if (settings)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << indent_str << "SETTINGS " << (s.hilite ? hilite_none : "");
        settings->formatImpl(s, state, frame);
    }
}


/// Compatibility functions. TODO Remove.


static const ASTTableExpression * getFirstTableExpression(const ASTSelectQuery & select)
{
    if (!select.tables)
        return {};

    const ASTTablesInSelectQuery & tables_in_select_query = static_cast<const ASTTablesInSelectQuery &>(*select.tables);
    if (tables_in_select_query.children.empty())
        return {};

    const ASTTablesInSelectQueryElement & tables_element = static_cast<const ASTTablesInSelectQueryElement &>(*tables_in_select_query.children[0]);
    if (!tables_element.table_expression)
        return {};

    return static_cast<const ASTTableExpression *>(tables_element.table_expression.get());
}

static ASTTableExpression * getFirstTableExpression(ASTSelectQuery & select)
{
    if (!select.tables)
        return {};

    ASTTablesInSelectQuery & tables_in_select_query = static_cast<ASTTablesInSelectQuery &>(*select.tables);
    if (tables_in_select_query.children.empty())
        return {};

    ASTTablesInSelectQueryElement & tables_element = static_cast<ASTTablesInSelectQueryElement &>(*tables_in_select_query.children[0]);
    if (!tables_element.table_expression)
        return {};

    return static_cast<ASTTableExpression *>(tables_element.table_expression.get());
}

static const ASTArrayJoin * getFirstArrayJoin(const ASTSelectQuery & select)
{
    if (!select.tables)
        return {};

    const ASTTablesInSelectQuery & tables_in_select_query = static_cast<const ASTTablesInSelectQuery &>(*select.tables);
    if (tables_in_select_query.children.empty())
        return {};

    const ASTArrayJoin * array_join = nullptr;
    for (const auto & child : tables_in_select_query.children)
    {
        const ASTTablesInSelectQueryElement & tables_element = static_cast<const ASTTablesInSelectQueryElement &>(*child);
        if (tables_element.array_join)
        {
            if (!array_join)
                array_join = static_cast<const ASTArrayJoin *>(tables_element.array_join.get());
            else
                throw Exception("Support for more than one ARRAY JOIN in query is not implemented", ErrorCodes::NOT_IMPLEMENTED);
        }
    }

    return array_join;
}

static const ASTTablesInSelectQueryElement * getFirstTableJoin(const ASTSelectQuery & select)
{
    if (!select.tables)
        return {};

    const ASTTablesInSelectQuery & tables_in_select_query = static_cast<const ASTTablesInSelectQuery &>(*select.tables);
    if (tables_in_select_query.children.empty())
        return {};

    const ASTTablesInSelectQueryElement * joined_table = nullptr;
    for (const auto & child : tables_in_select_query.children)
    {
        const ASTTablesInSelectQueryElement & tables_element = static_cast<const ASTTablesInSelectQueryElement &>(*child);
        if (tables_element.table_join)
        {
            if (!joined_table)
                joined_table = &tables_element;
            else
                throw Exception("Support for more than one JOIN in query is not implemented", ErrorCodes::NOT_IMPLEMENTED);
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
        auto tables_list = std::make_shared<ASTTablesInSelectQuery>();
        auto element = std::make_shared<ASTTablesInSelectQueryElement>();
        auto table_expr = std::make_shared<ASTTableExpression>();
        element->table_expression = table_expr;
        element->children.emplace_back(table_expr);
        tables_list->children.emplace_back(element);
        tables = tables_list;
        children.emplace_back(tables_list);
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
        auto tables_list = std::make_shared<ASTTablesInSelectQuery>();
        auto element = std::make_shared<ASTTablesInSelectQueryElement>();
        auto table_expr = std::make_shared<ASTTableExpression>();
        element->table_expression = table_expr;
        element->children.emplace_back(table_expr);
        tables_list->children.emplace_back(element);
        tables = tables_list;
        children.emplace_back(tables_list);
        table_expression = table_expr.get();
    }

    String table_alias = getTableExpressionAlias(table_expression);
    /// Maybe need to modify the alias, so we should clone new table_function node
    table_expression->table_function = table_function_ptr->clone();
    table_expression->database_and_table_name = nullptr;

    if (table_alias.empty())
        table_expression->table_function->setAlias(table_alias);
}

}

