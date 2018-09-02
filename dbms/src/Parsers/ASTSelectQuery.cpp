#include <Common/FieldVisitors.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Common/typeid_cast.h>


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


ASTPtr ASTSelectQuery::database() const
{
    const ASTTableExpression * table_expression = getFirstTableExpression(*this);
    if (!table_expression || !table_expression->database_and_table_name || table_expression->database_and_table_name->children.empty())
        return {};

    if (table_expression->database_and_table_name->children.size() != 2)
        throw Exception("Logical error: more than two components in table expression", ErrorCodes::LOGICAL_ERROR);

    return table_expression->database_and_table_name->children[0];
}


ASTPtr ASTSelectQuery::table() const
{
    const ASTTableExpression * table_expression = getFirstTableExpression(*this);
    if (!table_expression)
        return {};

    if (table_expression->database_and_table_name)
    {
        if (table_expression->database_and_table_name->children.empty())
            return table_expression->database_and_table_name;

        if (table_expression->database_and_table_name->children.size() != 2)
            throw Exception("Logical error: more than two components in table expression", ErrorCodes::LOGICAL_ERROR);

        return table_expression->database_and_table_name->children[1];
    }

    if (table_expression->table_function)
        return table_expression->table_function;

    if (table_expression->subquery)
        return static_cast<const ASTSubquery *>(table_expression->subquery.get())->children.at(0);

    throw Exception("Logical error: incorrect table expression", ErrorCodes::LOGICAL_ERROR);
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


ASTPtr ASTSelectQuery::array_join_expression_list() const
{
    const ASTArrayJoin * array_join = getFirstArrayJoin(*this);
    if (!array_join)
        return {};

    return array_join->expression_list;
}


bool ASTSelectQuery::array_join_is_left() const
{
    const ASTArrayJoin * array_join = getFirstArrayJoin(*this);
    if (!array_join)
        return {};

    return array_join->kind == ASTArrayJoin::Kind::Left;
}


const ASTTablesInSelectQueryElement * ASTSelectQuery::join() const
{
    return getFirstTableJoin(*this);
}


void ASTSelectQuery::setDatabaseIfNeeded(const String & database_name)
{
    ASTTableExpression * table_expression = getFirstTableExpression(*this);
    if (!table_expression)
        return;

    if (!table_expression->database_and_table_name)
        return;

    if (table_expression->database_and_table_name->children.empty())
    {
        ASTPtr database = std::make_shared<ASTIdentifier>(database_name, ASTIdentifier::Database);
        ASTPtr table = table_expression->database_and_table_name;

        const String & old_name = static_cast<ASTIdentifier &>(*table_expression->database_and_table_name).name;
        table_expression->database_and_table_name = std::make_shared<ASTIdentifier>(database_name + "." + old_name, ASTIdentifier::Table);
        table_expression->database_and_table_name->children = {database, table};
    }
    else if (table_expression->database_and_table_name->children.size() != 2)
    {
        throw Exception("Logical error: more than two components in table expression", ErrorCodes::LOGICAL_ERROR);
    }
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

    ASTPtr table = std::make_shared<ASTIdentifier>(table_name, ASTIdentifier::Table);

    if (!database_name.empty())
    {
        ASTPtr database = std::make_shared<ASTIdentifier>(database_name, ASTIdentifier::Database);

        table_expression->database_and_table_name = std::make_shared<ASTIdentifier>(database_name + "." + table_name, ASTIdentifier::Table);
        table_expression->database_and_table_name->children = {database, table};
    }
    else
    {
        table_expression->database_and_table_name = std::make_shared<ASTIdentifier>(table_name, ASTIdentifier::Table);
    }
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

    table_expression->table_function = table_function_ptr;
    table_expression->database_and_table_name = nullptr;
}

}

