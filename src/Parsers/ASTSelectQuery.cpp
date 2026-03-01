#include <Common/typeid_cast.h>
#include <Common/SipHash.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Interpreters/StorageID.h>
#include <IO/Operators.h>
#include <Parsers/QueryParameterVisitor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}


ASTPtr ASTSelectQuery::clone() const
{
    auto res = make_intrusive<ASTSelectQuery>(*this);

    /** NOTE Members must clone exactly in the same order in which they were inserted into `children` in ParserSelectQuery.
     * This is important because the AST hash depends on the children order and this hash is used for multiple things,
     * like the column identifiers in the case of subqueries in the IN statement or caching scalar queries (reused in CTEs so it's
     * important for them to have the same hash).
     * For distributed query processing, in case one of the servers is localhost and the other one is not, localhost query is executed
     * within the process and is cloned, and the request is sent to the remote server in text form via TCP.
     * And if the cloning order does not match the parsing order then different servers will get different identifiers.
     *
     * Since the positions map uses <key, position> we can copy it as is and ensure the new children array is created / pushed
     * in the same order as the existing one */
    res->children.clear();
    for (const auto & child : children)
        res->children.push_back(child->clone());

    return res;
}


void ASTSelectQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(recursive_with);
    hash_state.update(distinct);
    hash_state.update(group_by_with_totals);
    hash_state.update(group_by_with_rollup);
    hash_state.update(group_by_with_cube);
    hash_state.update(limit_with_ties);
    hash_state.update(limit_by_all);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}


void ASTSelectQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    frame.current_select = this;
    frame.need_parens = false;
    frame.expression_list_prepend_whitespace = true;

    std::string indent_str = s.one_line ? "" : std::string(4 * frame.indent, ' ');

    if (with())
    {
        ostr << indent_str << "WITH";

        if (recursive_with)
            ostr << " RECURSIVE";

        s.one_line
            ? with()->format(ostr, s, state, frame)
            : with()->as<ASTExpressionList &>().formatImplMultiline(ostr, s, state, frame);
        ostr << s.nl_or_ws;
    }

    ostr << indent_str << "SELECT" << (distinct ? " DISTINCT" : "");

    s.one_line
        ? select()->format(ostr, s, state, frame)
        : select()->as<ASTExpressionList &>().formatImplMultiline(ostr, s, state, frame);

    if (tables())
    {
        ostr << s.nl_or_ws << indent_str << "FROM";
        tables()->format(ostr, s, state, frame);
    }

    if (aliases())
    {
        const bool prep_whitespace = frame.expression_list_prepend_whitespace;
        frame.expression_list_prepend_whitespace = false;

        ostr << indent_str << " (";
        aliases()->format(ostr, s, state, frame);
        ostr << indent_str << ")";

        frame.expression_list_prepend_whitespace = prep_whitespace;
    }

    if (prewhere())
    {
        ostr << s.nl_or_ws << indent_str << "PREWHERE ";
        prewhere()->format(ostr, s, state, frame);
    }

    if (where())
    {
        ostr << s.nl_or_ws << indent_str << "WHERE ";
        where()->format(ostr, s, state, frame);
    }

    if (!group_by_all && groupBy())
    {
        ostr << s.nl_or_ws << indent_str << "GROUP BY";
        if (!group_by_with_grouping_sets)
        {
            s.one_line
            ? groupBy()->format(ostr, s, state, frame)
            : groupBy()->as<ASTExpressionList &>().formatImplMultiline(ostr, s, state, frame);
        }
    }

    if (group_by_all)
        ostr << s.nl_or_ws << indent_str << "GROUP BY ALL";

    if (group_by_with_grouping_sets && groupBy())
    {
        auto nested_frame = frame;
        nested_frame.surround_each_list_element_with_parens = true;
        nested_frame.expression_list_prepend_whitespace = false;
        nested_frame.indent++;
        ostr << s.nl_or_ws << indent_str << (s.one_line ? "" : "    ") << "GROUPING SETS";
        ostr << " (";
        s.one_line
        ? groupBy()->format(ostr, s, state, nested_frame)
        : groupBy()->as<ASTExpressionList &>().formatImplMultiline(ostr, s, state, nested_frame);
        ostr << ")";
    }

    if (group_by_with_rollup)
        ostr << s.nl_or_ws << indent_str << (s.one_line ? "" : "    ") << "WITH ROLLUP";

    if (group_by_with_cube)
        ostr << s.nl_or_ws << indent_str << (s.one_line ? "" : "    ") << "WITH CUBE";

    if (group_by_with_totals)
        ostr << s.nl_or_ws << indent_str << (s.one_line ? "" : "    ") << "WITH TOTALS";

    if (having())
    {
        ostr << s.nl_or_ws << indent_str << "HAVING ";
        having()->format(ostr, s, state, frame);
    }

    if (window())
    {
        ostr << s.nl_or_ws << indent_str <<
            "WINDOW";
        s.one_line
            ? window()->format(ostr, s, state, frame)
            : window()->as<ASTExpressionList &>().formatImplMultiline(ostr, s, state, frame);
    }

    if (qualify())
    {
        ostr << s.nl_or_ws << indent_str << "QUALIFY ";
        qualify()->format(ostr, s, state, frame);
    }

    if (!order_by_all && orderBy())
    {
        ostr << s.nl_or_ws << indent_str << "ORDER BY";
        s.one_line
            ? orderBy()->format(ostr, s, state, frame)
            : orderBy()->as<ASTExpressionList &>().formatImplMultiline(ostr, s, state, frame);

        if (interpolate())
        {
            ostr << s.nl_or_ws << indent_str << "INTERPOLATE";
            if (!interpolate()->children.empty())
            {
                auto nested_frame = frame;
                nested_frame.expression_list_prepend_whitespace = false;

                ostr << " (";
                interpolate()->format(ostr, s, state, nested_frame);
                ostr << ")";
            }
        }
    }

    if (order_by_all)
    {
        ostr << s.nl_or_ws << indent_str << "ORDER BY ALL";

        auto * elem = orderBy()->children[0]->as<ASTOrderByElement>();
        ostr << (elem->direction == -1 ? " DESC" : " ASC");

        if (elem->nulls_direction_was_explicitly_specified)
        {
            ostr << " NULLS " << (elem->nulls_direction == elem->direction ? "LAST" : "FIRST");
        }
    }

    if (limitByLength())
    {
        ostr << s.nl_or_ws << indent_str << "LIMIT ";
        if (limitByOffset())
        {
            limitByOffset()->format(ostr, s, state, frame);
            ostr << ", ";
        }
        limitByLength()->format(ostr, s, state, frame);
        ostr << " BY";
        if (limit_by_all)
        {
            ostr << " ALL";
        }
        else if (limitBy())
        {
            s.one_line ? limitBy()->format(ostr, s, state, frame)
                       : limitBy()->as<ASTExpressionList &>().formatImplMultiline(ostr, s, state, frame);
        }
    }

    if (limitLength())
    {
        ostr << s.nl_or_ws << indent_str << "LIMIT ";
        if (limitOffset())
        {
            limitOffset()->format(ostr, s, state, frame);
            ostr << ", ";
        }
        limitLength()->format(ostr, s, state, frame);
        if (limit_with_ties)
            ostr << s.nl_or_ws << indent_str << " WITH TIES";
    }
    else if (limitOffset())
    {
        ostr << s.nl_or_ws << indent_str << "OFFSET ";
        limitOffset()->format(ostr, s, state, frame);
    }

    if (settings())
    {
        ostr << s.nl_or_ws << indent_str << "SETTINGS ";
        settings()->format(ostr, s, state, frame);
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
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Support for more than one ARRAY JOIN in query is not implemented");
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
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Multiple JOIN does not support the query.");
        }
    }

    return joined_table;
}


ASTPtr ASTSelectQuery::sampleSize() const
{
    const ASTTableExpression * table_expression = getFirstTableExpression(*this);
    if (!table_expression)
        return {};

    return table_expression->sample_size;
}


ASTPtr ASTSelectQuery::sampleOffset() const
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

bool ASTSelectQuery::withFill() const
{
    const ASTPtr order_by = orderBy();
    if (!order_by)
        return false;

    for (const auto & order_expression_element : order_by->children)
        if (order_expression_element->as<ASTOrderByElement &>().with_fill)
            return true;

    return false;
}


std::pair<ASTPtr, bool> ASTSelectQuery::arrayJoinExpressionList() const
{
    const ASTArrayJoin * array_join = getFirstArrayJoin(*this);
    if (!array_join)
        return {};

    bool is_left = (array_join->kind == ASTArrayJoin::Kind::Left);
    return {array_join->expression_list, is_left};
}

const ASTTablesInSelectQueryElement * ASTSelectQuery::join() const
{
    return getFirstTableJoin(*this);
}

bool ASTSelectQuery::hasJoin() const
{
    if (!tables())
        return false;

    const auto & tables_in_select_query = tables()->as<ASTTablesInSelectQuery &>();
    if (tables_in_select_query.children.empty())
        return false;

    for (const auto & child : tables_in_select_query.children)
    {
        const auto & tables_element = child->as<ASTTablesInSelectQueryElement &>();
        if (tables_element.table_join)
            return true;
    }

    return false;
}

static String getTableExpressionAlias(const ASTTableExpression * table_expression)
{
    if (table_expression->subquery)
        return table_expression->subquery->tryGetAlias();
    if (table_expression->table_function)
        return table_expression->table_function->tryGetAlias();
    if (table_expression->database_and_table_name)
        return table_expression->database_and_table_name->tryGetAlias();

    return String();
}

void ASTSelectQuery::replaceDatabaseAndTable(const String & database_name, const String & table_name)
{
    assert(database_name != "_temporary_and_external_tables");
    replaceDatabaseAndTable(StorageID(database_name, table_name));
}

void ASTSelectQuery::replaceDatabaseAndTable(const StorageID & table_id)
{
    ASTTableExpression * table_expression = getFirstTableExpression(*this);

    if (!table_expression)
    {
        setExpression(Expression::TABLES, make_intrusive<ASTTablesInSelectQuery>());
        auto element = make_intrusive<ASTTablesInSelectQueryElement>();
        auto table_expr = make_intrusive<ASTTableExpression>();
        element->table_expression = table_expr;
        element->children.emplace_back(table_expr);
        tables()->children.emplace_back(element);
        table_expression = table_expr.get();
    }

    String table_alias = getTableExpressionAlias(table_expression);
    table_expression->database_and_table_name = make_intrusive<ASTTableIdentifier>(table_id);

    if (!table_alias.empty())
        table_expression->database_and_table_name->setAlias(table_alias);
}


void ASTSelectQuery::addTableFunction(const ASTPtr & table_function_ptr)
{
    ASTTableExpression * table_expression = getFirstTableExpression(*this);

    if (!table_expression)
    {
        setExpression(Expression::TABLES, make_intrusive<ASTTablesInSelectQuery>());
        auto element = make_intrusive<ASTTablesInSelectQueryElement>();
        auto table_expr = make_intrusive<ASTTableExpression>();
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

ASTPtr & ASTSelectQuery::getExpression(Expression expr)
{
    if (!positions.contains(expr))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Get expression before set");
    return children[positions[expr]];
}

void ASTSelectQuery::setFinal() // NOLINT method can be made const
{
    auto & tables_in_select_query = tables()->as<ASTTablesInSelectQuery &>();

    if (tables_in_select_query.children.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Tables list is empty, it's a bug");

    auto & tables_element = tables_in_select_query.children[0]->as<ASTTablesInSelectQueryElement &>();

    if (!tables_element.table_expression)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no table expression, it's a bug");

    tables_element.table_expression->as<ASTTableExpression &>().final = true;
}

bool ASTSelectQuery::hasQueryParameters() const
{
    if (!has_query_parameters.has_value())
    {
        has_query_parameters = !analyzeReceiveQueryParams(make_intrusive<ASTSelectQuery>(*this)).empty();
    }

    return  has_query_parameters.value();
}

NameToNameMap ASTSelectQuery::getQueryParameters() const
{
    if (!hasQueryParameters())
        return {};

    return analyzeReceiveQueryParamsWithType(make_intrusive<ASTSelectQuery>(*this));
}

}
