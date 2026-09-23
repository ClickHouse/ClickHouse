#include <Parsers/Prometheus/ParserPrometheusQuery.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/Access/ParserSetRoleQuery.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <base/find_symbols.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SETTING_VALUE;
    extern const int SYNTAX_ERROR;
}

namespace
{

/// Returns the position of the `;` ending the PromQL statement in [begin, end), or `end`.
/// The raw text is scanned with the PromQL lexical rules (see `PromQLLexer.g4`), because the SQL lexer
/// does not know that `#` starts a comment unless a space follows it, so a `;` in `up #keep ; x`
/// would read as the statement end. A `;` inside a string literal doesn't end the statement either.
const char * findEndOfPromQLStatement(const char * begin, const char * end)
{
    const char * p = begin;
    while (p < end)
    {
        const char c = *p;
        if (c == ';')
            return p;

        if (c == '#')
        {
            p = find_first_symbols<'\n'>(p, end);
        }
        else if (c == '"' || c == '\'' || c == '`')
        {
            /// Backquoted strings are raw, the others have backslash escapes.
            ++p;
            while (p < end && *p != c)
            {
                if (*p == '\\' && c != '`' && p + 1 < end)
                    ++p;
                ++p;
            }
            p = std::min(p + 1, end);
        }
        else
        {
            ++p;
        }
    }
    return end;
}

}


ParserPrometheusQuery::ParserPrometheusQuery(const String & database_name_, const String & table_name_, const Field & evaluation_time_)
    : database_name(database_name_), table_name(table_name_), evaluation_time(evaluation_time_)
{
}


bool ParserPrometheusQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// The `SET <setting>` shorthand would swallow PromQL queries over a metric named `set`
    /// (e.g. `set or up`), so SET is parsed only when the input unambiguously starts one.
    if (isCommittedToSetQuery(pos))
    {
        /// SET ROLE / SET DEFAULT ROLE are role statements: ParserSetQuery would take the leading
        /// ROLE / DEFAULT as a setting-name shorthand, so they go first, as in ParserQuery.
        ParserSetRoleQuery set_role_p;
        if (set_role_p.parse(pos, node, expected))
            return true;
        ParserSetQuery set_p;
        return set_p.parse(pos, node, expected);
    }

    if (table_name.empty())
    {
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                        "The name of a TimeSeries table to use with promql dialect is not specified, use: SET promql_table = '...'");
    }

    const auto * begin = pos->begin;

    // The same parsers are used in the client and the server, so the parser have to detect the end of a single query in case of multiquery queries
    const char * text_end = begin;
    for (Pos lookahead = pos; ; ++lookahead)
    {
        if (lookahead->isEnd())
        {
            text_end = lookahead->begin;
            break;
        }
    }

    const auto * end = findEndOfPromQLStatement(begin, text_end);

    /// Move to the SQL token at the statement end. The SQL tokens of a PromQL comment or string can
    /// differ from the PromQL ones, e.g. an apostrophe in a comment opens a SQL string literal which
    /// may run past the end. The position is then ambiguous, so fail instead of guessing.
    while (!pos->isEnd() && pos->end <= end)
        ++pos;

    if (pos->begin != end || !(pos->isEnd() || pos->type == TokenType::Semicolon))
        throw Exception(ErrorCodes::SYNTAX_ERROR,
                        "Cannot find the end of the PromQL statement: a comment or a string literal in it confuses the SQL lexer");

    /// We call PrometheusQueryTree here to check for syntax errors earlier.
    PrometheusQueryTree promql_query{std::string_view{begin, end}};

    /// Build a query.
    auto select_query = make_intrusive<ASTSelectQuery>();

    auto select_list_exp = make_intrusive<ASTExpressionList>();
    select_list_exp->children.push_back(make_intrusive<ASTAsterisk>());
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);

    ASTs arguments;
    if (!database_name.empty())
        arguments.push_back(make_intrusive<ASTLiteral>(Field{database_name}));
    arguments.push_back(make_intrusive<ASTLiteral>(Field{table_name}));
    arguments.push_back(make_intrusive<ASTLiteral>(Field{promql_query.toString()}));
    ASTPtr evaluation_time_ast;
    if (evaluation_time == Field("auto"))
        evaluation_time_ast = makeASTFunction("now");
    else
        evaluation_time_ast = make_intrusive<ASTLiteral>(evaluation_time);
    arguments.push_back(evaluation_time_ast);
    auto table_function = makeASTFunction("prometheusQuery", std::move(arguments));

    auto tables = make_intrusive<ASTTablesInSelectQuery>();
    auto table = make_intrusive<ASTTablesInSelectQueryElement>();
    auto table_exp = make_intrusive<ASTTableExpression>();
    table_exp->table_function = table_function;
    table_exp->children.emplace_back(table_exp->table_function);
    table->table_expression = table_exp;
    /// AST visitors traverse children, not members, so the table expression must be linked as a child too.
    table->children.push_back(table->table_expression);
    tables->children.push_back(table);
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);

    auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();
    auto list_of_selects = make_intrusive<ASTExpressionList>();
    list_of_selects->children.push_back(std::move(select_query));
    select_with_union_query->list_of_selects = list_of_selects;
    select_with_union_query->children.push_back(list_of_selects);

    node = select_with_union_query;
    return true;
}

}
