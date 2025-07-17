#include <Parsers/Prometheus/ParserPrometheusQuery.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SETTING_VALUE;
}


ParserPrometheusQuery::ParserPrometheusQuery(const String & database_name_, const String & table_name_, Float64 evaluation_time_)
    : database_name(database_name_), table_name(table_name_), evaluation_time(evaluation_time_)
{
}


bool ParserPrometheusQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserSetQuery set_p;

    if (set_p.parse(pos, node, expected))
        return true;

    if (table_name.empty())
    {
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
                        "The name of a TimeSeries table to use with promql dialect is not specified, use: SET promql_table_name = '...'");
    }

    const auto * begin = pos->begin;

    // The same parsers are used in the client and the server, so the parser have to detect the end of a single query in case of multiquery queries
    while (!pos->isEnd() && pos->type != TokenType::Semicolon)
        ++pos;

    const auto * end = pos->begin;

    /// We call PrometheusQueryTree here to check for syntax errors earlier.
    PrometheusQueryTree promql_query{std::string_view{begin, end}};

    /// Build a query.
    auto select_query = std::make_shared<ASTSelectQuery>();

    auto select_list_exp = std::make_shared<ASTExpressionList>();
    select_list_exp->children.push_back(std::make_shared<ASTAsterisk>());
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);

    ASTs arguments;
    if (!database_name.empty())
        arguments.push_back(std::make_shared<ASTLiteral>(Field{database_name}));
    arguments.push_back(std::make_shared<ASTLiteral>(Field{table_name}));
    arguments.push_back(std::make_shared<ASTLiteral>(Field{promql_query.toString()}));
    ASTPtr evaluation_time_ast;
    if (evaluation_time != 0)
        evaluation_time_ast = std::make_shared<ASTLiteral>(Field{evaluation_time});
    else
        evaluation_time_ast = makeASTFunction("now");
    arguments.push_back(evaluation_time_ast);
    auto table_function = makeASTFunction("prometheusQuery", std::move(arguments));

    auto tables = std::make_shared<ASTTablesInSelectQuery>();
    auto table = std::make_shared<ASTTablesInSelectQueryElement>();
    auto table_exp = std::make_shared<ASTTableExpression>();
    table_exp->table_function = table_function;
    table_exp->children.emplace_back(table_exp->table_function);
    table->table_expression = table_exp;
    tables->children.push_back(table);
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);

    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    auto list_of_selects = std::make_shared<ASTExpressionList>();
    list_of_selects->children.push_back(std::move(select_query));
    select_with_union_query->list_of_selects = list_of_selects;
    select_with_union_query->children.push_back(list_of_selects);

    node = select_with_union_query;
    return true;
}

}
