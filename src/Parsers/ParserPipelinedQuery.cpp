#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserPipelinedQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Common/StackTrace.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{
bool ParserPipelinedQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!allow_pipe_syntax)
    {
        // LOG_ERROR(getLogger("ParserPipelinedQuery"), "ParserPipelinedQuery allow_pipe_syntax is false {}", StackTrace().toString());
        LOG_ERROR(getLogger("ParserPipelinedQuery"), "ParserPipelinedQuery allow_pipe_syntax is false");
        return false;
    }

    ParserKeyword s_from(Keyword::FROM);
    if (!s_from.ignore(pos, expected))
        return false;

    ASTPtr tables;
    if (!ParserTableExpression().parse(pos, tables, expected))
        return false;

    auto select_query = make_intrusive<ASTSelectQuery>();

    auto select_expr_list = make_intrusive<ASTExpressionList>();
    select_expr_list->children.push_back(make_intrusive<ASTAsterisk>());
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expr_list));

    auto tables_in_select = make_intrusive<ASTTablesInSelectQuery>();
    auto tables_element = make_intrusive<ASTTablesInSelectQueryElement>();

    tables_element->table_expression = tables;
    tables_element->children.push_back(tables);

    tables_in_select->children.push_back(tables_element);

    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables_in_select));

    auto list_of_selects = make_intrusive<ASTExpressionList>();
    list_of_selects->children.push_back(select_query);

    auto union_query = make_intrusive<ASTSelectWithUnionQuery>();
    union_query->list_of_selects = list_of_selects;
    union_query->children.push_back(list_of_selects);

    node = std::move(union_query);
    return true;
}

}
