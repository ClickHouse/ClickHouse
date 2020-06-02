#include "parseQuery.h"

#include <Parsers/ASTSelectWithUnionQuery.h>

namespace DB
{

antlrcpp::Any ParserTreeVisitor::visitQueryList(ClickHouseParser::QueryListContext *ctx)
{
    std::vector<ASTPtr> query_list;

    for (auto * query : ctx->queryStmt())
    {
        query_list.push_back(query->accept(ctx));
    }

    return query_list;
}

antlrcpp::Any ParserTreeVisitor::visitSelectUnionStmt(ClickHouseParser::SelectUnionStmtContext *ctx)
{
    ASTSelectWithUnionQuery select_with_union_query;
    select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();

    // TODO!
}

}
