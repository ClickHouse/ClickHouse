#include <Parsers/New/ParseTreeVisitor.h>

#include <Parsers/New/AST/CreateDatabaseQuery.h>
#include <Parsers/New/AST/CreateTableQuery.h>
#include <Parsers/New/AST/DDLQuery.h>
#include <Parsers/New/AST/DropQuery.h>
#include <Parsers/New/AST/EngineExpr.h>
#include <Parsers/New/AST/InsertQuery.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/AST/OptimizeQuery.h>
#include <Parsers/New/AST/SelectStmt.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/AST/SetQuery.h>
#include <Parsers/New/AST/UseQuery.h>


namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitQueryList(ClickHouseParser::QueryListContext *ctx)
{
    auto query_list = std::make_shared<QueryList>();

    for (auto * query : ctx->queryStmt())
        query_list->append(query->accept(this));

    return query_list;
}

antlrcpp::Any ParseTreeVisitor::visitQueryStmt(ClickHouseParser::QueryStmtContext *ctx)
{
    auto query = ctx->query()->accept(this).as<PtrTo<Query>>();

    if (ctx->OUTFILE()) query->setOutFile(Literal::createString(ctx->STRING_LITERAL()));
    if (ctx->FORMAT()) query->setFormat(ctx->identifier()->accept(this));

    return query;
}

antlrcpp::Any ParseTreeVisitor::visitQuery(ClickHouseParser::QueryContext *ctx)
{
    if (ctx->distributedStmt())
        return std::static_pointer_cast<Query>(ctx->distributedStmt()->accept(this).as<PtrTo<DDLQuery>>());
    if (ctx->selectUnionStmt())
        return std::static_pointer_cast<Query>(ctx->selectUnionStmt()->accept(this).as<PtrTo<SelectUnionQuery>>());
    if (ctx->setStmt())
        return std::static_pointer_cast<Query>(ctx->setStmt()->accept(this).as<PtrTo<SetQuery>>());
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitDistributedStmt(ClickHouseParser::DistributedStmtContext *ctx)
{
    PtrTo<DDLQuery> query;

    if (ctx->createDatabaseStmt())
        query = ctx->createDatabaseStmt()->accept(this).as<PtrTo<CreateDatabaseQuery>>();
    else if (ctx->dropStmt())
        query = ctx->dropStmt()->accept(this).as<PtrTo<DropQuery>>();

    if (ctx->CLUSTER())
        query->setOnCluster(ctx->identifier()->accept(this));

    return query;
}

antlrcpp::Any ParseTreeVisitor::visitCreateDatabaseStmt(ClickHouseParser::CreateDatabaseStmtContext *ctx)
{
    return std::make_shared<CreateDatabaseQuery>(
        !!ctx->IF(),
        ctx->databaseIdentifier()->accept(this),
        ctx->engineExpr() ? ctx->engineExpr()->accept(this).as<PtrTo<EngineExpr>>() : nullptr);
}

antlrcpp::Any ParseTreeVisitor::visitCreateTableStmt(ClickHouseParser::CreateTableStmtContext *ctx)
{
    return std::make_shared<CreateTableQuery>(!!ctx->IF(), ctx->tableIdentifier()->accept(this), ctx->schemaClause()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitDropDatabaseStmt(ClickHouseParser::DropDatabaseStmtContext *ctx)
{
    return DropQuery::createDropDatabase(!!ctx->EXISTS(), ctx->databaseIdentifier()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitDropTableStmt(ClickHouseParser::DropTableStmtContext *ctx)
{
    return DropQuery::createDropTable(!!ctx->EXISTS(), !!ctx->TEMPORARY(), ctx->tableIdentifier()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitOptimizeStmt(ClickHouseParser::OptimizeStmtContext *ctx)
{
    return OptimizeQuery(ctx->tableIdentifier()->accept(this), ctx->partitionClause()->accept(this), !!ctx->FINAL(), !!ctx->DEDUPLICATE());
}

antlrcpp::Any ParseTreeVisitor::visitInsertStmt(ClickHouseParser::InsertStmtContext *ctx)
{
    auto list = std::make_shared<ColumnNameList>();

    for (auto * name : ctx->identifier()) list->append(name->accept(this));

    return std::make_shared<InsertQuery>(ctx->tableIdentifier()->accept(this), list, ctx->valuesClause()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitSelectUnionStmt(ClickHouseParser::SelectUnionStmtContext *ctx)
{
    auto select_union_query = std::make_shared<SelectUnionQuery>();

    for (auto * stmt : ctx->selectStmt())
        select_union_query->appendSelect(stmt->accept(this));

    return select_union_query;
}

antlrcpp::Any ParseTreeVisitor::visitSelectStmt(ClickHouseParser::SelectStmtContext *ctx)
{
    auto select_stmt = std::make_shared<SelectStmt>(ctx->columnExprList()->accept(this).as<PtrTo<ColumnExprList>>());

    if (ctx->withClause()) select_stmt->setWithClause(ctx->withClause()->accept(this));
    if (ctx->fromClause()) select_stmt->setFromClause(ctx->fromClause()->accept(this));
    if (ctx->sampleClause()) select_stmt->setSampleClause(ctx->sampleClause()->accept(this));
    if (ctx->arrayJoinClause()) select_stmt->setArrayJoinClause(ctx->arrayJoinClause()->accept(this));
    if (ctx->prewhereClause()) select_stmt->setPrewhereClause(ctx->prewhereClause()->accept(this));
    if (ctx->whereClause()) select_stmt->setWhereClause(ctx->whereClause()->accept(this));
    if (ctx->groupByClause()) select_stmt->setGroupByClause(ctx->groupByClause()->accept(this));
    if (ctx->havingClause()) select_stmt->setHavingClause(ctx->havingClause()->accept(this));
    if (ctx->orderByClause()) select_stmt->setOrderByClause(ctx->orderByClause()->accept(this));
    if (ctx->limitByClause()) select_stmt->setLimitByClause(ctx->limitByClause()->accept(this));
    if (ctx->limitClause()) select_stmt->setLimitClause(ctx->limitClause()->accept(this));
    if (ctx->settingsClause()) select_stmt->setSettingsClause(ctx->settingsClause()->accept(this));

    return select_stmt;
}

antlrcpp::Any ParseTreeVisitor::visitSetStmt(ClickHouseParser::SetStmtContext *ctx)
{
    return std::make_shared<SetQuery>(ctx->settingExprList()->accept(this).as<PtrTo<SettingExprList>>());
}

antlrcpp::Any ParseTreeVisitor::visitUseStmt(ClickHouseParser::UseStmtContext *ctx)
{
    return std::make_shared<UseQuery>(ctx->databaseIdentifier()->accept(this).as<PtrTo<DatabaseIdentifier>>());
}

}
