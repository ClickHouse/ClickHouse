#include <Parsers/New/ParseTreeVisitor.h>

#include <Parsers/New/AST/AlterTableQuery.h>
#include <Parsers/New/AST/CheckQuery.h>
#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/CreateDatabaseQuery.h>
#include <Parsers/New/AST/CreateTableQuery.h>
#include <Parsers/New/AST/DDLQuery.h>
#include <Parsers/New/AST/DescribeQuery.h>
#include <Parsers/New/AST/DropQuery.h>
#include <Parsers/New/AST/EngineExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/InsertQuery.h>
#include <Parsers/New/AST/JoinExpr.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/AST/OptimizeQuery.h>
#include <Parsers/New/AST/SelectStmt.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/AST/SetQuery.h>
#include <Parsers/New/AST/ShowQuery.h>
#include <Parsers/New/AST/TableExpr.h>
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
    if (ctx->selectUnionStmt())
        return std::static_pointer_cast<Query>(ctx->selectUnionStmt()->accept(this).as<PtrTo<SelectUnionQuery>>());
    if (ctx->setStmt())
        return std::static_pointer_cast<Query>(ctx->setStmt()->accept(this).as<PtrTo<SetQuery>>());
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableStmt(ClickHouseParser::AlterTableStmtContext *ctx)
{
    return std::make_shared<AlterTableQuery>(ctx->tableIdentifier()->accept(this), ctx->alterTableClause()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitCheckStmt(ClickHouseParser::CheckStmtContext *ctx)
{
    return std::make_shared<CheckQuery>(ctx->tableIdentifier()->accept(this));
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
    return std::make_shared<CreateTableQuery>(
        !!ctx->TEMPORARY(), !!ctx->IF(), ctx->tableIdentifier()->accept(this), ctx->schemaClause()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitDescribeStmt(ClickHouseParser::DescribeStmtContext *ctx)
{
    return std::make_shared<DescribeQuery>(ctx->tableIdentifier()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitDropDatabaseStmt(ClickHouseParser::DropDatabaseStmtContext *ctx)
{
    return DropQuery::createDropDatabase(!!ctx->EXISTS(), ctx->databaseIdentifier()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitDropTableStmt(ClickHouseParser::DropTableStmtContext *ctx)
{
    return DropQuery::createDropTable(!!ctx->EXISTS(), !!ctx->TEMPORARY(), ctx->tableIdentifier()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitInsertStmt(ClickHouseParser::InsertStmtContext *ctx)
{
    auto list = std::make_shared<ColumnNameList>();

    for (auto * name : ctx->identifier()) list->append(name->accept(this));

    return std::make_shared<InsertQuery>(ctx->tableIdentifier()->accept(this), list, ctx->valuesClause()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitOptimizeStmt(ClickHouseParser::OptimizeStmtContext *ctx)
{
    return OptimizeQuery(ctx->tableIdentifier()->accept(this), ctx->partitionClause()->accept(this), !!ctx->FINAL(), !!ctx->DEDUPLICATE());
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

antlrcpp::Any ParseTreeVisitor::visitShowCreateTableStmt(ClickHouseParser::ShowCreateTableStmtContext *ctx)
{
    return std::make_shared<ShowCreateTableQuery>(!!ctx->TEMPORARY(), ctx->tableIdentifier()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitShowTablesStmt(ClickHouseParser::ShowTablesStmtContext *ctx)
{
    // TODO: don't forget to convert TEMPORARY into 'is_temporary=1' condition.

    auto table_name = std::make_shared<ColumnIdentifier>(nullptr, std::make_shared<Identifier>("name"), nullptr);
    auto expr_list = PtrTo<ColumnExprList>(new ColumnExprList{ColumnExpr::createIdentifier(table_name)});
    auto select_stmt = std::make_shared<SelectStmt>(expr_list);

    auto and_args = PtrTo<ColumnExprList>(new ColumnExprList{Literal::createNumber("1")});

    if (ctx->databaseIdentifier())
    {
        auto database = std::make_shared<ColumnIdentifier>(nullptr, std::make_shared<Identifier>("database"), nullptr);
        auto args = PtrTo<ColumnExprList>(new ColumnExprList{ColumnExpr::createIdentifier(database), Literal::createString("db")});
        and_args->append(ColumnExpr::createFunction(std::make_shared<Identifier>("equals"), nullptr, args));
    }

    if (ctx->LIKE())
    {
        auto args = PtrTo<ColumnExprList>(
            new ColumnExprList{ColumnExpr::createIdentifier(table_name), Literal::createString(ctx->STRING_LITERAL())});
        and_args->append(ColumnExpr::createFunction(std::make_shared<Identifier>("like"), nullptr, args));
    }
    else if (ctx->whereClause())
        and_args->append(ctx->whereClause()->columnExpr()->accept(this));

    auto system = std::make_shared<DatabaseIdentifier>(std::make_shared<Identifier>("system"));
    auto tables = std::make_shared<TableIdentifier>(system, std::make_shared<Identifier>("tables"));
    auto system_tables = JoinExpr::createTableExpr(TableExpr::createIdentifier(tables));

    select_stmt->setFromClause(std::make_shared<FromClause>(system_tables, false));
    select_stmt->setWhereClause(
        std::make_shared<WhereClause>(ColumnExpr::createFunction(std::make_shared<Identifier>("and"), nullptr, and_args)));
    select_stmt->setLimitClause(ctx->limitClause() ? ctx->limitClause()->accept(this).as<PtrTo<LimitClause>>() : nullptr);

    return select_stmt;
}

antlrcpp::Any ParseTreeVisitor::visitUseStmt(ClickHouseParser::UseStmtContext *ctx)
{
    return std::make_shared<UseQuery>(ctx->databaseIdentifier()->accept(this).as<PtrTo<DatabaseIdentifier>>());
}

}
