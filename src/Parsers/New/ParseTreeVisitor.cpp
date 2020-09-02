#include <Parsers/New/AST/AlterPartitionQuery.h>
#include <Parsers/New/AST/AlterTableQuery.h>
#include <Parsers/New/AST/AnalyzeQuery.h>
#include <Parsers/New/AST/CheckQuery.h>
#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/CreateDatabaseQuery.h>
#include <Parsers/New/AST/CreateMaterializedViewQuery.h>
#include <Parsers/New/AST/CreateTableQuery.h>
#include <Parsers/New/AST/CreateViewQuery.h>
#include <Parsers/New/AST/DDLQuery.h>
#include <Parsers/New/AST/DescribeQuery.h>
#include <Parsers/New/AST/DropQuery.h>
#include <Parsers/New/AST/EngineExpr.h>
#include <Parsers/New/AST/ExistsQuery.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/InsertQuery.h>
#include <Parsers/New/AST/JoinExpr.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/AST/OptimizeQuery.h>
#include <Parsers/New/AST/RenameQuery.h>
#include <Parsers/New/AST/SelectStmt.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/AST/SetQuery.h>
#include <Parsers/New/AST/ShowQuery.h>
#include <Parsers/New/AST/SystemQuery.h>
#include <Parsers/New/AST/TableExpr.h>
#include <Parsers/New/AST/UseQuery.h>

// antlr-runtime undefines EOF macros, which is required in boost multiprecision numbers
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitQueryList(ClickHouseParser::QueryListContext *ctx)
{
    auto query_list = std::make_shared<QueryList>();
    for (auto * query : ctx->queryStmt()) query_list->append(visit(query));
    return query_list;
}

antlrcpp::Any ParseTreeVisitor::visitQueryStmt(ClickHouseParser::QueryStmtContext *ctx)
{
    auto query = visit(ctx->query()).as<PtrTo<Query>>();

    if (ctx->OUTFILE()) query->setOutFile(Literal::createString(ctx->STRING_LITERAL()));
    if (ctx->FORMAT()) query->setFormat(visit(ctx->identifierOrNull()));

    return query;
}

antlrcpp::Any ParseTreeVisitor::visitQuery(ClickHouseParser::QueryContext *ctx)
{
    auto query = visit(ctx->children[0]);

#define TRY_POINTER_CAST(TYPE) if (query.is<PtrTo<TYPE>>()) return std::static_pointer_cast<Query>(query.as<PtrTo<TYPE>>());
    TRY_POINTER_CAST(AlterPartitionQuery)
    TRY_POINTER_CAST(AlterTableQuery)
    TRY_POINTER_CAST(AlterPartitionQuery)
    TRY_POINTER_CAST(AnalyzeQuery)
    TRY_POINTER_CAST(CheckQuery)
    TRY_POINTER_CAST(CreateDatabaseQuery)
    TRY_POINTER_CAST(CreateMaterializedViewQuery)
    TRY_POINTER_CAST(CreateTableQuery)
    TRY_POINTER_CAST(CreateViewQuery)
    TRY_POINTER_CAST(DescribeQuery)
    TRY_POINTER_CAST(DropQuery)
    TRY_POINTER_CAST(ExistsQuery)
    TRY_POINTER_CAST(InsertQuery)
    TRY_POINTER_CAST(OptimizeQuery)
    TRY_POINTER_CAST(RenameQuery)
    TRY_POINTER_CAST(SelectUnionQuery)
    TRY_POINTER_CAST(SetQuery)
    TRY_POINTER_CAST(ShowCreateTableQuery)
    TRY_POINTER_CAST(SystemQuery)
    TRY_POINTER_CAST(UseQuery)
#undef TRY_POINTER_CAST

    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitAlterPartitionStmt(ClickHouseParser::AlterPartitionStmtContext *ctx)
{
    auto list= std::make_shared<List<AlterPartitionClause>>();
    for (auto * clause : ctx->alterPartitionClause()) list->append(visit(clause));
    return std::make_shared<AlterPartitionQuery>(visit(ctx->tableIdentifier()), list);
}

antlrcpp::Any ParseTreeVisitor::visitAlterTableStmt(ClickHouseParser::AlterTableStmtContext *ctx)
{
    auto list = std::make_shared<List<AlterTableClause>>();
    for (auto * clause : ctx->alterTableClause()) list->append(visit(clause));
    return std::make_shared<AlterTableQuery>(visit(ctx->tableIdentifier()), list);
}

antlrcpp::Any ParseTreeVisitor::visitAnalyzeStmt(ClickHouseParser::AnalyzeStmtContext *ctx)
{
    return std::make_shared<AnalyzeQuery>(visit(ctx->queryStmt()).as<PtrTo<Query>>());
}

antlrcpp::Any ParseTreeVisitor::visitCheckStmt(ClickHouseParser::CheckStmtContext *ctx)
{
    return std::make_shared<CheckQuery>(visit(ctx->tableIdentifier()).as<PtrTo<TableIdentifier>>());
}

antlrcpp::Any ParseTreeVisitor::visitCreateDatabaseStmt(ClickHouseParser::CreateDatabaseStmtContext *ctx)
{
    auto engine = ctx->engineExpr() ? visit(ctx->engineExpr()).as<PtrTo<EngineExpr>>() : nullptr;
    return std::make_shared<CreateDatabaseQuery>(!!ctx->IF(), visit(ctx->databaseIdentifier()), engine);
}

antlrcpp::Any ParseTreeVisitor::visitCreateMaterializedViewStmt(ClickHouseParser::CreateMaterializedViewStmtContext *ctx)
{
    auto schema = ctx->schemaClause() ? visit(ctx->schemaClause()).as<PtrTo<SchemaClause>>() : nullptr;
    auto engine = ctx->engineClause() ? visit(ctx->engineClause()).as<PtrTo<EngineClause>>() : nullptr;
    auto destination = ctx->destinationClause() ? visit(ctx->destinationClause()).as<PtrTo<DestinationClause>>() : nullptr;
    return std::make_shared<CreateMaterializedViewQuery>(
        !!ctx->IF(), visit(ctx->tableIdentifier()), schema, destination, engine, visit(ctx->subqueryClause()));
}

antlrcpp::Any ParseTreeVisitor::visitCreateTableStmt(ClickHouseParser::CreateTableStmtContext *ctx)
{
    auto schema = ctx->schemaClause() ? visit(ctx->schemaClause()).as<PtrTo<SchemaClause>>() : nullptr;
    auto engine = ctx->engineClause() ? visit(ctx->engineClause()).as<PtrTo<EngineClause>>() : nullptr;
    auto query = ctx->subqueryClause() ? visit(ctx->subqueryClause()).as<PtrTo<SelectUnionQuery>>() : nullptr;
    return std::make_shared<CreateTableQuery>(!!ctx->TEMPORARY(), !!ctx->IF(), visit(ctx->tableIdentifier()), schema, engine, query);
}

antlrcpp::Any ParseTreeVisitor::visitCreateViewStmt(ClickHouseParser::CreateViewStmtContext *ctx)
{
    return std::make_shared<CreateViewQuery>(!!ctx->IF(), visit(ctx->tableIdentifier()), visit(ctx->subqueryClause()));
}

antlrcpp::Any ParseTreeVisitor::visitDescribeStmt(ClickHouseParser::DescribeStmtContext *ctx)
{
    return std::make_shared<DescribeQuery>(visit(ctx->tableExpr()).as<PtrTo<TableExpr>>());
}

antlrcpp::Any ParseTreeVisitor::visitDropDatabaseStmt(ClickHouseParser::DropDatabaseStmtContext *ctx)
{
    return DropQuery::createDropDatabase(!!ctx->EXISTS(), visit(ctx->databaseIdentifier()));
}

antlrcpp::Any ParseTreeVisitor::visitDropTableStmt(ClickHouseParser::DropTableStmtContext *ctx)
{
    return DropQuery::createDropTable(!!ctx->EXISTS(), !!ctx->TEMPORARY(), visit(ctx->tableIdentifier()));
}

antlrcpp::Any ParseTreeVisitor::visitExistsStmt(ClickHouseParser::ExistsStmtContext *ctx)
{
    return std::make_shared<ExistsQuery>(!!ctx->TEMPORARY(), visit(ctx->tableIdentifier()));
}

antlrcpp::Any ParseTreeVisitor::visitInsertFunctionStmt(ClickHouseParser::InsertFunctionStmtContext *ctx)
{
    auto args = ctx->tableArgList() ? visit(ctx->tableArgList()).as<PtrTo<TableArgList>>() : nullptr;
    return InsertQuery::createFunction(visit(ctx->identifier()), args, visit(ctx->valuesClause()));
}

antlrcpp::Any ParseTreeVisitor::visitInsertTableStmt(ClickHouseParser::InsertTableStmtContext *ctx)
{
    auto list = std::make_shared<ColumnNameList>();
    for (auto * name : ctx->nestedIdentifier()) list->append(visit(name));
    return InsertQuery::createTable(visit(ctx->tableIdentifier()), list, visit(ctx->valuesClause()));
}

antlrcpp::Any ParseTreeVisitor::visitOptimizeStmt(ClickHouseParser::OptimizeStmtContext *ctx)
{
    auto clause = ctx->partitionClause() ? visit(ctx->partitionClause()).as<PtrTo<PartitionExprList>>() : nullptr;
    return std::make_shared<OptimizeQuery>(visit(ctx->tableIdentifier()), clause, !!ctx->FINAL(), !!ctx->DEDUPLICATE());
}

antlrcpp::Any ParseTreeVisitor::visitRenameStmt(ClickHouseParser::RenameStmtContext *ctx)
{
    auto list = std::make_shared<List<TableIdentifier>>();
    for (auto * identifier : ctx->tableIdentifier()) list->append(visit(identifier));
    return std::make_shared<RenameQuery>(list);
}

antlrcpp::Any ParseTreeVisitor::visitSelectUnionStmt(ClickHouseParser::SelectUnionStmtContext *ctx)
{
    auto select_union_query = std::make_shared<SelectUnionQuery>();
    for (auto * stmt : ctx->selectStmt()) select_union_query->appendSelect(visit(stmt));
    return select_union_query;
}

antlrcpp::Any ParseTreeVisitor::visitSetStmt(ClickHouseParser::SetStmtContext *ctx)
{
    return std::make_shared<SetQuery>(visit(ctx->settingExprList()).as<PtrTo<SettingExprList>>());
}

antlrcpp::Any ParseTreeVisitor::visitShowCreateTableStmt(ClickHouseParser::ShowCreateTableStmtContext *ctx)
{
    return std::make_shared<ShowCreateTableQuery>(!!ctx->TEMPORARY(), visit(ctx->tableIdentifier()));
}

antlrcpp::Any ParseTreeVisitor::visitShowTablesStmt(ClickHouseParser::ShowTablesStmtContext *ctx)
{
    // TODO: don't forget to convert TEMPORARY into 'is_temporary=1' condition.

    auto table_name = std::make_shared<ColumnIdentifier>(nullptr, std::make_shared<Identifier>("name"));
    auto expr_list = PtrTo<ColumnExprList>(new ColumnExprList{ColumnExpr::createIdentifier(table_name)});
    auto select_stmt = std::make_shared<SelectStmt>(expr_list);

    auto and_args = PtrTo<ColumnExprList>(new ColumnExprList{Literal::createNumber("1")});

    if (ctx->databaseIdentifier())
    {
        auto database = std::make_shared<ColumnIdentifier>(nullptr, std::make_shared<Identifier>("database"));
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
        and_args->append(visit(ctx->whereClause()->columnExpr()));

    auto system = std::make_shared<DatabaseIdentifier>(std::make_shared<Identifier>("system"));
    auto tables = std::make_shared<TableIdentifier>(system, std::make_shared<Identifier>("tables"));
    auto system_tables = JoinExpr::createTableExpr(TableExpr::createIdentifier(tables));

    select_stmt->setFromClause(std::make_shared<FromClause>(system_tables, false));
    select_stmt->setWhereClause(
        std::make_shared<WhereClause>(ColumnExpr::createFunction(std::make_shared<Identifier>("and"), nullptr, and_args)));
    select_stmt->setLimitClause(ctx->limitClause() ? visit(ctx->limitClause()).as<PtrTo<LimitClause>>() : nullptr);

    return PtrTo<SelectUnionQuery>(new SelectUnionQuery({select_stmt}));
}

antlrcpp::Any ParseTreeVisitor::visitSystemSyncStmt(ClickHouseParser::SystemSyncStmtContext *ctx)
{
    return SystemQuery::createSync(visit(ctx->tableIdentifier()).as<PtrTo<TableIdentifier>>());
}

antlrcpp::Any ParseTreeVisitor::visitUseStmt(ClickHouseParser::UseStmtContext *ctx)
{
    return std::make_shared<UseQuery>(visit(ctx->databaseIdentifier()).as<PtrTo<DatabaseIdentifier>>());
}

}
