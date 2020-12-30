#include <Parsers/New/AST/AlterTableQuery.h>
#include <Parsers/New/AST/AttachQuery.h>
#include <Parsers/New/AST/CheckQuery.h>
#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/CreateDatabaseQuery.h>
#include <Parsers/New/AST/CreateDictionaryQuery.h>
#include <Parsers/New/AST/CreateLiveViewQuery.h>
#include <Parsers/New/AST/CreateMaterializedViewQuery.h>
#include <Parsers/New/AST/CreateTableQuery.h>
#include <Parsers/New/AST/CreateViewQuery.h>
#include <Parsers/New/AST/DDLQuery.h>
#include <Parsers/New/AST/DescribeQuery.h>
#include <Parsers/New/AST/DropQuery.h>
#include <Parsers/New/AST/EngineExpr.h>
#include <Parsers/New/AST/ExistsQuery.h>
#include <Parsers/New/AST/ExplainQuery.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/InsertQuery.h>
#include <Parsers/New/AST/JoinExpr.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/AST/KillQuery.h>
#include <Parsers/New/AST/OptimizeQuery.h>
#include <Parsers/New/AST/RenameQuery.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/AST/SetQuery.h>
#include <Parsers/New/AST/ShowQuery.h>
#include <Parsers/New/AST/ShowCreateQuery.h>
#include <Parsers/New/AST/SystemQuery.h>
#include <Parsers/New/AST/TableExpr.h>
#include <Parsers/New/AST/TruncateQuery.h>
#include <Parsers/New/AST/UseQuery.h>
#include <Parsers/New/AST/WatchQuery.h>

// Include last, because antlr-runtime undefines EOF macros, which is required in boost multiprecision numbers.
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitQueryStmt(ClickHouseParser::QueryStmtContext *ctx)
{
    if (ctx->insertStmt()) return std::static_pointer_cast<Query>(visit(ctx->insertStmt()).as<PtrTo<InsertQuery>>());

    auto query = visit(ctx->query()).as<PtrTo<Query>>();

    if (ctx->OUTFILE()) query->setOutFile(Literal::createString(ctx->STRING_LITERAL()));
    if (ctx->FORMAT()) query->setFormat(visit(ctx->identifierOrNull()));

    return query;
}

antlrcpp::Any ParseTreeVisitor::visitQuery(ClickHouseParser::QueryContext *ctx)
{
    auto query = visit(ctx->children[0]);

#define TRY_POINTER_CAST(TYPE) if (query.is<PtrTo<TYPE>>()) return std::static_pointer_cast<Query>(query.as<PtrTo<TYPE>>());
    TRY_POINTER_CAST(AlterTableQuery)
    TRY_POINTER_CAST(AttachQuery)
    TRY_POINTER_CAST(CheckQuery)
    TRY_POINTER_CAST(CreateDatabaseQuery)
    TRY_POINTER_CAST(CreateDictionaryQuery)
    TRY_POINTER_CAST(CreateLiveViewQuery)
    TRY_POINTER_CAST(CreateMaterializedViewQuery)
    TRY_POINTER_CAST(CreateTableQuery)
    TRY_POINTER_CAST(CreateViewQuery)
    TRY_POINTER_CAST(DescribeQuery)
    TRY_POINTER_CAST(DropQuery)
    TRY_POINTER_CAST(ExistsQuery)
    TRY_POINTER_CAST(ExplainQuery)
    TRY_POINTER_CAST(KillQuery)
    TRY_POINTER_CAST(OptimizeQuery)
    TRY_POINTER_CAST(RenameQuery)
    TRY_POINTER_CAST(SelectUnionQuery)
    TRY_POINTER_CAST(SetQuery)
    TRY_POINTER_CAST(ShowQuery)
    TRY_POINTER_CAST(ShowCreateQuery)
    TRY_POINTER_CAST(SystemQuery)
    TRY_POINTER_CAST(TruncateQuery)
    TRY_POINTER_CAST(UseQuery)
    TRY_POINTER_CAST(WatchQuery)
#undef TRY_POINTER_CAST

    throw std::runtime_error("Query is unknown: " + ctx->children[0]->getText());

    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitShowDatabasesStmt(ClickHouseParser::ShowDatabasesStmtContext *)
{
    auto database_name = std::make_shared<ColumnIdentifier>(nullptr, std::make_shared<Identifier>("name"));
    auto expr_list = PtrTo<ColumnExprList>(new ColumnExprList{ColumnExpr::createIdentifier(database_name)});
    auto select_stmt = std::make_shared<SelectStmt>(false, SelectStmt::ModifierType::NONE, false, expr_list);

    auto system = std::make_shared<DatabaseIdentifier>(std::make_shared<Identifier>("system"));
    auto databases = std::make_shared<TableIdentifier>(system, std::make_shared<Identifier>("databases"));
    auto system_tables = JoinExpr::createTableExpr(TableExpr::createIdentifier(databases), nullptr, false);

    select_stmt->setFromClause(std::make_shared<FromClause>(system_tables));

    return PtrTo<SelectUnionQuery>(
        new SelectUnionQuery(std::make_shared<List<SelectStmt>>(std::initializer_list<PtrTo<SelectStmt>>{select_stmt})));
}

antlrcpp::Any ParseTreeVisitor::visitShowTablesStmt(ClickHouseParser::ShowTablesStmtContext *ctx)
{
    // TODO: don't forget to convert TEMPORARY into 'is_temporary=1' condition.

    auto table_name = std::make_shared<ColumnIdentifier>(nullptr, std::make_shared<Identifier>("name"));
    auto expr_list = PtrTo<ColumnExprList>(new ColumnExprList{ColumnExpr::createIdentifier(table_name)});
    auto select_stmt = std::make_shared<SelectStmt>(false, SelectStmt::ModifierType::NONE, false, expr_list);

    auto and_args = PtrTo<ColumnExprList>(new ColumnExprList{ColumnExpr::createLiteral(Literal::createNumber("1"))});

    if (ctx->databaseIdentifier())
    {
        auto database = std::make_shared<ColumnIdentifier>(nullptr, std::make_shared<Identifier>("database"));
        auto args = PtrTo<ColumnExprList>(new ColumnExprList{
            ColumnExpr::createIdentifier(database),
            ColumnExpr::createLiteral(Literal::createString(visit(ctx->databaseIdentifier()).as<PtrTo<DatabaseIdentifier>>()->getName()))
        });
        and_args->push(ColumnExpr::createFunction(std::make_shared<Identifier>("equals"), nullptr, args));
    }

    if (ctx->LIKE())
    {
        auto args = PtrTo<ColumnExprList>(new ColumnExprList{
            ColumnExpr::createIdentifier(table_name), ColumnExpr::createLiteral(Literal::createString(ctx->STRING_LITERAL()))});
        and_args->push(ColumnExpr::createFunction(std::make_shared<Identifier>("like"), nullptr, args));
    }
    else if (ctx->whereClause())
        and_args->push(visit(ctx->whereClause()->columnExpr()));

    auto system = std::make_shared<DatabaseIdentifier>(std::make_shared<Identifier>("system"));
    auto tables = std::make_shared<TableIdentifier>(system, std::make_shared<Identifier>("tables"));
    auto system_tables = JoinExpr::createTableExpr(TableExpr::createIdentifier(tables), nullptr, false);

    select_stmt->setFromClause(std::make_shared<FromClause>(system_tables));
    select_stmt->setWhereClause(
        std::make_shared<WhereClause>(ColumnExpr::createFunction(std::make_shared<Identifier>("and"), nullptr, and_args)));
    select_stmt->setLimitClause(ctx->limitClause() ? visit(ctx->limitClause()).as<PtrTo<LimitClause>>() : nullptr);

    return PtrTo<SelectUnionQuery>(
        new SelectUnionQuery(std::make_shared<List<SelectStmt>>(std::initializer_list<PtrTo<SelectStmt>>{select_stmt})));
}

}
