#include <Parsers/New/AST/InsertQuery.h>

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/AST/TableExpr.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

// static
PtrTo<DataClause> DataClause::createFormat(PtrTo<Identifier> identifier)
{
    return PtrTo<DataClause>(new DataClause(ClauseType::FORMAT, {identifier}));
}

// static
PtrTo<DataClause> DataClause::createSelect(PtrTo<SelectUnionQuery> query)
{
    return PtrTo<DataClause>(new DataClause(ClauseType::SELECT, {query}));
}

// static
PtrTo<DataClause> DataClause::createValues(PtrTo<ColumnExprList> list)
{
    return PtrTo<DataClause>(new DataClause(ClauseType::VALUES, {list->begin(), list->end()}));
}

DataClause::DataClause(ClauseType type, PtrList exprs) : clause_type(type)
{
    children = exprs;

    (void) clause_type; // TODO
}

// static
PtrTo<InsertQuery> InsertQuery::createTable(PtrTo<TableIdentifier> identifier, PtrTo<ColumnNameList> list, PtrTo<DataClause> clause)
{
    return PtrTo<InsertQuery>(new InsertQuery(QueryType::TABLE, {identifier, list, clause}));
}

// static
PtrTo<InsertQuery> InsertQuery::createFunction(PtrTo<TableFunctionExpr> function, PtrTo<ColumnNameList> list, PtrTo<DataClause> clause)
{
    return PtrTo<InsertQuery>(new InsertQuery(QueryType::FUNCTION, {function, list, clause}));
}

InsertQuery::InsertQuery(QueryType type, PtrList exprs) : query_type(type)
{
    children = exprs;
}

ASTPtr InsertQuery::convertToOld() const
{
    auto query = std::make_shared<ASTInsertQuery>();

    switch(query_type)
    {
        case QueryType::FUNCTION:
            query->table_function = children[FUNCTION]->convertToOld();
            break;
        case QueryType::TABLE:
            query->table_id = children[IDENTIFIER]->convertToOld();
            break;
    }

    if (has(COLUMNS)) query->columns = children[COLUMNS]->convertToOld();

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitInsertFunctionStmt(ClickHouseParser::InsertFunctionStmtContext *ctx)
{
    auto data = ctx->dataClause() ? visit(ctx->dataClause()).as<PtrTo<DataClause>>() : nullptr;
    auto list = std::make_shared<ColumnNameList>();
    for (auto * name : ctx->nestedIdentifier()) list->append(visit(name));
    return InsertQuery::createFunction(visit(ctx->tableFuncExpr()), list, data);
}

antlrcpp::Any ParseTreeVisitor::visitInsertTableStmt(ClickHouseParser::InsertTableStmtContext *ctx)
{
    auto data = ctx->dataClause() ? visit(ctx->dataClause()).as<PtrTo<DataClause>>() : nullptr;
    auto list = std::make_shared<ColumnNameList>();
    for (auto * name : ctx->nestedIdentifier()) list->append(visit(name));
    return InsertQuery::createTable(visit(ctx->tableIdentifier()), list, data);
}

antlrcpp::Any ParseTreeVisitor::visitDataClauseFormat(ClickHouseParser::DataClauseFormatContext *ctx)
{
    return DataClause::createFormat(visit(ctx->identifier()));
}

antlrcpp::Any ParseTreeVisitor::visitDataClauseSelect(ClickHouseParser::DataClauseSelectContext *ctx)
{
    return DataClause::createSelect(visit(ctx->selectUnionStmt()));
}

antlrcpp::Any ParseTreeVisitor::visitDataClauseValues(ClickHouseParser::DataClauseValuesContext *ctx)
{
    auto list = std::make_shared<ColumnExprList>();
    for (auto * expr : ctx->valueTupleExpr()) list->append(visit(expr));
    return DataClause::createValues(list);
}

antlrcpp::Any ParseTreeVisitor::visitValueTupleExpr(ClickHouseParser::ValueTupleExprContext *ctx)
{
    // TODO: copy-paste from |visitColumnExprTuple()|,
    //       should be removed once proper INSERT VALUES parsers are implemented.

    auto name = std::make_shared<Identifier>("tuple");
    auto args = visit(ctx->columnExprList()).as<PtrTo<ColumnExprList>>();
    return ColumnExpr::createFunction(name, nullptr, args);
}

}
