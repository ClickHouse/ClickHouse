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
            query->table = children[IDENTIFIER]->convertToOld();
            break;
    }

    if (has(COLUMNS)) query->columns = children[COLUMNS]->convertToOld();

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitColumnsClause(ClickHouseParser::ColumnsClauseContext *ctx)
{
    auto list = std::make_shared<ColumnNameList>();
    for (auto * name : ctx->nestedIdentifier()) list->append(visit(name));
    return list;
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

antlrcpp::Any ParseTreeVisitor::visitInsertStmt(ClickHouseParser::InsertStmtContext *ctx)
{
    auto data = ctx->dataClause() ? visit(ctx->dataClause()).as<PtrTo<DataClause>>() : nullptr;
    auto columns = ctx->columnsClause() ? visit(ctx->columnsClause()).as<PtrTo<ColumnNameList>>() : nullptr;

    if (ctx->FUNCTION()) return InsertQuery::createFunction(visit(ctx->tableFunctionExpr()), columns, data);
    if (ctx->tableIdentifier()) return InsertQuery::createTable(visit(ctx->tableIdentifier()), columns, data);
    __builtin_unreachable();
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
