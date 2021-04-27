#include <Parsers/New/AST/InsertQuery.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/AST/TableExpr.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

// static
PtrTo<DataClause> DataClause::createFormat(PtrTo<Identifier> identifier, size_t data_offset)
{
    PtrTo<DataClause> clause(new DataClause(ClauseType::FORMAT, {identifier}));
    clause->offset = data_offset;
    return clause;
}

// static
PtrTo<DataClause> DataClause::createSelect(PtrTo<SelectUnionQuery> query)
{
    return PtrTo<DataClause>(new DataClause(ClauseType::SELECT, {query}));
}

// static
PtrTo<DataClause> DataClause::createValues(size_t data_offset)
{
    PtrTo<DataClause> clause(new DataClause(ClauseType::VALUES, {}));
    clause->offset = data_offset;
    return clause;
}

DataClause::DataClause(ClauseType type, PtrList exprs) : INode(exprs), clause_type(type)
{
}

ASTPtr DataClause::convertToOld() const
{
    if (clause_type != ClauseType::SELECT) return {};
    return get(SUBQUERY)->convertToOld();
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

InsertQuery::InsertQuery(QueryType type, PtrList exprs) : Query(exprs), query_type(type)
{
}

ASTPtr InsertQuery::convertToOld() const
{
    auto query = std::make_shared<ASTInsertQuery>();

    switch(query_type)
    {
        case QueryType::FUNCTION:
            query->table_function = get(FUNCTION)->convertToOld();
            break;
        case QueryType::TABLE:
            query->table_id = getTableIdentifier(get(IDENTIFIER)->convertToOld());
            break;
    }

    if (has(COLUMNS)) query->columns = get(COLUMNS)->convertToOld();
    if (get<DataClause>(DATA)->getType() == DataClause::ClauseType::SELECT)
    {
        query->select = get(DATA)->convertToOld();
        query->children.push_back(query->select);
    }

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitColumnsClause(ClickHouseParser::ColumnsClauseContext *ctx)
{
    auto list = std::make_shared<ColumnNameList>();
    for (auto * name : ctx->nestedIdentifier()) list->push(visit(name));
    return list;
}

antlrcpp::Any ParseTreeVisitor::visitDataClauseFormat(ClickHouseParser::DataClauseFormatContext *ctx)
{
    return DataClause::createFormat(visit(ctx->identifier()), ctx->getStop()->getStopIndex() + 1);
}

antlrcpp::Any ParseTreeVisitor::visitDataClauseSelect(ClickHouseParser::DataClauseSelectContext *ctx)
{
    return DataClause::createSelect(visit(ctx->selectUnionStmt()));
}

antlrcpp::Any ParseTreeVisitor::visitDataClauseValues(ClickHouseParser::DataClauseValuesContext *ctx)
{
    return DataClause::createValues(ctx->getStop()->getStopIndex() + 1);
}

antlrcpp::Any ParseTreeVisitor::visitInsertStmt(ClickHouseParser::InsertStmtContext *ctx)
{
    auto columns = ctx->columnsClause() ? visit(ctx->columnsClause()).as<PtrTo<ColumnNameList>>() : nullptr;

    if (ctx->FUNCTION()) return InsertQuery::createFunction(visit(ctx->tableFunctionExpr()), columns, visit(ctx->dataClause()));
    if (ctx->tableIdentifier()) return InsertQuery::createTable(visit(ctx->tableIdentifier()), columns, visit(ctx->dataClause()));
    __builtin_unreachable();
}

}
