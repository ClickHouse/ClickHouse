#include <Parsers/New/AST/InsertQuery.h>

#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/SelectUnionQuery.h>

#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

// static
PtrTo<ValuesClause> ValuesClause::createValues(PtrTo<ColumnExprList> list)
{
    return PtrTo<ValuesClause>(new ValuesClause(ClauseType::VALUES, {list->begin(), list->end()}));
}

// static
PtrTo<ValuesClause> ValuesClause::createSelect(PtrTo<SelectUnionQuery> query)
{
    return PtrTo<ValuesClause>(new ValuesClause(ClauseType::SELECT, {query}));
}

ValuesClause::ValuesClause(ClauseType type, PtrList exprs) : clause_type(type)
{
    children = exprs;
    (void)clause_type; // TODO
}

// static
PtrTo<InsertQuery> InsertQuery::createTable(PtrTo<TableIdentifier> identifier, PtrTo<ColumnNameList> list, PtrTo<ValuesClause> clause)
{
    return PtrTo<InsertQuery>(new InsertQuery(QueryType::TABLE, {identifier, list, clause}));
}

// static
PtrTo<InsertQuery> InsertQuery::createFunction(PtrTo<Identifier> name, PtrTo<TableArgList> args, PtrTo<ValuesClause> clause)
{
    return PtrTo<InsertQuery>(new InsertQuery(QueryType::FUNCTION, {name, args, clause}));
}

InsertQuery::InsertQuery(QueryType type, PtrList exprs) : query_type(type)
{
    children = exprs;

    (void) query_type; // TODO
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitValuesClause(ClickHouseParser::ValuesClauseContext *ctx)
{
    if (ctx->VALUES())
    {
        auto list = std::make_shared<ColumnExprList>();
        for (auto * expr : ctx->valueTupleExpr()) list->append(visit(expr));
        return ValuesClause::createValues(list);
    }
    if (ctx->selectUnionStmt()) return ValuesClause::createSelect(visit(ctx->selectUnionStmt()));
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
