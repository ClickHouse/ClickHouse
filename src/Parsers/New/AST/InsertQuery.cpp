#include <Parsers/New/AST/InsertQuery.h>

#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/AST/ValueExpr.h>

#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

// static
PtrTo<ValuesClause> ValuesClause::createValues(PtrTo<ValueExprList> list)
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

InsertQuery::InsertQuery(PtrTo<TableIdentifier> identifier, PtrTo<ColumnNameList> list, PtrTo<ValuesClause> clause)
{
    children.push_back(identifier);
    children.push_back(list);
    children.push_back(clause);
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitValuesClause(ClickHouseParser::ValuesClauseContext *ctx)
{
    if (ctx->VALUES())
    {
        auto list = std::make_shared<ValueExprList>();
        for (auto * expr : ctx->valueTupleExpr()) list->append(visit(expr));
        return ValuesClause::createValues(list);
    }
    if (ctx->selectUnionStmt()) return ValuesClause::createSelect(visit(ctx->selectUnionStmt()));
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitValueTupleExpr(ClickHouseParser::ValueTupleExprContext *ctx)
{
    return ValueExpr::createTuple(visit(ctx->valueExprList()));
}

}
