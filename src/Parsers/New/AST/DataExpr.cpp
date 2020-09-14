#include <Parsers/New/AST/DataExpr.h>

#include <Parsers/New/AST/JsonExpr.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

// static
PtrTo<DataExpr> DataExpr::createJSON(PtrTo<JsonExprList> list)
{
    return PtrTo<DataExpr>(new DataExpr(ExprType::JSON, {list}));
}

// static
PtrTo<DataExpr> DataExpr::createValues(PtrTo<ColumnExprList> list)
{
    return PtrTo<DataExpr>(new DataExpr(ExprType::VALUES, {list}));
}

DataExpr::DataExpr(ExprType type, PtrList exprs) : expr_type(type)
{
    children = exprs;

    (void) expr_type; // TODO
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitDataExprCSV(ClickHouseParser::DataExprCSVContext *ctx)
{
    auto list = std::make_shared<List<Literal>>();
    for (auto * literal : ctx->dataLiteral()) list->append(visit(literal));
    return list;
}

antlrcpp::Any ParseTreeVisitor::visitDataExprJSON(ClickHouseParser::DataExprJSONContext *ctx)
{
    auto list = std::make_shared<JsonExprList>();
    for (auto * json : ctx->jsonExpr()) list->append(visit(json));
    return DataExpr::createJSON(list);
}

antlrcpp::Any ParseTreeVisitor::visitDataExprTSV(ClickHouseParser::DataExprTSVContext *ctx)
{
    auto list = std::make_shared<List<Literal>>();
    for (auto * literal : ctx->dataLiteral()) list->append(visit(literal));
    return list;
}

antlrcpp::Any ParseTreeVisitor::visitDataExprValues(ClickHouseParser::DataExprValuesContext *ctx)
{
    return DataExpr::createValues(visit(ctx->valuesExpr()));
}

}
