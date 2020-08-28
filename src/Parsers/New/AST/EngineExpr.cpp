#include <Parsers/New/AST/EngineExpr.h>

#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/AST/SelectStmt.h>

#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

PartitionByClause::PartitionByClause(PtrTo<ColumnExpr> expr)
{
    children.push_back(expr);
}

PrimaryKeyClause::PrimaryKeyClause(PtrTo<ColumnExpr> expr)
{
    children.push_back(expr);
}

SampleByClause::SampleByClause(PtrTo<ColumnExpr> expr)
{
    children.push_back(expr);
}

TTLClause::TTLClause(PtrTo<TTLExprList> list)
{
    children.assign(list->begin(), list->end());
}

EngineClause::EngineClause(PtrTo<EngineExpr> expr)
{
    children.resize(MAX_INDEX);

    children[ENGINE] = expr;
}

void EngineClause::setOrderByClause(PtrTo<OrderByClause> clause)
{
    children[ORDER_BY] = clause;
}

void EngineClause::setPartitionByClause(PtrTo<PartitionByClause> clause)
{
    children[PARTITION_BY] = clause;
}

void EngineClause::setPrimaryKeyClause(PtrTo<PrimaryKeyClause> clause)
{
    children[PRIMARY_KEY] = clause;
}

void EngineClause::setSampleByClause(PtrTo<SampleByClause> clause)
{
    children[SAMPLE_BY] = clause;
}

void EngineClause::setTTLClause(PtrTo<TTLClause> clause)
{
    children[TTL] = clause;
}

void EngineClause::setSettingsClause(PtrTo<SettingsClause> clause)
{
    children[SETTINGS] = clause;
}

EngineExpr::EngineExpr(PtrTo<Identifier> identifier, PtrTo<ColumnExprList> args)
{
    children.push_back(identifier);
    if (args) children.insert(children.end(), args->begin(), args->end());
}

TTLExpr::TTLExpr(PtrTo<ColumnExpr> expr, TTLType type, PtrTo<StringLiteral> literal) : ttl_type(type)
{
    children.push_back(expr);
    children.push_back(literal);
    (void)ttl_type; // TODO
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitEngineClause(ClickHouseParser::EngineClauseContext *ctx)
{
    auto clause = std::make_shared<EngineClause>(visit(ctx->engineExpr()).as<PtrTo<EngineExpr>>());

    if (ctx->orderByClause()) clause->setOrderByClause(visit(ctx->orderByClause()));
    if (ctx->partitionByClause()) clause->setPartitionByClause(visit(ctx->partitionByClause()));
    if (ctx->primaryKeyClause()) clause->setPrimaryKeyClause(visit(ctx->primaryKeyClause()));
    if (ctx->sampleByClause()) clause->setSampleByClause(visit(ctx->sampleByClause()));
    if (ctx->ttlClause()) clause->setTTLClause(visit(ctx->ttlClause()));
    if (ctx->settingsClause()) clause->setSettingsClause(visit(ctx->settingsClause()));

    return clause;
}

antlrcpp::Any ParseTreeVisitor::visitEngineExpr(ClickHouseParser::EngineExprContext *ctx)
{
    PtrTo<Identifier> identifier
        = ctx->identifier() ? visit(ctx->identifier()).as<PtrTo<Identifier>>() : std::make_shared<Identifier>("Null");
    return std::make_shared<EngineExpr>(
        identifier, ctx->columnExprList() ? visit(ctx->columnExprList()).as<PtrTo<ColumnExprList>>() : nullptr);
}

antlrcpp::Any ParseTreeVisitor::visitPartitionByClause(ClickHouseParser::PartitionByClauseContext *ctx)
{
    return std::make_shared<PartitionByClause>(visit(ctx->columnExpr()).as<PtrTo<ColumnExpr>>());
}

antlrcpp::Any ParseTreeVisitor::visitPrimaryKeyClause(ClickHouseParser::PrimaryKeyClauseContext *ctx)
{
    return std::make_shared<PrimaryKeyClause>(visit(ctx->columnExpr()).as<PtrTo<ColumnExpr>>());
}

antlrcpp::Any ParseTreeVisitor::visitSampleByClause(ClickHouseParser::SampleByClauseContext *ctx)
{
    return std::make_shared<SampleByClause>(visit(ctx->columnExpr()).as<PtrTo<ColumnExpr>>());
}

antlrcpp::Any ParseTreeVisitor::visitTtlClause(ClickHouseParser::TtlClauseContext *ctx)
{
    auto list = std::make_shared<TTLExprList>();
    for (auto * expr : ctx->ttlExpr()) list->append(expr->accept(this));
    return std::make_shared<TTLClause>(list);
}

antlrcpp::Any ParseTreeVisitor::visitTtlExpr(ClickHouseParser::TtlExprContext *ctx)
{
    TTLExpr::TTLType type;
    PtrTo<StringLiteral> literal;

    if (ctx->DELETE()) type = TTLExpr::TTLType::DELETE;
    else if (ctx->DISK()) type = TTLExpr::TTLType::TO_DISK;
    else if (ctx->VOLUME()) type = TTLExpr::TTLType::TO_VOLUME;
    else __builtin_unreachable();

    if (ctx->STRING_LITERAL()) literal = Literal::createString(ctx->STRING_LITERAL());

    return std::make_shared<TTLExpr>(visit(ctx->columnExpr()), type, literal);
}

}
