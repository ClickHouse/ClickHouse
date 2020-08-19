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
    children.insert(children.end(), args->begin(), args->end());
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
    auto clause = std::make_shared<EngineClause>(ctx->engineExpr()->accept(this).as<PtrTo<EngineExpr>>());

    if (ctx->orderByClause()) clause->setOrderByClause(ctx->orderByClause()->accept(this));
    if (ctx->partitionByClause()) clause->setPartitionByClause(ctx->partitionByClause()->accept(this));
    if (ctx->primaryKeyClause()) clause->setPrimaryKeyClause(ctx->primaryKeyClause()->accept(this));
    if (ctx->sampleByClause()) clause->setSampleByClause(ctx->sampleByClause()->accept(this));
    if (ctx->ttlClause()) clause->setTTLClause(ctx->ttlClause()->accept(this));
    if (ctx->settingsClause()) clause->setSettingsClause(ctx->settingsClause()->accept(this));

    return clause;
}

antlrcpp::Any ParseTreeVisitor::visitEngineExpr(ClickHouseParser::EngineExprContext *ctx)
{
    return std::make_shared<EngineExpr>(
        ctx->identifier()->accept(this), ctx->columnExprList() ? ctx->columnExprList()->accept(this).as<PtrTo<ColumnExprList>>() : nullptr);
}

antlrcpp::Any ParseTreeVisitor::visitPartitionByClause(ClickHouseParser::PartitionByClauseContext *ctx)
{
    return std::make_shared<PartitionByClause>(ctx->columnExpr()->accept(this).as<PtrTo<ColumnExpr>>());
}

antlrcpp::Any ParseTreeVisitor::visitPrimaryKeyClause(ClickHouseParser::PrimaryKeyClauseContext *ctx)
{
    return std::make_shared<PrimaryKeyClause>(ctx->columnExpr()->accept(this).as<PtrTo<ColumnExpr>>());
}

antlrcpp::Any ParseTreeVisitor::visitSampleByClause(ClickHouseParser::SampleByClauseContext *ctx)
{
    return std::make_shared<SampleByClause>(ctx->columnExpr()->accept(this).as<PtrTo<ColumnExpr>>());
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

    return std::make_shared<TTLExpr>(ctx->columnExpr()->accept(this), type, literal);
}

}
