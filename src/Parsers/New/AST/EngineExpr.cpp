#include <Parsers/New/AST/EngineExpr.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTTTLElement.h>
#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/ParseTreeVisitor.h>
#include <Storages/DataDestinationType.h>
#include <Storages/TTLMode.h>


namespace DB::ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
}

namespace DB::AST
{

EngineClause::EngineClause(PtrTo<EngineExpr> expr) : INode(MAX_INDEX)
{
    set(ENGINE, expr);
}

void EngineClause::setOrderByClause(PtrTo<OrderByClause> clause)
{
    set(ORDER_BY, clause);
}

void EngineClause::setPartitionByClause(PtrTo<PartitionByClause> clause)
{
    set(PARTITION_BY, clause);
}

void EngineClause::setPrimaryKeyClause(PtrTo<PrimaryKeyClause> clause)
{
    set(PRIMARY_KEY, clause);
}

void EngineClause::setSampleByClause(PtrTo<SampleByClause> clause)
{
    set(SAMPLE_BY, clause);
}

void EngineClause::setTTLClause(PtrTo<TTLClause> clause)
{
    set(TTL, clause);
}

void EngineClause::setSettingsClause(PtrTo<SettingsClause> clause)
{
    set(SETTINGS, clause);
}

ASTPtr EngineClause::convertToOld() const
{
    auto storage = std::make_shared<ASTStorage>();

    storage->set(storage->engine, get(ENGINE)->convertToOld());
    if (has(PARTITION_BY)) storage->set(storage->partition_by, get(PARTITION_BY)->convertToOld());
    if (has(PRIMARY_KEY)) storage->set(storage->primary_key, get(PRIMARY_KEY)->convertToOld());
    if (has(ORDER_BY))
    {
        /// XXX: old parser used very strange grammar for this case, instead of using OrderByElement's.
        auto expr_list = get(ORDER_BY)->convertToOld();
        if (expr_list->children.size() > 1)
            throw DB::Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Cannot convert multiple ORDER expression to old AST");
        storage->set(storage->order_by, expr_list->children[0]->children[0]);
    }
    if (has(SAMPLE_BY)) storage->set(storage->sample_by, get(SAMPLE_BY)->convertToOld());
    if (has(TTL)) storage->set(storage->ttl_table, get(TTL)->convertToOld());
    if (has(SETTINGS))
    {
        storage->set(storage->settings, get(SETTINGS)->convertToOld());
        storage->settings->is_standalone = false;
    }

    return storage;
}

EngineExpr::EngineExpr(PtrTo<Identifier> identifier, PtrTo<ColumnExprList> args) : INode{identifier, args}
{
}

ASTPtr EngineExpr::convertToOld() const
{
    auto expr = std::make_shared<ASTFunction>();

    expr->name = get<Identifier>(NAME)->getName();
    expr->no_empty_args = true;
    if (has(ARGS))
    {
        expr->arguments = get(ARGS)->convertToOld();
        expr->children.push_back(expr->arguments);
    }

    return expr;
}

TTLExpr::TTLExpr(PtrTo<ColumnExpr> expr, TTLType type, PtrTo<StringLiteral> literal) : INode{expr, literal}, ttl_type(type)
{
}

ASTPtr TTLExpr::convertToOld() const
{
    TTLMode mode = TTLMode::DELETE;
    DataDestinationType destination_type = DataDestinationType::DELETE;
    String destination_name;

    switch(ttl_type)
    {
        case TTLType::DELETE:
            mode = TTLMode::DELETE;
            destination_type = DataDestinationType::DELETE;
            break;
        case TTLType::TO_DISK:
            mode = TTLMode::MOVE;
            destination_type = DataDestinationType::DISK;
            destination_name = get(TYPE)->convertToOld()->as<ASTLiteral>()->value.get<String>();
            break;
        case TTLType::TO_VOLUME:
            mode = TTLMode::MOVE;
            destination_type = DataDestinationType::VOLUME;
            destination_name = get(TYPE)->convertToOld()->as<ASTLiteral>()->value.get<String>();
            break;
    }

    auto expr = std::make_shared<ASTTTLElement>(mode, destination_type, destination_name);
    expr->setTTL(get(EXPR)->convertToOld());
    return expr;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitEngineClause(ClickHouseParser::EngineClauseContext *ctx)
{
    auto clause = std::make_shared<EngineClause>(visit(ctx->engineExpr()).as<PtrTo<EngineExpr>>());

    if (!ctx->orderByClause().empty()) clause->setOrderByClause(visit(ctx->orderByClause(0)));
    if (!ctx->partitionByClause().empty()) clause->setPartitionByClause(visit(ctx->partitionByClause(0)));
    if (!ctx->primaryKeyClause().empty()) clause->setPrimaryKeyClause(visit(ctx->primaryKeyClause(0)));
    if (!ctx->sampleByClause().empty()) clause->setSampleByClause(visit(ctx->sampleByClause(0)));
    if (!ctx->ttlClause().empty()) clause->setTTLClause(visit(ctx->ttlClause(0)));
    if (!ctx->settingsClause().empty()) clause->setSettingsClause(visit(ctx->settingsClause(0)));

    return clause;
}

antlrcpp::Any ParseTreeVisitor::visitEngineExpr(ClickHouseParser::EngineExprContext *ctx)
{
    auto list = ctx->columnExprList() ? visit(ctx->columnExprList()).as<PtrTo<ColumnExprList>>() : nullptr;
    return std::make_shared<EngineExpr>(visit(ctx->identifierOrNull()), list);
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
    for (auto * expr : ctx->ttlExpr()) list->push(visit(expr));
    return std::make_shared<TTLClause>(list);
}

antlrcpp::Any ParseTreeVisitor::visitTtlExpr(ClickHouseParser::TtlExprContext *ctx)
{
    TTLExpr::TTLType type;
    PtrTo<StringLiteral> literal;

    if (ctx->DISK()) type = TTLExpr::TTLType::TO_DISK;
    else if (ctx->VOLUME()) type = TTLExpr::TTLType::TO_VOLUME;
    else type = TTLExpr::TTLType::DELETE;

    if (ctx->STRING_LITERAL()) literal = Literal::createString(ctx->STRING_LITERAL());

    return std::make_shared<TTLExpr>(visit(ctx->columnExpr()), type, literal);
}

}
