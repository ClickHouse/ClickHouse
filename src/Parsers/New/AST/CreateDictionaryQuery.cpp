#include <Parsers/New/AST/CreateDictionaryQuery.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/New/AST/ColumnExpr.h>
#include <Parsers/New/AST/ColumnTypeExpr.h>
#include <Parsers/New/AST/Identifier.h>
#include <Parsers/New/AST/Literal.h>
#include <Parsers/New/AST/SelectUnionQuery.h>
#include <Parsers/New/ParseTreeVisitor.h>

#include <Poco/String.h>


namespace DB::ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

namespace DB::AST
{

// DictionaryAttributeExpr

DictionaryAttributeExpr::DictionaryAttributeExpr(PtrTo<Identifier> identifier, PtrTo<ColumnTypeExpr> type) : INode{identifier, type}
{
}

void DictionaryAttributeExpr::setDefaultClause(PtrTo<Literal> literal)
{
    set(DEFAULT, literal);
}

void DictionaryAttributeExpr::setExpressionClause(PtrTo<ColumnExpr> expr)
{
    set(EXPRESSION, expr);
}

// DictionaryArgExpr

DictionaryArgExpr::DictionaryArgExpr(PtrTo<Identifier> identifier, PtrTo<ColumnExpr> expr) : INode{identifier, expr}
{
    if (expr->getType() != ColumnExpr::ExprType::LITERAL && expr->getType() != ColumnExpr::ExprType::IDENTIFIER
        && expr->getType() != ColumnExpr::ExprType::FUNCTION)
        throw DB::Exception(ErrorCodes::SYNTAX_ERROR, "Expected literal, identifier or function");
}

ASTPtr DictionaryArgExpr::convertToOld() const
{
    auto expr = std::make_shared<ASTPair>(true);  // FIXME: always true?

    // TODO: probably there are more variants to parse.

    expr->first = Poco::toLower(get(KEY)->as<Identifier>()->getName());
    expr->set(expr->second, get(VALUE)->convertToOld());

    return expr;
}

// SourceClause

SourceClause::SourceClause(PtrTo<Identifier> identifier, PtrTo<DictionaryArgList> list) : INode{identifier, list}
{
}

ASTPtr SourceClause::convertToOld() const
{
    auto clause = std::make_shared<ASTFunctionWithKeyValueArguments>(true);  // FIXME: always true?

    clause->name = Poco::toLower(get(NAME)->as<Identifier>()->getName());
    if (has(ARGS))
    {
        clause->elements = get(ARGS)->convertToOld();
        clause->children.push_back(clause->elements);
    }

    return clause;
}

// LifetimeClause

LifetimeClause::LifetimeClause(PtrTo<NumberLiteral> max, PtrTo<NumberLiteral> min) : INode{max, min}
{
}

ASTPtr LifetimeClause::convertToOld() const
{
    auto clause = std::make_shared<ASTDictionaryLifetime>();

    clause->max_sec = get(MAX)->convertToOld()->as<ASTLiteral>()->value.get<UInt64>();
    if (has(MIN)) clause->min_sec = get(MIN)->convertToOld()->as<ASTLiteral>()->value.get<UInt64>();

    return clause;
}

// LayoutClause

LayoutClause::LayoutClause(PtrTo<Identifier> identifier, PtrTo<DictionaryArgList> list) : INode{identifier, list}
{
}

ASTPtr LayoutClause::convertToOld() const
{
    auto clause = std::make_shared<ASTDictionaryLayout>();

    clause->layout_type = get(NAME)->as<Identifier>()->getName();
    clause->has_brackets = true;  // FIXME: maybe not?
    if (has(ARGS)) clause->set(clause->parameters, get(ARGS)->convertToOld());

    return clause;
}

// RangeClause

RangeClause::RangeClause(PtrTo<Identifier> max, PtrTo<Identifier> min) : INode{max, min}
{
}

ASTPtr RangeClause::convertToOld() const
{
    auto clause = std::make_shared<ASTDictionaryRange>();

    clause->max_attr_name = get(MAX)->as<Identifier>()->getName();
    clause->min_attr_name = get(MIN)->as<Identifier>()->getName();

    return clause;
}

// DictionaryEngineClause

DictionaryEngineClause::DictionaryEngineClause(PtrTo<PrimaryKeyClause> clause) : INode{clause}
{
}

void DictionaryEngineClause::setSourceClause(PtrTo<SourceClause> clause)
{
    set(SOURCE, clause);
}

void DictionaryEngineClause::setLifetimeClause(PtrTo<LifetimeClause> clause)
{
    set(LIFETIME, clause);
}

void DictionaryEngineClause::setLayoutClause(PtrTo<LayoutClause> clause)
{
    set(LAYOUT, clause);
}

void DictionaryEngineClause::setRangeClause(PtrTo<RangeClause> clause)
{
    set(RANGE, clause);
}

void DictionaryEngineClause::setSettingsClause(PtrTo<SettingsClause> clause)
{
    set(SETTINGS, clause);
}

ASTPtr DictionaryEngineClause::convertToOld() const
{
    auto clause = std::make_shared<ASTDictionary>();

    if (has(PRIMARY_KEY)) clause->set(clause->primary_key, get(PRIMARY_KEY)->convertToOld());
    if (has(SOURCE)) clause->set(clause->source, get(SOURCE)->convertToOld());
    if (has(LIFETIME)) clause->set(clause->lifetime, get(LIFETIME)->convertToOld());
    if (has(LAYOUT)) clause->set(clause->layout, get(LAYOUT)->convertToOld());
    if (has(RANGE)) clause->set(clause->range, get(RANGE)->convertToOld());
    if (has(SETTINGS)) clause->set(clause->dict_settings, get(SETTINGS)->convertToOld());

    return clause;
}

// CreateDictionaryQuery

CreateDictionaryQuery::CreateDictionaryQuery(
    PtrTo<ClusterClause> cluster,
    bool attach_,
    bool if_not_exists_,
    PtrTo<TableIdentifier> identifier,
    PtrTo<UUIDClause> uuid,
    PtrTo<DictionarySchemaClause> schema,
    PtrTo<DictionaryEngineClause> engine)
    : DDLQuery(cluster, {identifier, uuid, schema, engine}), attach(attach_), if_not_exists(if_not_exists_)
{
}

ASTPtr CreateDictionaryQuery::convertToOld() const
{
    auto query = std::make_shared<ASTCreateQuery>();

    {
        auto table_id = getTableIdentifier(get(NAME)->convertToOld());
        query->database = table_id.database_name;
        query->table = table_id.table_name;
        query->uuid
            = has(UUID) ? parseFromString<DB::UUID>(get(UUID)->convertToOld()->as<ASTLiteral>()->value.get<String>()) : table_id.uuid;
    }

    query->cluster = cluster_name;

    query->attach = attach;
    query->if_not_exists = if_not_exists;

    query->set(query->dictionary_attributes_list, get(SCHEMA)->convertToOld());
    query->set(query->dictionary, get(ENGINE)->convertToOld());

    return query;
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitCreateDictionaryStmt(ClickHouseParser::CreateDictionaryStmtContext *ctx)
{
    auto uuid = ctx->uuidClause() ? visit(ctx->uuidClause()).as<PtrTo<UUIDClause>>() : nullptr;
    auto cluster = ctx->clusterClause() ? visit(ctx->clusterClause()).as<PtrTo<ClusterClause>>() : nullptr;
    auto schema = ctx->dictionarySchemaClause() ? visit(ctx->dictionarySchemaClause()).as<PtrTo<DictionarySchemaClause>>() : nullptr;
    auto engine = ctx->dictionaryEngineClause() ? visit(ctx->dictionaryEngineClause()).as<PtrTo<DictionaryEngineClause>>() : nullptr;
    return std::make_shared<CreateDictionaryQuery>(
        cluster, !!ctx->ATTACH(), !!ctx->IF(), visit(ctx->tableIdentifier()), uuid, schema, engine);
}

antlrcpp::Any ParseTreeVisitor::visitDictionaryArgExpr(ClickHouseParser::DictionaryArgExprContext *ctx)
{
    return std::make_shared<DictionaryArgExpr>(visit(ctx->identifier()), visit(ctx->columnExpr()));
}

antlrcpp::Any ParseTreeVisitor::visitDictionaryAttrDfnt(ClickHouseParser::DictionaryAttrDfntContext *ctx)
{
    auto expr = std::make_shared<DictionaryAttributeExpr>(visit(ctx->identifier()), visit(ctx->columnTypeExpr()));
    if (!ctx->DEFAULT().empty()) expr->setDefaultClause(visit(ctx->literal(0)));
    if (!ctx->EXPRESSION().empty()) expr->setExpressionClause(visit(ctx->columnExpr(0)));
    if (!ctx->HIERARCHICAL().empty()) expr->setHierarchicalFlag();
    if (!ctx->INJECTIVE().empty()) expr->setInjectiveFlag();
    if (!ctx->IS_OBJECT_ID().empty()) expr->setIsObjectIdFlag();
    return expr;
}

antlrcpp::Any ParseTreeVisitor::visitDictionaryEngineClause(ClickHouseParser::DictionaryEngineClauseContext *ctx)
{
    auto clause = std::make_shared<DictionaryEngineClause>(visit(ctx->primaryKeyClause()));
    if (!ctx->sourceClause().empty()) clause->setSourceClause(visit(ctx->sourceClause(0)));
    if (!ctx->lifetimeClause().empty()) clause->setLifetimeClause(visit(ctx->lifetimeClause(0)));
    if (!ctx->layoutClause().empty()) clause->setLayoutClause(visit(ctx->layoutClause(0)));
    if (!ctx->rangeClause().empty()) clause->setRangeClause(visit(ctx->rangeClause(0)));
    if (!ctx->settingsClause().empty()) clause->setSettingsClause(visit(ctx->settingsClause(0)));
    return clause;
}

antlrcpp::Any ParseTreeVisitor::visitDictionarySchemaClause(ClickHouseParser::DictionarySchemaClauseContext *ctx)
{
    auto list = std::make_shared<DictionaryAttributeList>();
    for (auto * attr : ctx->dictionaryAttrDfnt()) list->push(visit(attr));
    return std::make_shared<DictionarySchemaClause>(list);
}

antlrcpp::Any ParseTreeVisitor::visitLayoutClause(ClickHouseParser::LayoutClauseContext *ctx)
{
    auto list = ctx->dictionaryArgExpr().empty() ? nullptr : std::make_shared<DictionaryArgList>();
    for (auto * arg : ctx->dictionaryArgExpr()) list->push(visit(arg));
    return std::make_shared<LayoutClause>(visit(ctx->identifier()), list);
}

antlrcpp::Any ParseTreeVisitor::visitLifetimeClause(ClickHouseParser::LifetimeClauseContext *ctx)
{
    if (ctx->DECIMAL_LITERAL().size() == 1) return std::make_shared<LifetimeClause>(Literal::createNumber(ctx->DECIMAL_LITERAL(0)));
    if (ctx->MAX()->getSymbol()->getTokenIndex() < ctx->MIN()->getSymbol()->getTokenIndex())
        return std::make_shared<LifetimeClause>(
            Literal::createNumber(ctx->DECIMAL_LITERAL(0)), Literal::createNumber(ctx->DECIMAL_LITERAL(1)));
    else
        return std::make_shared<LifetimeClause>(
            Literal::createNumber(ctx->DECIMAL_LITERAL(1)), Literal::createNumber(ctx->DECIMAL_LITERAL(0)));
}

antlrcpp::Any ParseTreeVisitor::visitRangeClause(ClickHouseParser::RangeClauseContext *ctx)
{
    if (ctx->MAX()->getSymbol()->getTokenIndex() < ctx->MIN()->getSymbol()->getTokenIndex())
        return std::make_shared<RangeClause>(visit(ctx->identifier(0)), visit(ctx->identifier(1)));
    else
        return std::make_shared<RangeClause>(visit(ctx->identifier(1)), visit(ctx->identifier(0)));
}

antlrcpp::Any ParseTreeVisitor::visitSourceClause(ClickHouseParser::SourceClauseContext *ctx)
{
    auto list = ctx->dictionaryArgExpr().empty() ? nullptr : std::make_shared<DictionaryArgList>();
    for (auto * arg : ctx->dictionaryArgExpr()) list->push(visit(arg));
    return std::make_shared<SourceClause>(visit(ctx->identifier()), list);
}

}
