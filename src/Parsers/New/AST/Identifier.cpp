#include <memory>
#include <Parsers/New/AST/Identifier.h>

#include <Parsers/New/ParseTreeVisitor.h>

#include <Parsers/ASTIdentifier.h>
#include "Parsers/New/AST/INode.h"


namespace DB::AST
{

Identifier::Identifier(const std::string & name_) : name(name_)
{
}

ASTPtr Identifier::convertToOld() const
{
    return std::make_shared<ASTIdentifier>(getQualifiedName());
}

DatabaseIdentifier::DatabaseIdentifier(PtrTo<Identifier> name) : Identifier(*name)
{
}

TableIdentifier::TableIdentifier(PtrTo<DatabaseIdentifier> database, PtrTo<Identifier> name) : Identifier(*name), db(database)
{
}

ColumnIdentifier::ColumnIdentifier(PtrTo<TableIdentifier> table_, PtrTo<Identifier> name) : Identifier(*name), table(table_)
{
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitDatabaseIdentifier(ClickHouseParser::DatabaseIdentifierContext *ctx)
{
    return std::make_shared<DatabaseIdentifier>(ctx->identifier()->accept(this).as<PtrTo<Identifier>>());
}

antlrcpp::Any ParseTreeVisitor::visitTableIdentifier(ClickHouseParser::TableIdentifierContext *ctx)
{
    // TODO: not complete!
    return std::make_shared<TableIdentifier>(
        ctx->databaseIdentifier() ? ctx->databaseIdentifier()->accept(this).as<PtrTo<DatabaseIdentifier>>() : nullptr,
        ctx->identifier()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitColumnIdentifier(ClickHouseParser::ColumnIdentifierContext *ctx)
{
    return std::make_shared<ColumnIdentifier>(
        ctx->tableIdentifier() ? ctx->tableIdentifier()->accept(this).as<PtrTo<TableIdentifier>>() : nullptr,
        ctx->identifier()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitIdentifier(ClickHouseParser::IdentifierContext *ctx)
{
    if (ctx->IDENTIFIER()) return std::make_shared<Identifier>(ctx->IDENTIFIER()->getText());
    if (ctx->INTERVAL_TYPE()) return std::make_shared<Identifier>(ctx->INTERVAL_TYPE()->getText());
    if (ctx->keyword()) return std::make_shared<Identifier>(ctx->keyword()->getText());
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitKeyword(ClickHouseParser::KeywordContext *)
{
    __builtin_unreachable();
}

}
