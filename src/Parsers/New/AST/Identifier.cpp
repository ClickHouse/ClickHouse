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

antlrcpp::Any ParseTreeVisitor::visitDatabaseIdentifier(ClickHouseParser::DatabaseIdentifierContext *ctx)
{
    return std::make_shared<AST::DatabaseIdentifier>(ctx->identifier()->accept(this).as<AST::PtrTo<AST::Identifier>>());
}

antlrcpp::Any ParseTreeVisitor::visitTableIdentifier(ClickHouseParser::TableIdentifierContext *ctx)
{
    // TODO: not complete!
    return std::make_shared<AST::TableIdentifier>(
        ctx->databaseIdentifier() ? ctx->databaseIdentifier()->accept(this).as<AST::PtrTo<AST::DatabaseIdentifier>>() : nullptr,
        ctx->identifier()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitColumnIdentifier(ClickHouseParser::ColumnIdentifierContext *ctx)
{
    return std::make_shared<AST::ColumnIdentifier>(
        ctx->tableIdentifier() ? ctx->tableIdentifier()->accept(this).as<AST::PtrTo<AST::TableIdentifier>>() : nullptr,
        ctx->identifier()->accept(this));
}

antlrcpp::Any ParseTreeVisitor::visitIdentifier(ClickHouseParser::IdentifierContext *ctx)
{
    return std::make_shared<AST::Identifier>(ctx->IDENTIFIER()->getText());
}

}
