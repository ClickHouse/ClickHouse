#include <Parsers/New/AST/Identifier.h>

#include <Parsers/New/ParseTreeVisitor.h>

#include <Parsers/ASTIdentifier.h>


namespace DB::AST
{

Identifier::Identifier(const std::string & name_) : name(name_)
{
}

TableIdentifier::TableIdentifier(PtrTo<DatabaseIdentifier> database, PtrTo<Identifier> name_) : Identifier(*name_), db(database)
{
}

ASTPtr TableIdentifier::convertToOld() const
{
    return std::make_shared<ASTIdentifier>((db ? db->getName() + "." : String()) + getName());
}

}

namespace DB
{

antlrcpp::Any ParseTreeVisitor::visitTableIdentifier(ClickHouseParser::TableIdentifierContext *ctx)
{
    // TODO: not complete!
    return std::make_shared<AST::TableIdentifier>(
        ctx->databaseIdentifier() ? ctx->databaseIdentifier()->accept(this).as<AST::PtrTo<AST::DatabaseIdentifier>>() : nullptr,
        ctx->identifier()->accept(this).as<AST::PtrTo<AST::Identifier>>());
}

antlrcpp::Any ParseTreeVisitor::visitIdentifier(ClickHouseParser::IdentifierContext *ctx)
{
    return std::make_shared<AST::Identifier>(ctx->IDENTIFIER()->getText());
}

}
