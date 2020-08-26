#include <Parsers/New/AST/Identifier.h>

#include <Parsers/New/ParseTreeVisitor.h>

#include <Parsers/ASTIdentifier.h>


namespace DB::AST
{

Identifier::Identifier(const String & name_) : name(name_)
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

void TableIdentifier::makeCompound() const
{
    if (db)
    {
        name = db->getName();
        db.reset();
    }
}

ColumnIdentifier::ColumnIdentifier(PtrTo<TableIdentifier> table_, PtrTo<Identifier> name, PtrTo<Identifier> nested_)
    : Identifier(name->getName() + (nested_ ? "." + nested_->getName() : String())), table(table_), nested(nested_)
{
}

void ColumnIdentifier::makeCompound() const
{
    if (table && !nested)
    {
        nested = std::make_shared<Identifier>(getName());
        name = table->getName() + "." + getName();
        if (table->getDatabase()) table->makeCompound();
        else table.reset();
    }
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitDatabaseIdentifier(ClickHouseParser::DatabaseIdentifierContext *ctx)
{
    return std::make_shared<DatabaseIdentifier>(visit(ctx->identifier()).as<PtrTo<Identifier>>());
}

antlrcpp::Any ParseTreeVisitor::visitTableIdentifier(ClickHouseParser::TableIdentifierContext *ctx)
{
    // TODO: not complete!
    return std::make_shared<TableIdentifier>(
        ctx->databaseIdentifier() ? visit(ctx->databaseIdentifier()).as<PtrTo<DatabaseIdentifier>>() : nullptr,
        visit(ctx->identifier()));
}

antlrcpp::Any ParseTreeVisitor::visitColumnIdentifier(ClickHouseParser::ColumnIdentifierContext *ctx)
{
    return std::make_shared<ColumnIdentifier>(
        ctx->tableIdentifier() ? visit(ctx->tableIdentifier()).as<PtrTo<TableIdentifier>>() : nullptr,
        ctx->identifier(0)->accept(this),
        ctx->identifier().size() == 2 ? ctx->identifier(1)->accept(this).as<PtrTo<Identifier>>() : nullptr);
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
