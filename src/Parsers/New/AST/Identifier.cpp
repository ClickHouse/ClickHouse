#include <Parsers/New/AST/Identifier.h>

#include <Parsers/New/ParseTreeVisitor.h>

#include <Parsers/ASTIdentifier.h>


namespace DB::AST
{

Identifier::Identifier(const String & name_) : name(name_)
{
}

Identifier::Identifier(const String & name_, const String & nested_name) : name(name_ + "." + nested_name)
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

ColumnIdentifier::ColumnIdentifier(PtrTo<TableIdentifier> table_, PtrTo<Identifier> name) : Identifier(name->getName()), table(table_)
{
}

void ColumnIdentifier::makeCompound() const
{
    if (table)
    {
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
    auto database = ctx->databaseIdentifier() ? visit(ctx->databaseIdentifier()).as<PtrTo<DatabaseIdentifier>>() : nullptr;
    return std::make_shared<TableIdentifier>(database, visit(ctx->identifier()));
}

antlrcpp::Any ParseTreeVisitor::visitColumnIdentifier(ClickHouseParser::ColumnIdentifierContext *ctx)
{
    auto table = ctx->tableIdentifier() ? visit(ctx->tableIdentifier()).as<PtrTo<TableIdentifier>>() : nullptr;
    return std::make_shared<ColumnIdentifier>(table, visit(ctx->nestedIdentifier()));
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

antlrcpp::Any ParseTreeVisitor::visitNestedIdentifier(ClickHouseParser::NestedIdentifierContext *ctx)
{
    if (ctx->identifier().size() == 2)
    {
        auto name1 = visit(ctx->identifier(0)).as<PtrTo<Identifier>>()->getName();
        auto name2 = visit(ctx->identifier(1)).as<PtrTo<Identifier>>()->getName();
        return std::make_shared<Identifier>(name1, name2);
    }
    else return visit(ctx->identifier(0));
}

}
