#include <Parsers/New/AST/Identifier.h>

#include <IO/ReadHelpers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/New/ParseTreeVisitor.h>


namespace DB::AST
{

Identifier::Identifier(const String & name_) : name(name_)
{
    if (name.front() == '`' || name.front() == '"')
    {
        String s;
        ReadBufferFromMemory in(name.data(), name.size());

        if (name.front() == '`')
            readBackQuotedStringWithSQLStyle(s, in);
        else
            readDoubleQuotedStringWithSQLStyle(s, in);

        assert(in.count() == name.size());
        name = s;
    }
}

Identifier::Identifier(const String & name_, const String & nested_name) : name(name_ + "." + nested_name)
{
}

ASTPtr Identifier::convertToOld() const
{
    return std::make_shared<ASTIdentifier>(getQualifiedName());
}

String Identifier::toString() const
{
    return getQualifiedName();
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

ASTPtr TableIdentifier::convertToOld() const
{
    std::vector<String> parts;

    if (db && !db->getName().empty()) parts.push_back(db->getName());
    parts.push_back(getName());

    return std::make_shared<ASTIdentifier>(std::move(parts));
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

ASTPtr ColumnIdentifier::convertToOld() const
{
    std::vector<String> parts;

    if (table)
    {
        if (table->getDatabase()) parts.push_back(table->getDatabase()->getName());
        parts.push_back(table->getName());
    }
    parts.push_back(getName());

    return std::make_shared<ASTIdentifier>(std::move(parts));
}

}

namespace DB
{

using namespace AST;

antlrcpp::Any ParseTreeVisitor::visitAlias(ClickHouseParser::AliasContext *ctx)
{
    if (ctx->IDENTIFIER()) return std::make_shared<Identifier>(ctx->IDENTIFIER()->getText());
    if (ctx->keywordForAlias()) return std::make_shared<Identifier>(ctx->keywordForAlias()->getText());
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitColumnIdentifier(ClickHouseParser::ColumnIdentifierContext *ctx)
{
    auto table = ctx->tableIdentifier() ? visit(ctx->tableIdentifier()).as<PtrTo<TableIdentifier>>() : nullptr;
    return std::make_shared<ColumnIdentifier>(table, visit(ctx->nestedIdentifier()));
}

antlrcpp::Any ParseTreeVisitor::visitDatabaseIdentifier(ClickHouseParser::DatabaseIdentifierContext *ctx)
{
    return std::make_shared<DatabaseIdentifier>(visit(ctx->identifier()).as<PtrTo<Identifier>>());
}

antlrcpp::Any ParseTreeVisitor::visitIdentifier(ClickHouseParser::IdentifierContext *ctx)
{
    if (ctx->IDENTIFIER()) return std::make_shared<Identifier>(ctx->IDENTIFIER()->getText());
    if (ctx->interval()) return std::make_shared<Identifier>(ctx->interval()->getText());
    if (ctx->keyword()) return std::make_shared<Identifier>(ctx->keyword()->getText());
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitIdentifierOrNull(ClickHouseParser::IdentifierOrNullContext *ctx)
{
    if (ctx->identifier()) return visit(ctx->identifier());
    if (ctx->NULL_SQL())
    {
        if (ctx->NULL_SQL()->getSymbol()->getText() == "Null") return std::make_shared<Identifier>("Null");
        else {
            // TODO: raise error
        }
    }
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitInterval(ClickHouseParser::IntervalContext *)
{
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitKeyword(ClickHouseParser::KeywordContext *)
{
    __builtin_unreachable();
}

antlrcpp::Any ParseTreeVisitor::visitKeywordForAlias(ClickHouseParser::KeywordForAliasContext *)
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

antlrcpp::Any ParseTreeVisitor::visitTableIdentifier(ClickHouseParser::TableIdentifierContext *ctx)
{
    auto database = ctx->databaseIdentifier() ? visit(ctx->databaseIdentifier()).as<PtrTo<DatabaseIdentifier>>() : nullptr;
    return std::make_shared<TableIdentifier>(database, visit(ctx->identifier()));
}

}
