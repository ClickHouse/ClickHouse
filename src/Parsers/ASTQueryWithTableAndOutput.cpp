#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>


namespace DB
{

String ASTQueryWithTableAndOutput::getDatabase() const
{
    String name;
    tryGetIdentifierNameInto(database, name);
    return name;
}

String ASTQueryWithTableAndOutput::getTable() const
{
    String name;
    tryGetIdentifierNameInto(table, name);
    return name;
}

void ASTQueryWithTableAndOutput::setDatabase(const String & name)
{
    reset(database);
    if (!name.empty())
        set(database, make_intrusive<ASTIdentifier>(name));
}

void ASTQueryWithTableAndOutput::setTable(const String & name)
{
    reset(table);
    if (!name.empty())
        set(table, make_intrusive<ASTIdentifier>(name));
}

static ASTPtr makeQuotedIdentifier(const String & name, IdentifierPartQuote quote)
{
    IdentifierName parts(std::vector<String>{name});
    parts.front().quote = quote;
    return make_intrusive<ASTIdentifier>(std::move(parts));
}

void ASTQueryWithTableAndOutput::setDatabase(const String & name, IdentifierPartQuote quote)
{
    reset(database);
    if (!name.empty())
        set(database, makeQuotedIdentifier(name, quote));
}

void ASTQueryWithTableAndOutput::setTable(const String & name, IdentifierPartQuote quote)
{
    reset(table);
    if (!name.empty())
        set(table, makeQuotedIdentifier(name, quote));
}

void ASTQueryWithTableAndOutput::cloneTableOptions(ASTQueryWithTableAndOutput & cloned) const
{
    if (database)
        cloned.set(cloned.database, database->clone());
    if (table)
        cloned.set(cloned.table, table->clone());
}

}
