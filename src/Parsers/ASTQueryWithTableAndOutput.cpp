#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


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
    if (name.empty())
        database.reset();
    else
        database = std::make_shared<ASTIdentifier>(name);
}

void ASTQueryWithTableAndOutput::setTable(const String & name)
{
    if (name.empty())
        table.reset();
    else
        table = std::make_shared<ASTIdentifier>(name);
}

void ASTQueryWithTableAndOutput::cloneTableOptions(ASTQueryWithTableAndOutput & cloned) const
{
    if (database)
    {
        cloned.database = database->clone();
        cloned.children.push_back(cloned.database);
    }
    if (table)
    {
        cloned.table = table->clone();
        cloned.children.push_back(cloned.table);
    }
}
void ASTQueryWithTableAndOutput::formatHelper(const FormatSettings & settings, const char * name) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << name << " " << (settings.hilite ? hilite_none : "");
    settings.ostr << (!getDatabase().empty() ? backQuoteIfNeed(getDatabase()) + "." : "") << backQuoteIfNeed(getTable());
}

}

