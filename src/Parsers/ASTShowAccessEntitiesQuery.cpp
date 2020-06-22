#include <Parsers/ASTShowAccessEntitiesQuery.h>
#include <Common/quoteString.h>


namespace DB
{
using EntityTypeInfo = IAccessEntity::TypeInfo;


String ASTShowAccessEntitiesQuery::getKeyword() const
{
    if (current_quota)
        return "CURRENT QUOTA";
    if (current_roles)
        return "CURRENT ROLES";
    if (enabled_roles)
        return "ENABLED ROLES";
    return EntityTypeInfo::get(type).plural_name;
}


String ASTShowAccessEntitiesQuery::getID(char) const
{
    return "SHOW " + String(getKeyword()) + " query";
}

void ASTShowAccessEntitiesQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW " << getKeyword() << (settings.hilite ? hilite_none : "");

    if (!short_name.empty())
        settings.ostr << " " << backQuoteIfNeed(short_name);

    if (database_and_table_name)
    {
        const String & database = database_and_table_name->first;
        const String & table_name = database_and_table_name->second;
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " ON " << (settings.hilite ? hilite_none : "");
        settings.ostr << (database.empty() ? "" : backQuoteIfNeed(database) + ".");
        settings.ostr << (table_name.empty() ? "*" : backQuoteIfNeed(table_name));
    }
}


void ASTShowAccessEntitiesQuery::replaceEmptyDatabaseWithCurrent(const String & current_database)
{
    if (database_and_table_name)
    {
        String & database = database_and_table_name->first;
        if (database.empty())
            database = current_database;
    }
}

}
