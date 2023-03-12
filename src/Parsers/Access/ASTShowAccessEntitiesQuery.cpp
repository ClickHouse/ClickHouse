#include <Parsers/Access/ASTShowAccessEntitiesQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <fmt/format.h>


namespace DB
{

String ASTShowAccessEntitiesQuery::getKeyword() const
{
    if (current_quota)
        return "CURRENT QUOTA";
    if (current_roles)
        return "CURRENT ROLES";
    if (enabled_roles)
        return "ENABLED ROLES";
    return AccessEntityTypeInfo::get(type).plural_name;
}


String ASTShowAccessEntitiesQuery::getID(char) const
{
    return fmt::format("SHOW {} query", getKeyword());
}

void ASTShowAccessEntitiesQuery::formatQueryImpl(const FormattingBuffer & out) const
{
    out.writeKeyword("SHOW ");

    if (!short_name.empty())
        out.ostr << " " << backQuoteIfNeed(short_name);

    if (database_and_table_name)
    {
        const String & database = database_and_table_name->first;
        const String & table_name = database_and_table_name->second;
        out.writeKeyword(" ON ");
        out.ostr << (database.empty() ? "" : backQuoteIfNeed(database) + ".");
        out.ostr << (table_name.empty() ? "*" : backQuoteIfNeed(table_name));
    }
}


void ASTShowAccessEntitiesQuery::replaceEmptyDatabase(const String & current_database)
{
    if (database_and_table_name)
    {
        String & database = database_and_table_name->first;
        if (database.empty())
            database = current_database;
    }
}

}
