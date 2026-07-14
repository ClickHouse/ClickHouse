#include <Parsers/Access/ASTShowAccessEntitiesQuery.h>
#include <Parsers/Access/parseAccessEntityName.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <fmt/format.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

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

void ASTShowAccessEntitiesQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    ostr << "SHOW " << getKeyword();

    if (!short_name.empty())
        ostr << " " << backQuoteAccessEntityNameIfNeed(short_name);

    if (database_and_table_name)
    {
        const String & database = database_and_table_name->first;
        const String & table_name = database_and_table_name->second;
        ostr << " ON ";
        ostr << (database.empty() ? "" : backQuoteIfNeed(database) + ".");
        ostr << (table_name.empty() ? "*" : backQuoteIfNeed(table_name));
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

void ASTShowAccessEntitiesQuery::replaceEmptyDatabase(const CurrentDatabaseInfo & current_database)
{
    if (database_and_table_name)
    {
        String & database = database_and_table_name->first;
        String & table = database_and_table_name->second;
        if (database.empty())
        {
            /// there is no namespace-scoped wildcard: ON * would widen to the whole database
            if (!current_database.table_prefix.empty() && table.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "ON * is not supported while a namespace is selected "
                    "(it would target the whole database {}); specify a table "
                    "or use the database without a namespace", backQuoteIfNeed(current_database.database));
            database = current_database.database;
            /// USE db.namespace: an unqualified policy target is the namespace-qualified table.
            if (!current_database.table_prefix.empty() && !table.empty())
                table = current_database.table_prefix + "." + table;
        }
    }
}

}
