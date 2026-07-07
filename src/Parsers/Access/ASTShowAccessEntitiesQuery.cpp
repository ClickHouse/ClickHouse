#include <Parsers/Access/ASTShowAccessEntitiesQuery.h>
#include <Parsers/Access/parseAccessEntityName.h>
#include <Common/SipHash.h>
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

void ASTShowAccessEntitiesQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    /// Fold the semantic fields kept outside `children`. See the header comment for why the
    /// rewrite-rule matcher needs this. Each field is produced by the formatter, so it survives the
    /// format -> parse round-trip that the debug-build AST consistency check requires.
    hash_state.update(short_name);
    hash_state.update(database_and_table_name.has_value());
    if (database_and_table_name)
    {
        hash_state.update(database_and_table_name->first);
        hash_state.update(database_and_table_name->second);
    }
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

}
