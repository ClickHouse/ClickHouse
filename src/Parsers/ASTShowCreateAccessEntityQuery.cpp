#include <Parsers/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/ASTRowPolicyName.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
namespace
{
    using EntityType = IAccessEntity::Type;
    using EntityTypeInfo = IAccessEntity::TypeInfo;

    void formatNames(const Strings & names, const IAST::FormatSettings & settings)
    {
        bool need_comma = false;
        for (const auto & name : names)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ',';
            settings.ostr << ' ' << backQuoteIfNeed(name);
        }
    }
}


String ASTShowCreateAccessEntityQuery::getKeyword() const
{
    size_t total_count = (names.size()) + (row_policy_names ? row_policy_names->size() : 0) + current_user + current_quota;
    bool multiple = (total_count != 1) || all || !short_name.empty() || database_and_table_name;
    const auto & type_info = EntityTypeInfo::get(type);
    return multiple ? type_info.plural_name : type_info.name;
}


String ASTShowCreateAccessEntityQuery::getID(char) const
{
    return String("SHOW CREATE ") + getKeyword() + " query";
}


ASTPtr ASTShowCreateAccessEntityQuery::clone() const
{
    return std::make_shared<ASTShowCreateAccessEntityQuery>(*this);
}


void ASTShowCreateAccessEntityQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW CREATE " << getKeyword() << (settings.hilite ? hilite_none : "");

    if (!names.empty())
        formatNames(names, settings);

    if (row_policy_names)
    {
        settings.ostr << " ";
        row_policy_names->format(settings);
    }

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


void ASTShowCreateAccessEntityQuery::replaceEmptyDatabase(const String & current_database)
{
    if (row_policy_names)
        row_policy_names->replaceEmptyDatabase(current_database);

    if (database_and_table_name)
    {
        String & database = database_and_table_name->first;
        if (database.empty())
            database = current_database;
    }
}

}
