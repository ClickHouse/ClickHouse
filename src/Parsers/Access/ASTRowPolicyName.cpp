#include <Parsers/Access/ASTRowPolicyName.h>
#include <IO/Operators.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


void ASTRowPolicyName::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    const String & database = name_parts.database;
    const String & table_name = name_parts.table_name;
    const String & short_name = name_parts.short_name;
    settings.ostr << backQuoteIfNeed(short_name) << (settings.hilite ? hilite_keyword : "") << " ON "
                  << (settings.hilite ? hilite_none : "") << (database.empty() ? String{} : backQuoteIfNeed(database) + ".")
                  << backQuoteIfNeed(table_name);

    formatOnCluster(settings);
}


void ASTRowPolicyName::replaceEmptyDatabase(const String & current_database)
{
    if (name_parts.database.empty())
        name_parts.database = current_database;
}


void ASTRowPolicyNames::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (name_parts.empty())
        throw Exception("No names of row policies in AST", ErrorCodes::LOGICAL_ERROR);

    bool same_short_name = true;
    if (name_parts.size() > 1)
    {
        for (size_t i = 1; i != name_parts.size(); ++i)
            if (name_parts[i].short_name != name_parts[0].short_name)
            {
                same_short_name = false;
                break;
            }
    }

    bool same_db_and_table_name = true;
    if (name_parts.size() > 1)
    {
        for (size_t i = 1; i != name_parts.size(); ++i)
            if ((name_parts[i].database != name_parts[0].database) || (name_parts[i].table_name != name_parts[0].table_name))
            {
                same_db_and_table_name = false;
                break;
            }
    }

    if (same_short_name)
    {
        const String & short_name = name_parts[0].short_name;
        settings.ostr << backQuoteIfNeed(short_name) << (settings.hilite ? hilite_keyword : "") << " ON "
                      << (settings.hilite ? hilite_none : "");

        bool need_comma = false;
        for (const auto & np : name_parts)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            const String & database = np.database;
            const String & table_name = np.table_name;
            if (!database.empty())
                settings.ostr << backQuoteIfNeed(database) + ".";
            settings.ostr << backQuoteIfNeed(table_name);
        }
    }
    else if (same_db_and_table_name)
    {
        bool need_comma = false;
        for (const auto & np : name_parts)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            const String & short_name = np.short_name;
            settings.ostr << backQuoteIfNeed(short_name);
        }

        const String & database = name_parts[0].database;
        const String & table_name = name_parts[0].table_name;
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " ON " << (settings.hilite ? hilite_none : "");
        if (!database.empty())
            settings.ostr << backQuoteIfNeed(database) + ".";
        settings.ostr << backQuoteIfNeed(table_name);
    }
    else
    {
        bool need_comma = false;
        for (const auto & np : name_parts)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            const String & short_name = np.short_name;
            const String & database = np.database;
            const String & table_name = np.table_name;
            settings.ostr << backQuoteIfNeed(short_name) << (settings.hilite ? hilite_keyword : "") << " ON "
                          << (settings.hilite ? hilite_none : "");
            if (!database.empty())
                settings.ostr << backQuoteIfNeed(database) + ".";
            settings.ostr << backQuoteIfNeed(table_name);
        }
    }

    formatOnCluster(settings);
}


Strings ASTRowPolicyNames::toStrings() const
{
    Strings res;
    res.reserve(name_parts.size());
    for (const auto & np : name_parts)
        res.emplace_back(np.toString());
    return res;
}


void ASTRowPolicyNames::replaceEmptyDatabase(const String & current_database)
{
    for (auto & np : name_parts)
        if (np.database.empty())
            np.database = current_database;
}

}
