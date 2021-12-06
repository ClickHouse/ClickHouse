#include <Parsers/Access/ASTRowPolicyName.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


void ASTRowPolicyName::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    const String & database = full_name.database;
    const String & table_name = full_name.table_name;
    const String & short_name = full_name.short_name;
    settings.ostr << backQuoteIfNeed(short_name) << (settings.hilite ? hilite_keyword : "") << " ON "
                  << (settings.hilite ? hilite_none : "") << (database.empty() ? String{} : backQuoteIfNeed(database) + ".")
                  << backQuoteIfNeed(table_name);

    formatOnCluster(settings);
}


void ASTRowPolicyName::replaceEmptyDatabase(const String & current_database)
{
    if (full_name.database.empty())
        full_name.database = current_database;
}


void ASTRowPolicyNames::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (full_names.empty())
        throw Exception("No names of row policies in AST", ErrorCodes::LOGICAL_ERROR);

    bool same_short_name = true;
    if (full_names.size() > 1)
    {
        for (size_t i = 1; i != full_names.size(); ++i)
            if (full_names[i].short_name != full_names[0].short_name)
            {
                same_short_name = false;
                break;
            }
    }

    bool same_db_and_table_name = true;
    if (full_names.size() > 1)
    {
        for (size_t i = 1; i != full_names.size(); ++i)
            if ((full_names[i].database != full_names[0].database) || (full_names[i].table_name != full_names[0].table_name))
            {
                same_db_and_table_name = false;
                break;
            }
    }

    if (same_short_name)
    {
        const String & short_name = full_names[0].short_name;
        settings.ostr << backQuoteIfNeed(short_name) << (settings.hilite ? hilite_keyword : "") << " ON "
                      << (settings.hilite ? hilite_none : "");

        bool need_comma = false;
        for (const auto & full_name : full_names)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            const String & database = full_name.database;
            const String & table_name = full_name.table_name;
            if (!database.empty())
                settings.ostr << backQuoteIfNeed(database) + ".";
            settings.ostr << backQuoteIfNeed(table_name);
        }
    }
    else if (same_db_and_table_name)
    {
        bool need_comma = false;
        for (const auto & full_name : full_names)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            const String & short_name = full_name.short_name;
            settings.ostr << backQuoteIfNeed(short_name);
        }

        const String & database = full_names[0].database;
        const String & table_name = full_names[0].table_name;
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " ON " << (settings.hilite ? hilite_none : "");
        if (!database.empty())
            settings.ostr << backQuoteIfNeed(database) + ".";
        settings.ostr << backQuoteIfNeed(table_name);
    }
    else
    {
        bool need_comma = false;
        for (const auto & full_name : full_names)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            const String & short_name = full_name.short_name;
            const String & database = full_name.database;
            const String & table_name = full_name.table_name;
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
    res.reserve(full_names.size());
    for (const auto & full_name : full_names)
        res.emplace_back(full_name.toString());
    return res;
}


void ASTRowPolicyNames::replaceEmptyDatabase(const String & current_database)
{
    for (auto & full_name : full_names)
        if (full_name.database.empty())
            full_name.database = current_database;
}

}
