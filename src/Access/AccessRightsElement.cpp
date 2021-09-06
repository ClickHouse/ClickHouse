#include <Access/AccessRightsElement.h>
#include <Dictionaries/IDictionary.h>
#include <Common/quoteString.h>
#include <boost/range/algorithm/sort.hpp>
#include <boost/range/algorithm/unique.hpp>
#include <boost/range/algorithm_ext/is_sorted.hpp>
#include <boost/range/algorithm_ext/erase.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>


namespace DB
{
namespace
{
    using Kind = AccessRightsElementWithOptions::Kind;

    String formatOptions(bool grant_option, Kind kind, const String & inner_part)
    {
        if (kind == Kind::REVOKE)
        {
            if (grant_option)
                return "REVOKE GRANT OPTION " + inner_part;
            else
                return "REVOKE " + inner_part;
        }
        else
        {
            if (grant_option)
                return "GRANT " + inner_part + " WITH GRANT OPTION";
            else
                return "GRANT " + inner_part;
        }
    }


    String formatONClause(const String & database, bool any_database, const String & table, bool any_table)
    {
        String msg = "ON ";

        if (any_database)
            msg += "*.";
        else if (!database.empty())
            msg += backQuoteIfNeed(database) + ".";

        if (any_table)
            msg += "*";
        else
            msg += backQuoteIfNeed(table);
        return msg;
    }


    String formatAccessFlagsWithColumns(const AccessFlags & access_flags, const Strings & columns, bool any_column)
    {
        String columns_in_parentheses;
        if (!any_column)
        {
            if (columns.empty())
                return "USAGE";
            for (const auto & column : columns)
            {
                columns_in_parentheses += columns_in_parentheses.empty() ? "(" : ", ";
                columns_in_parentheses += backQuoteIfNeed(column);
            }
            columns_in_parentheses += ")";
        }

        auto keywords = access_flags.toKeywords();
        if (keywords.empty())
            return "USAGE";

        String msg;
        for (const std::string_view & keyword : keywords)
        {
            if (!msg.empty())
                msg += ", ";
            msg += String{keyword} + columns_in_parentheses;
        }
        return msg;
    }
}


String AccessRightsElement::toString() const
{
    return formatAccessFlagsWithColumns(access_flags, columns, any_column) + " " + formatONClause(database, any_database, table, any_table);
}

String AccessRightsElementWithOptions::toString() const
{
    return formatOptions(grant_option, kind, AccessRightsElement::toString());
}

String AccessRightsElements::toString() const
{
    if (empty())
        return "USAGE ON *.*";

    String res;
    String inner_part;

    for (size_t i = 0; i != size(); ++i)
    {
        const auto & element = (*this)[i];

        if (!inner_part.empty())
            inner_part += ", ";
        inner_part += formatAccessFlagsWithColumns(element.access_flags, element.columns, element.any_column);

        bool next_element_uses_same_table = false;
        if (i != size() - 1)
        {
            const auto & next_element = (*this)[i + 1];
            if (element.sameDatabaseAndTable(next_element))
                next_element_uses_same_table = true;
        }

        if (!next_element_uses_same_table)
        {
            if (!res.empty())
                res += ", ";
            res += inner_part + " " + formatONClause(element.database, element.any_database, element.table, element.any_table);
            inner_part.clear();
        }
    }

    return res;
}

String AccessRightsElementsWithOptions::toString() const
{
    if (empty())
        return "GRANT USAGE ON *.*";

    String res;
    String inner_part;

    for (size_t i = 0; i != size(); ++i)
    {
        const auto & element = (*this)[i];

        if (!inner_part.empty())
            inner_part += ", ";
        inner_part += formatAccessFlagsWithColumns(element.access_flags, element.columns, element.any_column);

        bool next_element_uses_same_mode_and_table = false;
        if (i != size() - 1)
        {
            const auto & next_element = (*this)[i + 1];
            if (element.sameDatabaseAndTable(next_element) && element.sameOptions(next_element))
                next_element_uses_same_mode_and_table = true;
        }

        if (!next_element_uses_same_mode_and_table)
        {
            if (!res.empty())
                res += ", ";
            res += formatOptions(
                element.grant_option,
                element.kind,
                inner_part + " " + formatONClause(element.database, element.any_database, element.table, element.any_table));
            inner_part.clear();
        }
    }

    return res;
}

}
