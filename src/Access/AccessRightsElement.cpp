#include <Access/AccessRightsElement.h>
#include <Common/quoteString.h>
#include <boost/range/algorithm_ext/erase.hpp>


namespace DB
{
namespace
{
    void formatColumnNames(const Strings & columns, String & result)
    {
        result += "(";
        bool need_comma = false;
        for (const auto & column : columns)
        {
            if (need_comma)
                result += ", ";
            need_comma = true;
            result += backQuoteIfNeed(column);
        }
        result += ")";
    }

    void formatONClause(const String & database, bool any_database, const String & table, bool any_table, String & result)
    {
        result += "ON ";
        if (any_database)
        {
            result += "*.*";
        }
        else
        {
            if (!database.empty())
            {
                result += backQuoteIfNeed(database);
                result += ".";
            }
            if (any_table)
                result += "*";
            else
                result += backQuoteIfNeed(table);
        }
    }

    void formatOptions(bool grant_option, bool is_partial_revoke, String & result)
    {
        if (is_partial_revoke)
        {
            if (grant_option)
                result.insert(0, "REVOKE GRANT OPTION ");
            else
                result.insert(0, "REVOKE ");
        }
        else
        {
            if (grant_option)
                result.insert(0, "GRANT ").append(" WITH GRANT OPTION");
            else
                result.insert(0, "GRANT ");
        }
    }

    void formatAccessFlagsWithColumns(const AccessFlags & access_flags, const Strings & columns, bool any_column, String & result)
    {
        String columns_as_str;
        if (!any_column)
        {
            if (columns.empty())
            {
                result += "USAGE";
                return;
            }
            formatColumnNames(columns, columns_as_str);
        }

        auto keywords = access_flags.toKeywords();
        if (keywords.empty())
        {
            result += "USAGE";
            return;
        }

        bool need_comma = false;
        for (const std::string_view & keyword : keywords)
        {
            if (need_comma)
                result.append(", ");
            need_comma = true;
            result += keyword;
            result += columns_as_str;
        }
    }

    String toStringImpl(const AccessRightsElement & element, bool with_options)
    {
        String result;
        formatAccessFlagsWithColumns(element.access_flags, element.columns, element.any_column, result);
        result += " ";
        formatONClause(element.database, element.any_database, element.table, element.any_table, result);
        if (with_options)
            formatOptions(element.grant_option, element.is_partial_revoke, result);
        return result;
    }

    String toStringImpl(const AccessRightsElements & elements, bool with_options)
    {
        if (elements.empty())
            return with_options ? "GRANT USAGE ON *.*" : "USAGE ON *.*";

        String result;
        String part;

        for (size_t i = 0; i != elements.size(); ++i)
        {
            const auto & element = elements[i];

            if (!part.empty())
                part += ", ";
            formatAccessFlagsWithColumns(element.access_flags, element.columns, element.any_column, part);

            bool next_element_uses_same_table_and_options = false;
            if (i != elements.size() - 1)
            {
                const auto & next_element = elements[i + 1];
                if (element.sameDatabaseAndTable(next_element) && element.sameOptions(next_element))
                    next_element_uses_same_table_and_options = true;
            }

            if (!next_element_uses_same_table_and_options)
            {
                part += " ";
                formatONClause(element.database, element.any_database, element.table, element.any_table, part);
                if (with_options)
                    formatOptions(element.grant_option, element.is_partial_revoke, part);
                if (result.empty())
                    result = std::move(part);
                else
                    result.append(", ").append(part);
                part.clear();
            }
        }

        return result;
    }
}


String AccessRightsElement::toString() const { return toStringImpl(*this, true); }
String AccessRightsElement::toStringWithoutOptions() const { return toStringImpl(*this, false); }
String AccessRightsElements::toString() const { return toStringImpl(*this, true); }
String AccessRightsElements::toStringWithoutOptions() const { return toStringImpl(*this, false); }

void AccessRightsElements::eraseNonGrantable()
{
    boost::range::remove_erase_if(*this, [](AccessRightsElement & element)
    {
        element.eraseNonGrantable();
        return element.empty();
    });
}

}
