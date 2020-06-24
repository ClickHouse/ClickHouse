#include <Access/AccessRightsElement.h>
#include <Dictionaries/IDictionary.h>
#include <Common/quoteString.h>


namespace DB
{
void AccessRightsElement::setDatabase(const String & new_database)
{
    database = new_database;
    any_database = false;
}


void AccessRightsElement::replaceEmptyDatabase(const String & new_database)
{
    if (isEmptyDatabase())
        setDatabase(new_database);
}


bool AccessRightsElement::isEmptyDatabase() const
{
    return !any_database && database.empty();
}


String AccessRightsElement::toString() const
{
    String columns_in_parentheses;
    if (!any_column)
    {
        for (const auto & column : columns)
        {
            columns_in_parentheses += columns_in_parentheses.empty() ? "(" : ", ";
            columns_in_parentheses += backQuoteIfNeed(column);
        }
        columns_in_parentheses += ")";
    }

    String msg;
    for (const std::string_view & keyword : access_flags.toKeywords())
    {
        if (!msg.empty())
            msg += ", ";
        msg += String{keyword} + columns_in_parentheses;
    }

    msg += " ON ";

    if (any_database)
        msg += "*.";
    else if (!database.empty() && (database != IDictionary::NO_DATABASE_TAG))
        msg += backQuoteIfNeed(database) + ".";

    if (any_table)
        msg += "*";
    else
        msg += backQuoteIfNeed(table);
    return msg;
}


void AccessRightsElements::replaceEmptyDatabase(const String & new_database)
{
    for (auto & element : *this)
        element.replaceEmptyDatabase(new_database);
}


String AccessRightsElements::toString() const
{
    String res;
    bool need_comma = false;
    for (const auto & element : *this)
    {
        if (std::exchange(need_comma, true))
            res += ", ";
        res += element.toString();
    }

    if (res.empty())
        res = "USAGE ON *.*";
    return res;
}
}
