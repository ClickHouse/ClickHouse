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
    size_t groupElements(AccessRightsElements & elements, size_t start)
    {
        auto & start_element = elements[start];
        auto it = std::find_if(elements.begin() + start + 1, elements.end(),
                               [&](const AccessRightsElement & element)
        {
            return (element.database != start_element.database) ||
                   (element.any_database != start_element.any_database) ||
                   (element.table != start_element.table) ||
                   (element.any_table != start_element.any_table) ||
                   (element.any_column != start_element.any_column);
        });
        size_t end = it - elements.begin();

        /// All the elements at indices from start to end here specify
        /// the same database and table.

        if (start_element.any_column)
        {
            /// Easy case: the elements don't specify columns.
            /// All we need is to combine the access flags.
            for (size_t i = start + 1; i != end; ++i)
            {
                start_element.access_flags |= elements[i].access_flags;
                elements[i].access_flags = {};
            }
            return end;
        }

        /// Difficult case: the elements specify columns.
        /// We have to find groups of columns with common access flags.
        for (size_t i = start; i != end; ++i)
        {
            if (!elements[i].access_flags)
                continue;

            AccessFlags common_flags = elements[i].access_flags;
            size_t num_elements_with_common_flags = 1;
            for (size_t j = i + 1; j != end; ++j)
            {
                auto new_common_flags = common_flags & elements[j].access_flags;
                if (new_common_flags)
                {
                    common_flags = new_common_flags;
                    ++num_elements_with_common_flags;
                }
            }

            if (num_elements_with_common_flags == 1)
                continue;

            if (elements[i].access_flags != common_flags)
            {
                elements.insert(elements.begin() + i + 1, elements[i]);
                elements[i].access_flags = common_flags;
                elements[i].columns.clear();
                ++end;
            }

            for (size_t j = i + 1; j != end; ++j)
            {
                if ((elements[j].access_flags & common_flags) == common_flags)
                {
                    boost::range::push_back(elements[i].columns, elements[j].columns);
                    elements[j].access_flags -= common_flags;
                }
            }
        }

        return end;
    }

    /// Tries to combine elements to decrease their number.
    void groupElements(AccessRightsElements & elements)
    {
        if (!boost::range::is_sorted(elements))
            boost::range::sort(elements); /// Algorithm in groupElement() requires elements to be sorted.
        for (size_t start = 0; start != elements.size();)
            start = groupElements(elements, start);
    }

    /// Removes unnecessary elements, sorts elements and makes them unique.
    void sortElementsAndMakeUnique(AccessRightsElements & elements)
    {
        /// Remove empty elements.
        boost::range::remove_erase_if(elements, [](const AccessRightsElement & element)
        {
            return !element.access_flags || (!element.any_column && element.columns.empty());
        });

        /// Sort columns and make them unique.
        for (auto & element : elements)
        {
            if (element.any_column)
                continue;

            if (!boost::range::is_sorted(element.columns))
                boost::range::sort(element.columns);
            element.columns.erase(std::unique(element.columns.begin(), element.columns.end()), element.columns.end());
        }

        /// Sort elements themselves.
        boost::range::sort(elements);
        elements.erase(std::unique(elements.begin(), elements.end()), elements.end());
    }
}

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
    String msg = toStringWithoutON();
    msg += " ON ";

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

String AccessRightsElement::toStringWithoutON() const
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


void AccessRightsElements::replaceEmptyDatabase(const String & new_database)
{
    for (auto & element : *this)
        element.replaceEmptyDatabase(new_database);
}


String AccessRightsElements::toString()
{
    normalize();

    if (empty())
        return "USAGE ON *.*";

    String msg;
    bool need_comma = false;
    for (size_t i = 0; i != size(); ++i)
    {
        const auto & element = (*this)[i];
        if (std::exchange(need_comma, true))
            msg += ", ";
        bool next_element_on_same_db_and_table = false;
        if (i != size() - 1)
        {
            const auto & next_element = (*this)[i + 1];
            if ((element.database == next_element.database) && (element.any_database == next_element.any_database)
                && (element.table == next_element.table) && (element.any_table == next_element.any_table))
                next_element_on_same_db_and_table = true;
        }
        if (next_element_on_same_db_and_table)
            msg += element.toStringWithoutON();
        else
            msg += element.toString();
    }
    return msg;
}


void AccessRightsElements::normalize()
{
    groupElements(*this);
    sortElementsAndMakeUnique(*this);
}

}
