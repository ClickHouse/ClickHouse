#include <Parsers/Access/ASTCheckGrantQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

namespace
{
    void formatColumnNames(const Strings & columns, const IAST::FormatSettings & settings)
    {
        settings.ostr << "(";
        bool need_comma = false;
        for (const auto & column : columns)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            settings.ostr << backQuoteIfNeed(column);
        }
        settings.ostr << ")";
    }


    void formatONClause(const AccessRightsElement & element, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "ON " << (settings.hilite ? IAST::hilite_none : "");
        if (element.isGlobalWithParameter())
        {
            if (element.anyParameter())
                settings.ostr << "*";
            else
                settings.ostr << backQuoteIfNeed(element.parameter);
        }
        else if (element.anyDatabase())
        {
            settings.ostr << "*.*";
        }
        else
        {
            if (!element.database.empty())
                settings.ostr << backQuoteIfNeed(element.database) << ".";
            if (element.anyDatabase())
                settings.ostr << "*";
            else
                settings.ostr << backQuoteIfNeed(element.table);
        }
    }


    void formatElementsWithoutOptions(const AccessRightsElements & elements, const IAST::FormatSettings & settings)
    {
        bool no_output = true;
        for (size_t i = 0; i != elements.size(); ++i)
        {
            const auto & element = elements[i];
            auto keywords = element.access_flags.toKeywords();
            if (keywords.empty() || (!element.anyColumn() && element.columns.empty()))
                continue;

            for (const auto & keyword : keywords)
            {
                if (!std::exchange(no_output, false))
                    settings.ostr << ", ";

                settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << keyword << (settings.hilite ? IAST::hilite_none : "");
                if (!element.anyColumn())
                    formatColumnNames(element.columns, settings);
            }

            bool next_element_on_same_db_and_table = false;
            if (i != elements.size() - 1)
            {
                const auto & next_element = elements[i + 1];
                if (element.sameDatabaseAndTableAndParameter(next_element))
                {
                    next_element_on_same_db_and_table = true;
                }
            }

            if (!next_element_on_same_db_and_table)
            {
                settings.ostr << " ";
                formatONClause(element, settings);
            }
        }

        if (no_output)
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "USAGE ON " << (settings.hilite ? IAST::hilite_none : "") << "*.*";
    }

}


String ASTCheckGrantQuery::getID(char) const
{
    return "CheckGrantQuery";
}


ASTPtr ASTCheckGrantQuery::clone() const
{
    auto res = std::make_shared<ASTCheckGrantQuery>(*this);

    return res;
}


void ASTCheckGrantQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "CHECK GRANT"
                  << (settings.hilite ? IAST::hilite_none : "");

    settings.ostr << " ";

    formatElementsWithoutOptions(access_rights_elements, settings);
}


void ASTCheckGrantQuery::replaceEmptyDatabase(const String & current_database)
{
    access_rights_elements.replaceEmptyDatabase(current_database);
}

}
