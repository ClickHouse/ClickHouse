#include <Parsers/ASTGrantQuery.h>
#include <Parsers/ASTRolesOrUsersSet.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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


    void formatONClause(const String & database, bool any_database, const String & table, bool any_table, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << " ON " << (settings.hilite ? IAST::hilite_none : "");
        if (any_database)
            settings.ostr << "*";
        else if (!database.empty())
            settings.ostr << backQuoteIfNeed(database);

        settings.ostr << ".";
        if (any_table)
            settings.ostr << "*";
        else
            settings.ostr << backQuoteIfNeed(table);
    }


    void formatElementsWithoutOptions(const AccessRightsElements & elements, const IAST::FormatSettings & settings)
    {
        bool no_output = true;
        for (size_t i = 0; i != elements.size(); ++i)
        {
            const auto & element = elements[i];
            auto keywords = element.access_flags.toKeywords();
            if (keywords.empty() || (!element.any_column && element.columns.empty()))
                continue;

            for (const auto & keyword : keywords)
            {
                if (!std::exchange(no_output, false))
                    settings.ostr << ", ";

                settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << keyword << (settings.hilite ? IAST::hilite_none : "");
                if (!element.any_column)
                    formatColumnNames(element.columns, settings);
            }

            bool next_element_on_same_db_and_table = false;
            if (i != elements.size() - 1)
            {
                const auto & next_element = elements[i + 1];
                if ((element.database == next_element.database) && (element.any_database == next_element.any_database)
                    && (element.table == next_element.table) && (element.any_table == next_element.any_table))
                    next_element_on_same_db_and_table = true;
            }

            if (!next_element_on_same_db_and_table)
                formatONClause(element.database, element.any_database, element.table, element.any_table, settings);
        }

        if (no_output)
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "USAGE ON " << (settings.hilite ? IAST::hilite_none : "") << "*.*";
    }
}


String ASTGrantQuery::getID(char) const
{
    return "GrantQuery";
}


ASTPtr ASTGrantQuery::clone() const
{
    return std::make_shared<ASTGrantQuery>(*this);
}


void ASTGrantQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (static_cast<int>(access_rights_elements.empty()) + static_cast<int>(!roles) != 1)
        throw Exception("Either roles or access rights elements should be set", ErrorCodes::LOGICAL_ERROR);

    bool grant_option = false;
    bool is_revoke_in_use = is_revoke;
    if (!access_rights_elements.empty())
    {
        if (!access_rights_elements.sameOptions())
            throw Exception("Elements in the same ASTGrantQuery must use same options", ErrorCodes::LOGICAL_ERROR);
        grant_option = access_rights_elements[0].grant_option;
        is_revoke_in_use |= access_rights_elements[0].is_revoke;
    }

    settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << (attach_mode ? "ATTACH " : "") << (is_revoke ? "REVOKE" : "GRANT")
                  << (settings.hilite ? IAST::hilite_none : "");

    formatOnCluster(settings);

    if (is_revoke_in_use)
    {
        if (grant_option)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " GRANT OPTION FOR" << (settings.hilite ? hilite_none : "");
        else if (admin_option)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " ADMIN OPTION FOR" << (settings.hilite ? hilite_none : "");
    }

    settings.ostr << " ";
    if (roles)
        roles->format(settings);
    else
        formatElementsWithoutOptions(access_rights_elements, settings);

    settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << (is_revoke_in_use ? " FROM " : " TO ")
                  << (settings.hilite ? IAST::hilite_none : "");
    grantees->format(settings);

    if (!is_revoke_in_use)
    {
        if (grant_option)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH GRANT OPTION" << (settings.hilite ? hilite_none : "");
        else if (admin_option)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH ADMIN OPTION" << (settings.hilite ? hilite_none : "");
    }
}


void ASTGrantQuery::replaceEmptyDatabaseWithCurrent(const String & current_database)
{
    access_rights_elements.replaceEmptyDatabase(current_database);
}


void ASTGrantQuery::replaceCurrentUserTagWithName(const String & current_user_name) const
{
    if (grantees)
        grantees->replaceCurrentUserTagWithName(current_user_name);
}

}
