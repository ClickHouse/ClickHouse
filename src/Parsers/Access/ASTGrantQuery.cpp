#include <Parsers/Access/ASTGrantQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
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


    void formatONClause(const AccessRightsElement & element, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "ON " << (settings.hilite ? IAST::hilite_none : "");
        if (element.isGlobalWithParameter())
        {
            if (element.any_parameter)
                settings.ostr << "*";
            else
                settings.ostr << backQuoteIfNeed(element.parameter);
        }
        else if (element.any_database)
        {
            settings.ostr << "*.*";
        }
        else
        {
            if (!element.database.empty())
                settings.ostr << backQuoteIfNeed(element.database) << ".";
            if (element.any_table)
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


    void formatCurrentGrantsElements(const AccessRightsElements & elements, const IAST::FormatSettings & settings)
    {
        for (size_t i = 0; i != elements.size(); ++i)
        {
            const auto & element = elements[i];

            bool next_element_on_same_db_and_table = false;
            if (i != elements.size() - 1)
            {
                const auto & next_element = elements[i + 1];
                if (element.sameDatabaseAndTableAndParameter(next_element))
                    next_element_on_same_db_and_table = true;
            }

            if (!next_element_on_same_db_and_table)
            {
                settings.ostr << " ";
                formatONClause(element, settings);
            }
        }
    }
}


String ASTGrantQuery::getID(char) const
{
    return "GrantQuery";
}


ASTPtr ASTGrantQuery::clone() const
{
    auto res = std::make_shared<ASTGrantQuery>(*this);

    if (roles)
        res->roles = std::static_pointer_cast<ASTRolesOrUsersSet>(roles->clone());

    if (grantees)
        res->grantees = std::static_pointer_cast<ASTRolesOrUsersSet>(grantees->clone());

    return res;
}


void ASTGrantQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << (attach_mode ? "ATTACH " : "")
                  << (settings.hilite ? hilite_keyword : "") << (is_revoke ? "REVOKE" : "GRANT")
                  << (settings.hilite ? IAST::hilite_none : "");

    if (!access_rights_elements.sameOptions())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Elements of an ASTGrantQuery are expected to have the same options");
    if (!access_rights_elements.empty() &&  access_rights_elements[0].is_partial_revoke && !is_revoke)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A partial revoke should be revoked, not granted");
    bool grant_option = !access_rights_elements.empty() && access_rights_elements[0].grant_option;

    formatOnCluster(settings);

    if (is_revoke)
    {
        if (grant_option)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " GRANT OPTION FOR" << (settings.hilite ? hilite_none : "");
        else if (admin_option)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " ADMIN OPTION FOR" << (settings.hilite ? hilite_none : "");
    }

    settings.ostr << " ";
    if (roles)
    {
        roles->format(settings);
        if (!access_rights_elements.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "ASTGrantQuery can contain either roles or access rights elements "
                            "to grant or revoke, not both of them");
    }
    else if (current_grants)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "CURRENT GRANTS" << (settings.hilite ? hilite_none : "");
        formatCurrentGrantsElements(access_rights_elements, settings);
    }
    else
    {
        formatElementsWithoutOptions(access_rights_elements, settings);
    }

    settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << (is_revoke ? " FROM " : " TO ")
                  << (settings.hilite ? IAST::hilite_none : "");
    grantees->format(settings);

    if (!is_revoke)
    {
        if (grant_option)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH GRANT OPTION" << (settings.hilite ? hilite_none : "");
        else if (admin_option)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH ADMIN OPTION" << (settings.hilite ? hilite_none : "");

        if (replace_access || replace_granted_roles)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH REPLACE OPTION" << (settings.hilite ? hilite_none : "");
    }
}


void ASTGrantQuery::replaceEmptyDatabase(const String & current_database)
{
    access_rights_elements.replaceEmptyDatabase(current_database);
}


void ASTGrantQuery::replaceCurrentUserTag(const String & current_user_name) const
{
    if (grantees)
        grantees->replaceCurrentUserTag(current_user_name);
}

}
