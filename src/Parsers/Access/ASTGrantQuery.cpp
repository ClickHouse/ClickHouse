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


    void formatONClause(const String & database, bool any_database, const String & table, bool any_table, const IAST::FormatSettings & settings)
    {
        settings.writeKeyword("ON ");
        if (any_database)
        {
            settings.ostr << "*.*";
        }
        else
        {
            if (!database.empty())
                settings.ostr << backQuoteIfNeed(database) << ".";
            if (any_table)
                settings.ostr << "*";
            else
                settings.ostr << backQuoteIfNeed(table);
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

                settings.writeKeyword(keyword);
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
            {
                settings.ostr << " ";
                formatONClause(element.database, element.any_database, element.table, element.any_table, settings);
            }
        }

        if (no_output)
        {
            settings.writeKeyword("USAGE ON ");
            settings.ostr << "*.*";
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
    settings.writeKeyword(attach_mode ? "ATTACH " : "");
    settings.writeKeyword((!is_revoke && (replace_access || replace_granted_roles)) ? "REPLACE " : "");
    settings.writeKeyword(is_revoke ? "REVOKE" : "GRANT");

    if (!access_rights_elements.sameOptions())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Elements of an ASTGrantQuery are expected to have the same options");
    if (!access_rights_elements.empty() &&  access_rights_elements[0].is_partial_revoke && !is_revoke)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A partial revoke should be revoked, not granted");
    bool grant_option = !access_rights_elements.empty() && access_rights_elements[0].grant_option;

    formatOnCluster(settings);

    if (is_revoke)
    {
        if (grant_option)
            settings.writeKeyword(" GRANT OPTION FOR");
        else if (admin_option)
            settings.writeKeyword(" ADMIN OPTION FOR");
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
    else
        formatElementsWithoutOptions(access_rights_elements, settings);

    settings.writeKeyword(is_revoke ? " FROM " : " TO ");
    grantees->format(settings);

    if (!is_revoke)
    {
        if (grant_option)
            settings.writeKeyword(" WITH GRANT OPTION");
        else if (admin_option)
            settings.writeKeyword(" WITH ADMIN OPTION");
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
