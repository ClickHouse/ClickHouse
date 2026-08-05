#include <Parsers/Access/ASTGrantQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <IO/Operators.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    void formatCurrentGrantsElements(const AccessRightsElements & elements, WriteBuffer & ostr, const IAST::FormatSettings &)
    {
        ostr << "(";
        elements.formatElementsWithoutOptions(ostr);
        ostr << ")";
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


void ASTGrantQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    ostr << (attach_mode ? "ATTACH " : "")
                  << (is_revoke ? "REVOKE" : "GRANT")
                 ;

    if (!access_rights_elements.sameOptions())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Elements of an ASTGrantQuery are expected to have the same options");
    if (!access_rights_elements.empty() &&  access_rights_elements[0].is_partial_revoke && !is_revoke)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A partial revoke should be revoked, not granted");
    bool grant_option = !access_rights_elements.empty() && access_rights_elements[0].grant_option;

    formatOnCluster(ostr, settings);

    if (is_revoke)
    {
        if (grant_option)
            ostr << " GRANT OPTION FOR";
        else if (admin_option)
            ostr << " ADMIN OPTION FOR";
    }

    ostr << " ";
    if (roles)
    {
        roles->format(ostr, settings);
        if (!access_rights_elements.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "ASTGrantQuery can contain either roles or access rights elements "
                            "to grant or revoke, not both of them");
    }
    else if (current_grants)
    {
        ostr << "CURRENT GRANTS";
        formatCurrentGrantsElements(access_rights_elements, ostr, settings);
    }
    else
    {
        access_rights_elements.formatElementsWithoutOptions(ostr);
    }

    ostr << (is_revoke ? " FROM " : " TO ")
                 ;
    grantees->format(ostr, settings);

    if (!is_revoke)
    {
        if (grant_option)
            ostr << " WITH GRANT OPTION";
        else if (admin_option)
            ostr << " WITH ADMIN OPTION";

        if (replace_access || replace_granted_roles)
            ostr << " WITH REPLACE OPTION";
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
