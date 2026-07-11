#include <Parsers/Access/ASTGrantQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Common/SipHash.h>


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
    auto res = make_intrusive<ASTGrantQuery>(*this);

    if (roles)
        res->roles = boost::static_pointer_cast<ASTRolesOrUsersSet>(roles->clone());

    if (grantees)
        res->grantees = boost::static_pointer_cast<ASTRolesOrUsersSet>(grantees->clone());

    return res;
}


void ASTGrantQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// The base hash folds only `getID` (a constant here) and `children` (empty for this
    /// query), so without this override every `GRANT` / `REVOKE` collides in the tree hash.
    /// The rewrite-rule matcher relies on the tree hash for semantic equality, so fold every
    /// semantic field the formatter emits. Only formatter-emitted state is folded, so the hash
    /// stays stable across the debug-build format -> parse -> format consistency check.
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);

    hash_state.update(attach_mode);
    hash_state.update(is_revoke);
    hash_state.update(admin_option);
    hash_state.update(current_grants);
    hash_state.update(cluster);
    /// The formatter emits the two `replace_*` flags only through their combined
    /// `WITH REPLACE OPTION`, so fold the same combined bit.
    hash_state.update(replace_access || replace_granted_roles);

    /// `access_rights_elements` is an `AccessRightsElements` (not an AST), so the base tree hash
    /// cannot see it: `GRANT SELECT ON a TO u` and `GRANT SELECT ON b TO u` would otherwise
    /// collide. Fold exactly the text the formatter emits for it, plus the query-level
    /// grant-option bit it derives from `access_rights_elements[0]`.
    const bool grant_option = !access_rights_elements.empty() && access_rights_elements[0].grant_option;
    hash_state.update(grant_option);
    {
        WriteBufferFromOwnString buf;
        access_rights_elements.formatElementsWithoutOptions(buf);
        hash_state.update(buf.str());
    }

    /// `roles` / `grantees` are `ASTRolesOrUsersSet` members kept outside `children`; fold a
    /// presence flag before delegating so a role grant and an access-rights grant cannot collide.
    hash_state.update(static_cast<bool>(roles));
    if (roles)
        roles->updateTreeHash(hash_state, ignore_aliases);

    hash_state.update(static_cast<bool>(grantees));
    if (grantees)
        grantees->updateTreeHash(hash_state, ignore_aliases);
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
