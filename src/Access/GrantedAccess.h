#pragma once

#include <Access/AccessRights.h>


namespace DB
{
/// Access rights as they are granted to a role or user.
/// Stores both the access rights themselves and the access rights with grant option.
struct GrantedAccess
{
    AccessRights access;
    AccessRights access_with_grant_option;

    template <typename... Args>
    void grant(const Args &... args)
    {
        access.grant(args...);
    }

    template <typename... Args>
    void grantWithGrantOption(const Args &... args)
    {
        access.grant(args...);
        access_with_grant_option.grant(args...);
    }

    template <typename... Args>
    void revoke(const Args &... args)
    {
        access.revoke(args...);
        access_with_grant_option.revoke(args...);
    }

    template <typename... Args>
    void revokeGrantOption(const Args &... args)
    {
        access_with_grant_option.revoke(args...);
    }

    struct GrantsAndPartialRevokes
    {
        AccessRightsElements grants;
        AccessRightsElements revokes;
        AccessRightsElements grants_with_grant_option;
        AccessRightsElements revokes_grant_option;
    };

    /// Retrieves the information about grants and partial revokes.
    GrantsAndPartialRevokes getGrantsAndPartialRevokes() const;

    friend bool operator ==(const GrantedAccess & left, const GrantedAccess & right) { return (left.access == right.access) && (left.access_with_grant_option == right.access_with_grant_option); }
    friend bool operator !=(const GrantedAccess & left, const GrantedAccess & right) { return !(left == right); }
};
}
