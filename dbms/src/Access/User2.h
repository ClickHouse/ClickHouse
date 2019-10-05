#pragma once

#include <Access/Role.h>
#include <Access/Authentication.h>
#include <Access/AllowedHosts.h>
#include <Access/SettingsConstraints.h>
#include <Common/SettingsChanges.h>


namespace DB
{
/// Represents an user in Role-based Access Control.
/// Syntax:
/// CREATE USER [IF NOT EXISTS] name
///     [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH}] BY password/hash]
///     [ALLOWED HOST {NAME 'hostname' [,...] | REGEXP 'hostname' [,...]} | IP 'address/subnet' [,...] | ANY | FROM_USERNAME}]
///     [DEFAULT ROLE {role[,...] | NONE}]
///     [SET varname [= value] [MIN min] [MAX max] [READONLY] [,...]]
///     [ACCOUNT {LOCK | UNLOCK}]
///
/// ALTER USER [IF EXISTS] name
///     [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH}] BY password/hash]
///     [[ADD|REMOVE] ALLOWED HOST {NAME 'hostname' [,...] | REGEXP 'hostname' [,...]} | IP 'address/subnet' [,...] | ANY | FROM_USERNAME}]
///     [[ADD|REMOVE] DEFAULT ROLE {role[,...] | NONE | ALL}]
///     [SET varname [= value] [MIN min] [MAX max] [READONLY] [,...]]
///     [UNSET varname | ALL]
///     [ACCOUNT {LOCK | UNLOCK}]
///
/// SHOW CREATE USER name
///
/// DROP USER [IF EXISTS] name
struct User2 : public Role
{
    Authentication authentication;
    AllowedHosts allowed_hosts;

    std::unordered_set<UUID> default_roles;

    SettingsChanges settings;
    SettingsConstraints settings_constraints;

    bool account_locked = false;

    static const Type TYPE;
    const Type & getType() const override { return TYPE; }
    std::shared_ptr<IAttributes> clone() const override { return cloneImpl<User2>(); }
    bool equal(const IAttributes & other) const override;
    bool hasReferences(const UUID & id) const override;
    void removeReferences(const UUID & id) override;
};

using User2Ptr = std::shared_ptr<const User2>;
}
