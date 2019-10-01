#pragma once

#include <ACL/Role.h>
#include <ACL/EncodedPassword.h>
#include <ACL/AllowedHosts.h>
#include <ACL/SettingsConstraints.h>
#include <Common/SettingsChanges.h>


namespace DB
{
/// Represents an user in Role-based Access Control.
/// Syntax:
/// CREATE USER [IF NOT EXISTS] name
///     [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH}] BY password/hash
///     [HOST 'hostname' [,...] | HOST REGEXP 'hostname' [,...] | HOST IP 'address/subnet' [,...] | HOST ANY]
///     [DEFAULT ROLE NONE | ALL | role[,...]]
///     [SET varname [= value] [MIN min] [MAX max] [READONLY] [,...]]
///     [ACCOUNT {LOCK | UNLOCK}]
///
/// ALTER USER [IF EXISTS] name
///     [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH}] BY password/hash
///     [HOST NAME 'hostname' [,...] | HOST REGEXP 'hostname' [,...] | HOST IP 'address/subnet' [,...] | HOST ANY]
///     [DEFAULT ROLE NONE | ALL | role[,...]]
///     [SET varname [= value] [MIN min] [MAX max] [READONLY] [,...]]
///     [ACCOUNT {LOCK | UNLOCK}]
///
/// SHOW CREATE USER name
///
/// DROP USER [IF EXISTS] name
struct User2 : public Role
{
    EncodedPassword password;
    AllowedHosts allowed_hosts;

    std::unordered_set<UUID> default_roles;
    bool default_all_roles = false;

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
