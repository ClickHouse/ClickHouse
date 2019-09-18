#pragma once

#include <ACL/IControlAttributesDriven.h>
//#include <ACL/Privileges.h>


namespace DB
{
/// Represents a role in Role-based Access Control.
/// Syntax:
/// CREATE ROLE [IF NOT EXISTS] name [, name ...]
///
/// DROP ROLE name [, name ...]
class Role : public IControlAttributesDriven
{
public:
    struct Attributes : public IControlAttributes
    {
#if 0
        /// Granted privileges. This doesn't include the privileges from the granted roles.
        Privileges privileges;
        Privileges grant_options;

        struct GrantedRoleSettings
        {
            bool admin_option = false;
            bool enabled_by_default = false;

            friend bool operator==(const GrantedRoleSettings & lhs, const GrantedRoleSettings & rhs) { return (lhs.admin_option == rhs.admin_option) && (lhs.enabled_by_default == rhs.enabled_by_default); }
            friend bool operator!=(const GrantedRoleSettings & lhs, const GrantedRoleSettings & rhs) { return !(lhs == rhs); }
        };

        /// Granted roles.
        std::unordered_map<UUID, GrantedRoleSettings> granted_roles;

        /// Applied row-level security policies.
        //std::unordered_set<UUID> applied_row_level_security_plocies;

        /// Assigned quotas.
        std::vector<UUID> assigned_quotas;

        /// Applied setting profiles.
        //std::unordered_set<UUID> applied_settings_profiles;
        /// Settings settings;
        /// SettingsConstraints settings_constraints;
#endif

        static const Type TYPE;
        const Type & getType() const override { return TYPE; }
        std::shared_ptr<IControlAttributes> clone() const override { return cloneImpl<Attributes>(); }
#if 0
        bool hasReferences(UUID ref_id) const override;
        void removeReferences(UUID ref_id) override;
#endif
    protected:
        bool equal(const IControlAttributes & other) const override;
    };

    using AttributesPtr = std::shared_ptr<const Attributes>;

    using IControlAttributesDriven::IControlAttributesDriven;

    static const Type & TYPE;
    const Type & getType() const override { return Attributes::TYPE; }
    AttributesPtr getAttributes() const { return getAttributesImpl<Attributes>(); }
    AttributesPtr tryGetAttributes() const { return tryGetAttributesImpl<Attributes>(); }

#if 0
    struct GrantParams
    {
        bool with_grant_option = false;
        GrantParams() {}
    };

    /// Grants privileges.
    void grant(Privileges::Types access, const GrantParams & params = {});
    void grant(Privileges::Types access, const String & database, const GrantParams & params = {});
    void grant(Privileges::Types access, const String & database, const String & table, const GrantParams & params = {});
    void grant(Privileges::Types access, const String & database, const String & table, const String & column, const GrantParams & params = {});
    void grant(Privileges::Types access, const String & database, const String & table, const Strings & columns, const GrantParams & params = {});
    Changes grantChanges(Privileges::Types access, const GrantParams & params = {});
    Changes grantChanges(Privileges::Types access, const String & database, const GrantParams & params = {});
    Changes grantChanges(Privileges::Types access, const String & database, const String & table, const GrantParams & params = {});
    Changes grantChanges(Privileges::Types access, const String & database, const String & table, const String & column, const GrantParams & params = {});
    Changes grantChanges(Privileges::Types access, const String & database, const String & table, const Strings & columns, const GrantParams & params = {});

    struct RevokeParams
    {
        bool only_grant_option = false;
        bool partial_revokes = false;
        RevokeParams() {}
    };

    /// Revokes privileges. Returns false if this role didn't use to have the specified privileges.
    bool revoke(Privileges::Types access, const RevokeParams & params = {});
    bool revoke(Privileges::Types access, const String & database, const RevokeParams & params = {});
    bool revoke(Privileges::Types access, const String & database, const String & table, const RevokeParams & params = {});
    bool revoke(Privileges::Types access, const String & database, const String & table, const String & column, const RevokeParams & params = {});
    bool revoke(Privileges::Types access, const String & database, const String & table, const Strings & columns, const RevokeParams & params = {});
    Changes revokeChanges(Privileges::Types access, const RevokeParams & params = {}, bool * revoked = nullptr);
    Changes revokeChanges(Privileges::Types access, const String & database, const RevokeParams & params = {}, bool * revoked = nullptr);
    Changes revokeChanges(Privileges::Types access, const String & database, const String & table, const RevokeParams & params = {}, bool * revoked = nullptr);
    Changes revokeChanges(Privileges::Types access, const String & database, const String & table, const String & column, const RevokeParams & params = {}, bool * revoked = nullptr);
    Changes revokeChanges(Privileges::Types access, const String & database, const String & table, const Strings & columns, const RevokeParams & params = {}, bool * revoked = nullptr);

    Privileges getPrivileges() const;
    Privileges getGrantOptions() const;

    struct GrantRoleParams
    {
        bool with_admin_option = false;
        GrantRoleParams() {}
    };

    /// Grants another role to this role.
    void grantRole(const Role & role, const GrantRoleParams & params = {});
    Changes grantRoleChanges(const Role & role, const GrantRoleParams & params = {});

    struct RevokeRoleParams
    {
        bool only_admin_option = false;
        RevokeRoleParams() {}
    };

    /// Revokes granted role from this role.
    /// Returns false if this role didn't use to have the specified roles granted.
    bool revokeRole(const Role & role, const RevokeRoleParams & params = {});
    Changes revokeRoleChanges(const Role & role, const RevokeRoleParams & params = {}, bool * revoked = nullptr);

    std::vector<Role> getGrantedRoles() const;
    std::vector<Role> getGrantedRolesWithAdminOption() const;

protected:
    ACLAttributesType getType() const override;
    std::pair<AttributesPtr, IControlAttributesDrivenManager *> getAttributesWithManagerStrict() const;
#endif
};

void f()
{
    Role role1;
    Role role2 = role1;
    IControlAttributesStorageManager * mgr = nullptr;
    Role role3("sd", *mgr);
}

}
