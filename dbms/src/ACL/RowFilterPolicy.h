#pragma once

#include <ACL/IAccessControlElement.h>
#include <ACL/Privileges.h>


namespace DB
{
/// Represents a role in Role-based Access Control.
class RowPolicy : public IControlAttributesDriven
{
public:
    static const Type TYPE;

    struct Attributes : public IAttributes
    {
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

        /// Applied quotas.
        //std::unordered_set<UUID> applied_quotas;

        /// Applied setting profiles.
        //std::unordered_set<UUID> applied_settings_profiles;

        Type getType() const override { return Type::ROLE; }
        std::shared_ptr<IAccessControlElement::Attributes> clone() const override;
        bool hasReferences(UUID ref_id) const override;
        void removeReferences(UUID ref_id) override;

    protected:
        bool equal(const IAccessControlElement::Attributes & other) const override;
    };

    using AttributesPtr = std::shared_ptr<const Attributes>;
    using IAccessControlElement::IAccessControlElement;

    AttributesPtr getAttributes() const { return getAttributesImpl<Attributes>(); }
    AttributesPtr tryGetAttributes() const { return tryGetAttributesImpl<Attributes>(); }

    struct GrantParams
    {
        bool with_grant_option = false;
        GrantParams() {}
    };

    /// Grants privileges.
    void grant(Privileges::Types access, const GrantParams & params = {}) { perform(grantOp(access, params)); }
    void grant(Privileges::Types access, const String & database, const GrantParams & params = {}) { perform(grantOp(access, database, params)); }
    void grant(Privileges::Types access, const String & database, const String & table, const GrantParams & params = {}) { perform(grantOp(access, database, table, params)); }
    void grant(Privileges::Types access, const String & database, const String & table, const String & column, const GrantParams & params = {}) { perform(grantOp(access, database, table, column, params)); }
    void grant(Privileges::Types access, const String & database, const String & table, const Strings & columns, const GrantParams & params = {}) { perform(grantOp(access, database, table, columns, params)); }
    Operation grantOp(Privileges::Types access, const GrantParams & params = {}) const;
    Operation grantOp(Privileges::Types access, const String & database, const GrantParams & params = {}) const;
    Operation grantOp(Privileges::Types access, const String & database, const String & table, const GrantParams & params = {}) const;
    Operation grantOp(Privileges::Types access, const String & database, const String & table, const String & column, const GrantParams & params = {}) const;
    Operation grantOp(Privileges::Types access, const String & database, const String & table, const Strings & columns, const GrantParams & params = {}) const;

    struct RevokeParams
    {
        bool only_grant_option = false;
        bool partial_revokes = false;
        RevokeParams() {}
    };

    /// Revokes privileges. Returns false if this role didn't use to have the specified privileges.
    bool revoke(Privileges::Types access, const RevokeParams & params = {}) { bool revoked; perform(revokeOp(access, params, &revoked)); return revoked; }
    bool revoke(Privileges::Types access, const String & database, const RevokeParams & params = {}) { bool revoked; perform(revokeOp(access, database, params, &revoked)); return revoked; }
    bool revoke(Privileges::Types access, const String & database, const String & table, const RevokeParams & params = {}) { bool revoked; perform(revokeOp(access, database, table, params, &revoked)); return revoked; }
    bool revoke(Privileges::Types access, const String & database, const String & table, const String & column, const RevokeParams & params = {}) { bool revoked; perform(revokeOp(access, database, table, column, params, &revoked)); return revoked; }
    bool revoke(Privileges::Types access, const String & database, const String & table, const Strings & columns, const RevokeParams & params = {}) { bool revoked; perform(revokeOp(access, database, table, columns, params, &revoked)); return revoked; }
    Operation revokeOp(Privileges::Types access, const RevokeParams & params = {}, bool * revoked = nullptr) const;
    Operation revokeOp(Privileges::Types access, const String & database, const RevokeParams & params = {}, bool * revoked = nullptr) const;
    Operation revokeOp(Privileges::Types access, const String & database, const String & table, const RevokeParams & params = {}, bool * revoked = nullptr) const;
    Operation revokeOp(Privileges::Types access, const String & database, const String & table, const String & column, const RevokeParams & params = {}, bool * revoked = nullptr) const;
    Operation revokeOp(Privileges::Types access, const String & database, const String & table, const Strings & columns, const RevokeParams & params = {}, bool * revoked = nullptr) const;

    Privileges getPrivileges() const;
    Privileges getGrantOptions() const;

    struct GrantRoleParams
    {
        bool with_admin_option = false;
        GrantRoleParams() {}
    };

    /// Grants another role to this role.
    void grantRole(const Role & role, const GrantRoleParams & params = {}) { perform(grantRoleOp(role, params)); }
    Operation grantRoleOp(const Role & role, const GrantRoleParams & params = {}) const;

    struct RevokeRoleParams
    {
        bool only_admin_option = false;
        RevokeRoleParams() {}
    };

    /// Revokes granted role from this role.
    /// Returns false if this role didn't use to have the specified roles granted.
    bool revokeRole(const Role & role, const RevokeRoleParams & params = {}) { bool revoked; perform(revokeRoleOp(role, params, &revoked)); return revoked; }
    Operation revokeRoleOp(const Role & role, const RevokeRoleParams & params = {}, bool * revoked = nullptr) const;

    std::vector<Role> getGrantedRoles() const;
    std::vector<Role> getGrantedRolesWithAdminOption() const;

private:
    Operation prepareOperation(const std::function<void(Attributes &)> & fn) const;
    const String & getTypeName() const override;
    int getNotFoundErrorCode() const override;
    int getAlreadyExistsErrorCode() const override;
};

}
