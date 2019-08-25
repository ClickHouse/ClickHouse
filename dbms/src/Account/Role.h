#pragma once

#include <Account/RoleAttributes.h>


namespace DB
{
class IAccessStorage;
using AccessStoragePtr = std::shared_ptr<IAccessStorage>;


class Role
{
public:
    Role(const UUID & id_, const AccessStoragePtr & storage_) : id(id_), storage(storage_) {}
    ~Role() {}

    const UUID & getID() const { return id; }
    AccessStoragePtr getStorage() const { return storage; }
    RoleAttributesPtr getAttributes() const;
    String getName() const { return getAttributes()->name; }

    void grant(Privileges::Types access, bool with_grant_option = false);
    void grant(Privileges::Types access, const String & database, bool with_grant_option = false);
    void grant(Privileges::Types access, const String & database, const String & table, bool with_grant_option = false);
    void grant(Privileges::Types access, const String & database, const String & table, const String & column, bool with_grant_option = false);
    void grant(Privileges::Types access, const String & database, const String & table, const Strings & columns, bool with_grant_option = false);

    void revoke(Privileges::Types access, bool only_grant_option = false);
    void revoke(Privileges::Types access, const String & database, bool only_grant_option = false);
    void revoke(Privileges::Types access, const String & database, const String & table, bool only_grant_option = false);
    void revoke(Privileges::Types access, const String & database, const String & table, const String & column, bool only_grant_option = false);
    void revoke(Privileges::Types access, const String & database, const String & table, const Strings & columns, bool only_grant_option = false);

    void grantRole(const Role & role, bool with_admin_option = false);
    void revokeRole(const Role & role, bool only_admin_option = false);
    void revokeRoles(const std::vector<Role> & roles, bool only_admin_option = false);
    void setDefaultRoles(const std::vector<Role> & roles);

    //void applyRLSPolicy(const RLSPolicy & rls_policy);
    //void unapplyRLSPolicy(const RLSPolicy & rls_policy);

    void drop();

private:
    const UUID id;
    AccessStoragePtr storage;
};

}
