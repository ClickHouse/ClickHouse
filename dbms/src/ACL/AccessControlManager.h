#pragma once

#include <ACL/Role.h>
#include <ACL/User2.h>


namespace DB
{
class IAttributesStorage;


/// Manages access control entities.
class AccessControlManager
{
public:
    AccessControlManager();
    ~AccessControlManager();

    Role createRole(const Role::Attributes & attrs, bool if_not_exists = false);
    void dropRole(const String & name, bool if_not_exists = false);
    void dropRoles(const Strings & name, bool if_not_exists = false);
    Role getRole(const String & name);
    std::optional<Role> findRole(const String & name);
    ConstRole getRole(const String & name) const;
    std::optional<ConstRole> findRole(const String & name) const;

    User2 createUser(const User2::Attributes & attrs, bool if_not_exists = false);
    void dropUser(const String & names, bool if_not_exists = false);
    void dropUsers(const Strings & names, bool if_not_exists = false);
    User2 getUser(const String & name);
    std::optional<User2> findUser(const String & name);
    ConstUser getUser(const String & name) const;
    std::optional<ConstUser> findUser(const String & name) const;

private:
    std::unique_ptr<IAttributesStorage> storage;
};

}
