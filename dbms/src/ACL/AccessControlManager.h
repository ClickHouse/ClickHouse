#pragma once

#include <ACL/Role.h>


namespace DB
{
class IControlAttributesStorage;


/// Manages access control entities.
class AccessControlManager
{
public:
    AccessControlManager();
    ~AccessControlManager();

    Role createRole(const Role::Attributes & attrs, bool if_not_exists = false);
    void dropRole(const String & name, bool if_not_exists = false);
    Role getRole(const String & name);
    std::optional<Role> findRole(const String & name);
    ConstRole getRole(const String & name) const;
    std::optional<ConstRole> findRole(const String & name) const;

private:
    std::unique_ptr<IControlAttributesStorage> storage;
};

}
