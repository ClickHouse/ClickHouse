#include <ACL/AccessControlManager.h>
#include <ACL/MultipleControlAttributesStorage.h>
#include <ACL/MemoryControlAttributesStorage.h>


namespace DB
{
namespace AccessControlNames
{
    extern const size_t ROLE_NAMESPACE_IDX = 0;
}


AccessControlManager::AccessControlManager()
{
    storage = std::make_unique<MemoryControlAttributesStorage>();
}


AccessControlManager::~AccessControlManager()
{
}


Role AccessControlManager::createRole(const Role::Attributes & attrs, bool if_not_exists)
{
    UUID id = if_not_exists ? storage->tryInsert(attrs).first : storage->insert(attrs);
    return Role(id, *storage);
}

Role AccessControlManager::getRole(const String & name)
{
    return Role(storage->getID(name, Role::TYPE), *storage);
}


void AccessControlManager::dropRole(const String & name, bool if_not_exists)
{
    if (if_not_exists)
    {
        auto role = findRole(name);
        if (role)
            role->drop(true);
    }
    else
        getRole(name).drop(false);
}


void AccessControlManager::dropRoles(const Strings & names, bool if_not_exists)
{
    for (const String & name : names)
        dropRole(name, if_not_exists);
}


std::optional<Role> AccessControlManager::findRole(const String & name)
{
    auto id = storage->find(name, Role::TYPE);
    if (id)
        return Role(*id, *storage);
    return {};
}


ConstRole AccessControlManager::getRole(const String & name) const
{
    return ConstRole(storage->getID(name, Role::TYPE), *storage);
}


std::optional<ConstRole> AccessControlManager::findRole(const String & name) const
{
    auto id = storage->find(name, Role::TYPE);
    if (id)
        return ConstRole(*id, *storage);
    return {};
}


User2 AccessControlManager::createUser(const User2::Attributes & attrs, bool if_not_exists)
{
    UUID id = if_not_exists ? storage->tryInsert(attrs).first : storage->insert(attrs);
    return User2(id, *storage);
}

User2 AccessControlManager::getUser(const String & name)
{
    return User2(storage->getID(name, Role::TYPE), *storage);
}


void AccessControlManager::dropUser(const String & name, bool if_not_exists)
{
    if (if_not_exists)
    {
        auto role = findUser(name);
        if (role)
            role->drop(true);
    }
    else
        getUser(name).drop(false);
}


void AccessControlManager::dropUsers(const Strings & names, bool if_not_exists)
{
    for (const String & name : names)
        dropUser(name, if_not_exists);
}


std::optional<User2> AccessControlManager::findUser(const String & name)
{
    auto id = storage->find(name, Role::TYPE);
    if (id)
        return User2(*id, *storage);
    return {};
}


ConstUser AccessControlManager::getUser(const String & name) const
{
    return ConstUser(storage->getID(name, Role::TYPE), *storage);
}


std::optional<ConstUser> AccessControlManager::findUser(const String & name) const
{
    auto id = storage->find(name, Role::TYPE);
    if (id)
        return ConstUser(*id, *storage);
    return {};
}
}
