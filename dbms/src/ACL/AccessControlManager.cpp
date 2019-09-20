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
}
