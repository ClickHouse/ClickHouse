#include <ACL/AccessControlManager.h>
#include <ACL/MultipleAttributesStorage.h>
#include <ACL/MemoryAttributesStorage.h>
#include <ACL/User2.h>
#include <ACL/Role.h>


namespace DB
{
namespace AccessControlNames
{
    extern const size_t ROLE_NAMESPACE_IDX = 0;
}


AccessControlManager::AccessControlManager()
{
    storage = std::make_unique<MemoryAttributesStorage>();
}


AccessControlManager::~AccessControlManager()
{
}


template <typename ACLType>
ACLType AccessControlManager::create(const typename ACLType::Attributes & attrs, bool if_not_exists)
{
    if (if_not_exists)
        return {storage->tryInsert(attrs).first, *storage};
    return {storage->insert(attrs), *storage};
}


template <typename ACLType>
void AccessControlManager::update(const String & name, const UpdateFunction<ACLType> & update_function)
{
    storage->update(storage->getID(name, ACLType::TYPE), update_function);
}


template <typename ACLType>
void AccessControlManager::update(const UpdateFunctions<ACLType> & update_functions)
{
    for (const auto & [name, update_function] : update_functions)
        update<ACLType>(name, update_function);
}


template <typename ACLType>
bool AccessControlManager::drop(const String & name, bool if_not_exists)
{
    if (if_not_exists)
    {
        auto id = storage->find(name, ACLType::TYPE);
        if (id)
            return storage->tryRemove(*id);
        return false;
    }
    storage->remove(storage->getID(name, ACLType::TYPE), ACLType::TYPE);
    return true;
}


template <typename ACLType>
void AccessControlManager::drop(const Strings & names, bool if_not_exists)
{
    for (const String & name : names)
        drop<ACLType>(name, if_not_exists);
}


template <typename ACLType>
ACLType AccessControlManager::get(const String & name)
{
    return ACLType(storage->getID(name, ACLType::TYPE), *storage);
}


template <typename ACLType>
ACLType AccessControlManager::get(const String & name) const
{
    return ACLType(storage->getID(name, ACLType::TYPE), *storage);
}


template <typename ACLType>
std::optional<ACLType> AccessControlManager::find(const String & name)
{
    auto id = storage->find(name, ACLType::TYPE);
    if (id)
        return ACLType(*id, *storage);
    return {};
}


template <typename ACLType>
std::optional<ACLType> AccessControlManager::find(const String & name) const
{
    auto id = storage->find(name, ACLType::TYPE);
    if (id)
        return ACLType(*id, *storage);
    return {};
}


template Role AccessControlManager::create<Role>(const Role::Attributes &, bool);
template User2 AccessControlManager::create<User2>(const User2::Attributes &, bool);

template void AccessControlManager::update<Role>(const String &, const UpdateFunction<Role> &);
template void AccessControlManager::update<User2>(const String &, const UpdateFunction<User2> &);

template void AccessControlManager::update<Role>(const UpdateFunctions<Role> &);
template void AccessControlManager::update<User2>(const UpdateFunctions<User2> &);

template bool AccessControlManager::drop<Role>(const String &, bool);
template bool AccessControlManager::drop<User2>(const String &, bool);

template void AccessControlManager::drop<Role>(const Strings &, bool);
template void AccessControlManager::drop<User2>(const Strings &, bool);

template Role AccessControlManager::get<Role>(const String &);
template User2 AccessControlManager::get<User2>(const String &);

template ConstRole AccessControlManager::get<ConstRole>(const String &) const;
template ConstUser AccessControlManager::get<ConstUser>(const String &) const;

template std::optional<Role> AccessControlManager::find<Role>(const String &);
template std::optional<User2> AccessControlManager::find<User2>(const String &);

template std::optional<ConstRole> AccessControlManager::find<ConstRole>(const String &) const;
template std::optional<ConstUser> AccessControlManager::find<ConstUser>(const String &) const;
}
