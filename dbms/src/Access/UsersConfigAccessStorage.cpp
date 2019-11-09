#include <Access/UsersConfigAccessStorage.h>
#include <Core/Defines.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/MD5Engine.h>
#include <cstring>


namespace DB
{
namespace
{
#if 0
    char getTypeChar(std::type_index type)
    {
        UNUSED(type); /// TODO
        return 0;
    }


    UUID generateID(std::type_index type, const String & name)
    {
        Poco::MD5Engine md5;
        md5.update(name);
        char type_storage_chars[] = " USRSXML";
        type_storage_chars[0] = getTypeChar(type);
        md5.update(type_storage_chars, strlen(type_storage_chars));
        UUID result;
        memcpy(&result, md5.digest().data(), md5.digestLength());
        return result;
    }


    UUID generateID(const IAccessEntity & entity) { return generateID(entity.getType(), entity.getFullName()); }
#endif
}


UsersConfigAccessStorage::UsersConfigAccessStorage() : IAccessStorage("users.xml")
{
}


UsersConfigAccessStorage::~UsersConfigAccessStorage() {}


void UsersConfigAccessStorage::loadFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    std::vector<std::pair<UUID, AccessEntityPtr>> all_entities;
    UNUSED(config); /// TODO
    memory_storage.setAll(all_entities);
}


std::optional<UUID> UsersConfigAccessStorage::findImpl(std::type_index type, const String & name) const
{
    return memory_storage.find(type, name);
}


std::vector<UUID> UsersConfigAccessStorage::findAllImpl(std::type_index type) const
{
    return memory_storage.findAll(type);
}


bool UsersConfigAccessStorage::existsImpl(const UUID & id) const
{
    return memory_storage.exists(id);
}


AccessEntityPtr UsersConfigAccessStorage::readImpl(const UUID & id) const
{
    return memory_storage.read(id);
}


String UsersConfigAccessStorage::readNameImpl(const UUID & id) const
{
    return memory_storage.readName(id);
}


UUID UsersConfigAccessStorage::insertImpl(const AccessEntityPtr & entity, bool)
{
    throwReadonlyCannotInsert(entity->getType(), entity->getFullName());
}


void UsersConfigAccessStorage::removeImpl(const UUID & id)
{
    auto entity = read(id);
    throwReadonlyCannotRemove(entity->getType(), entity->getFullName());
}


void UsersConfigAccessStorage::updateImpl(const UUID & id, const UpdateFunc &)
{
    auto entity = read(id);
    throwReadonlyCannotUpdate(entity->getType(), entity->getFullName());
}


IAccessStorage::SubscriptionPtr UsersConfigAccessStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const
{
    return memory_storage.subscribeForChanges(id, handler);
}


IAccessStorage::SubscriptionPtr UsersConfigAccessStorage::subscribeForChangesImpl(std::type_index type, const OnChangedHandler & handler) const
{
    return memory_storage.subscribeForChanges(type, handler);
}


bool UsersConfigAccessStorage::hasSubscriptionImpl(const UUID & id) const
{
    return memory_storage.hasSubscription(id);
}


bool UsersConfigAccessStorage::hasSubscriptionImpl(std::type_index type) const
{
    return memory_storage.hasSubscription(type);
}
}
