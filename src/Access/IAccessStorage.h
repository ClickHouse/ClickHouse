#pragma once

#include <Access/IAccessEntity.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <common/scope_guard.h>
#include <functional>
#include <optional>
#include <vector>
#include <atomic>


namespace Poco { class Logger; }
namespace Poco::Net { class IPAddress; }

namespace DB
{
struct User;
class Credentials;
class ExternalAuthenticators;

/// Contains entities, i.e. instances of classes derived from IAccessEntity.
/// The implementations of this class MUST be thread-safe.
class IAccessStorage
{
public:
    IAccessStorage(const String & storage_name_) : storage_name(storage_name_) {}
    virtual ~IAccessStorage() {}

    /// Returns the name of this storage.
    const String & getStorageName() const { return storage_name; }
    virtual const char * getStorageType() const = 0;

    /// Returns a JSON with the parameters of the storage. It's up to the storage type to fill the JSON.
    virtual String getStorageParamsJSON() const { return "{}"; }

    using EntityType = IAccessEntity::Type;
    using EntityTypeInfo = IAccessEntity::TypeInfo;

    /// Returns the identifiers of all the entities of a specified type contained in the storage.
    std::vector<UUID> findAll(EntityType type) const;

    template <typename EntityClassT>
    std::vector<UUID> findAll() const { return findAll(EntityClassT::TYPE); }

    /// Searches for an entity with specified type and name. Returns std::nullopt if not found.
    std::optional<UUID> find(EntityType type, const String & name) const;

    template <typename EntityClassT>
    std::optional<UUID> find(const String & name) const { return find(EntityClassT::TYPE, name); }

    std::vector<UUID> find(EntityType type, const Strings & names) const;

    template <typename EntityClassT>
    std::vector<UUID> find(const Strings & names) const { return find(EntityClassT::TYPE, names); }

    /// Searches for an entity with specified name and type. Throws an exception if not found.
    UUID getID(EntityType type, const String & name) const;

    template <typename EntityClassT>
    UUID getID(const String & name) const { return getID(EntityClassT::TYPE, name); }

    std::vector<UUID> getIDs(EntityType type, const Strings & names) const;

    template <typename EntityClassT>
    std::vector<UUID> getIDs(const Strings & names) const { return getIDs(EntityClassT::TYPE, names); }

    /// Returns whether there is an entity with such identifier in the storage.
    bool exists(const UUID & id) const;

    /// Reads an entity. Throws an exception if not found.
    template <typename EntityClassT = IAccessEntity>
    std::shared_ptr<const EntityClassT> read(const UUID & id) const;

    template <typename EntityClassT = IAccessEntity>
    std::shared_ptr<const EntityClassT> read(const String & name) const;

    /// Reads an entity. Returns nullptr if not found.
    template <typename EntityClassT = IAccessEntity>
    std::shared_ptr<const EntityClassT> tryRead(const UUID & id) const;

    template <typename EntityClassT = IAccessEntity>
    std::shared_ptr<const EntityClassT> tryRead(const String & name) const;

    /// Reads only name of an entity.
    String readName(const UUID & id) const;
    std::optional<String> tryReadName(const UUID & id) const;

    /// Returns true if a specified entity can be inserted into this storage.
    /// This function doesn't check whether there are no entities with such name in the storage.
    bool canInsert(const AccessEntityPtr & entity) const { return canInsertImpl(entity); }

    /// Inserts an entity to the storage. Returns ID of a new entry in the storage.
    /// Throws an exception if the specified name already exists.
    UUID insert(const AccessEntityPtr & entity);
    std::vector<UUID> insert(const std::vector<AccessEntityPtr> & multiple_entities);

    /// Inserts an entity to the storage. Returns ID of a new entry in the storage.
    std::optional<UUID> tryInsert(const AccessEntityPtr & entity);
    std::vector<UUID> tryInsert(const std::vector<AccessEntityPtr> & multiple_entities);

    /// Inserts an entity to the storage. Return ID of a new entry in the storage.
    /// Replaces an existing entry in the storage if the specified name already exists.
    UUID insertOrReplace(const AccessEntityPtr & entity);
    std::vector<UUID> insertOrReplace(const std::vector<AccessEntityPtr> & multiple_entities);

    /// Removes an entity from the storage. Throws an exception if couldn't remove.
    void remove(const UUID & id);
    void remove(const std::vector<UUID> & ids);

    /// Removes an entity from the storage. Returns false if couldn't remove.
    bool tryRemove(const UUID & id);

    /// Removes multiple entities from the storage. Returns the list of successfully dropped.
    std::vector<UUID> tryRemove(const std::vector<UUID> & ids);

    using UpdateFunc = std::function<AccessEntityPtr(const AccessEntityPtr &)>;

    /// Updates an entity stored in the storage. Throws an exception if couldn't update.
    void update(const UUID & id, const UpdateFunc & update_func);
    void update(const std::vector<UUID> & ids, const UpdateFunc & update_func);

    /// Updates an entity stored in the storage. Returns false if couldn't update.
    bool tryUpdate(const UUID & id, const UpdateFunc & update_func);

    /// Updates multiple entities in the storage. Returns the list of successfully updated.
    std::vector<UUID> tryUpdate(const std::vector<UUID> & ids, const UpdateFunc & update_func);

    using OnChangedHandler = std::function<void(const UUID & /* id */, const AccessEntityPtr & /* new or changed entity, null if removed */)>;

    /// Subscribes for all changes.
    /// Can return nullptr if cannot subscribe (identifier not found) or if it doesn't make sense (the storage is read-only).
    scope_guard subscribeForChanges(EntityType type, const OnChangedHandler & handler) const;

    template <typename EntityClassT>
    scope_guard subscribeForChanges(OnChangedHandler handler) const { return subscribeForChanges(EntityClassT::TYPE, handler); }

    /// Subscribes for changes of a specific entry.
    /// Can return nullptr if cannot subscribe (identifier not found) or if it doesn't make sense (the storage is read-only).
    scope_guard subscribeForChanges(const UUID & id, const OnChangedHandler & handler) const;
    scope_guard subscribeForChanges(const std::vector<UUID> & ids, const OnChangedHandler & handler) const;

    bool hasSubscription(EntityType type) const;
    bool hasSubscription(const UUID & id) const;

    /// Finds a user, check the provided credentials and returns the ID of the user if they are valid.
    /// Throws an exception if no such user or credentials are invalid.
    UUID login(const Credentials & credentials, const Poco::Net::IPAddress & address, const ExternalAuthenticators & external_authenticators, bool replace_exception_with_cannot_authenticate = true) const;

    /// Returns the ID of a user who has logged in (maybe on another node).
    /// The function assumes that the password has been already checked somehow, so we can skip checking it now.
    UUID getIDOfLoggedUser(const String & user_name) const;

protected:
    virtual std::optional<UUID> findImpl(EntityType type, const String & name) const = 0;
    virtual std::vector<UUID> findAllImpl(EntityType type) const = 0;
    virtual bool existsImpl(const UUID & id) const = 0;
    virtual AccessEntityPtr readImpl(const UUID & id) const = 0;
    virtual String readNameImpl(const UUID & id) const = 0;
    virtual bool canInsertImpl(const AccessEntityPtr & entity) const = 0;
    virtual UUID insertImpl(const AccessEntityPtr & entity, bool replace_if_exists) = 0;
    virtual void removeImpl(const UUID & id) = 0;
    virtual void updateImpl(const UUID & id, const UpdateFunc & update_func) = 0;
    virtual scope_guard subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const = 0;
    virtual scope_guard subscribeForChangesImpl(EntityType type, const OnChangedHandler & handler) const = 0;
    virtual bool hasSubscriptionImpl(const UUID & id) const = 0;
    virtual bool hasSubscriptionImpl(EntityType type) const = 0;
    virtual UUID loginImpl(const Credentials & credentials, const Poco::Net::IPAddress & address, const ExternalAuthenticators & external_authenticators) const;
    virtual bool areCredentialsValidImpl(const User & user, const Credentials & credentials, const ExternalAuthenticators & external_authenticators) const;
    virtual bool isAddressAllowedImpl(const User & user, const Poco::Net::IPAddress & address) const;
    virtual UUID getIDOfLoggedUserImpl(const String & user_name) const;

    static UUID generateRandomID();
    Poco::Logger * getLogger() const;
    static String outputEntityTypeAndName(EntityType type, const String & name) { return EntityTypeInfo::get(type).outputWithEntityName(name); }
    [[noreturn]] void throwNotFound(const UUID & id) const;
    [[noreturn]] void throwNotFound(EntityType type, const String & name) const;
    [[noreturn]] static void throwBadCast(const UUID & id, EntityType type, const String & name, EntityType required_type);
    [[noreturn]] void throwIDCollisionCannotInsert(
        const UUID & id, EntityType type, const String & name, EntityType existing_type, const String & existing_name) const;
    [[noreturn]] void throwNameCollisionCannotInsert(EntityType type, const String & name) const;
    [[noreturn]] void throwNameCollisionCannotRename(EntityType type, const String & old_name, const String & new_name) const;
    [[noreturn]] void throwReadonlyCannotInsert(EntityType type, const String & name) const;
    [[noreturn]] void throwReadonlyCannotUpdate(EntityType type, const String & name) const;
    [[noreturn]] void throwReadonlyCannotRemove(EntityType type, const String & name) const;
    [[noreturn]] static void throwAddressNotAllowed(const Poco::Net::IPAddress & address);
    [[noreturn]] static void throwInvalidCredentials();
    [[noreturn]] static void throwCannotAuthenticate(const String & user_name);

    using Notification = std::tuple<OnChangedHandler, UUID, AccessEntityPtr>;
    using Notifications = std::vector<Notification>;
    static void notify(const Notifications & notifications);

private:
    AccessEntityPtr tryReadBase(const UUID & id) const;

    const String storage_name;
    mutable std::atomic<Poco::Logger *> log = nullptr;
};


template <typename EntityClassT>
std::shared_ptr<const EntityClassT> IAccessStorage::read(const UUID & id) const
{
    auto entity = readImpl(id);
    if constexpr (std::is_same_v<EntityClassT, IAccessEntity>)
        return entity;
    else
    {
        auto ptr = typeid_cast<std::shared_ptr<const EntityClassT>>(entity);
        if (ptr)
            return ptr;
        throwBadCast(id, entity->getType(), entity->getName(), EntityClassT::TYPE);
    }
}


template <typename EntityClassT>
std::shared_ptr<const EntityClassT> IAccessStorage::read(const String & name) const
{
    return read<EntityClassT>(getID<EntityClassT>(name));
}


template <typename EntityClassT>
std::shared_ptr<const EntityClassT> IAccessStorage::tryRead(const UUID & id) const
{
    auto entity = tryReadBase(id);
    if (!entity)
        return nullptr;
    return typeid_cast<std::shared_ptr<const EntityClassT>>(entity);
}


template <typename EntityClassT>
std::shared_ptr<const EntityClassT> IAccessStorage::tryRead(const String & name) const
{
    auto id = find<EntityClassT>(name);
    return id ? tryRead<EntityClassT>(*id) : nullptr;
}
}
