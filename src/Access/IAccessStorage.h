#pragma once

#include <Access/IAccessEntity.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <ext/scope_guard.h>
#include <functional>
#include <optional>
#include <vector>
#include <atomic>


namespace Poco { class Logger; }

namespace DB
{
/// Contains entities, i.e. instances of classes derived from IAccessEntity.
/// The implementations of this class MUST be thread-safe.
class IAccessStorage
{
public:
    IAccessStorage(const String & storage_name_) : storage_name(storage_name_) {}
    virtual ~IAccessStorage() {}

    /// Returns the name of this storage.
    const String & getStorageName() const { return storage_name; }

    /// Returns the identifiers of all the entities of a specified type contained in the storage.
    std::vector<UUID> findAll(std::type_index type) const;

    template <typename EntityType>
    std::vector<UUID> findAll() const { return findAll(typeid(EntityType)); }

    /// Searchs for an entity with specified type and name. Returns std::nullopt if not found.
    std::optional<UUID> find(std::type_index type, const String & name) const;

    template <typename EntityType>
    std::optional<UUID> find(const String & name) const { return find(typeid(EntityType), name); }

    std::vector<UUID> find(std::type_index type, const Strings & names) const;

    template <typename EntityType>
    std::vector<UUID> find(const Strings & names) const { return find(typeid(EntityType), names); }

    /// Searchs for an entity with specified name and type. Throws an exception if not found.
    UUID getID(std::type_index type, const String & name) const;

    template <typename EntityType>
    UUID getID(const String & name) const { return getID(typeid(EntityType), name); }

    std::vector<UUID> getIDs(std::type_index type, const Strings & names) const;

    template <typename EntityType>
    std::vector<UUID> getIDs(const Strings & names) const { return getIDs(typeid(EntityType), names); }

    /// Returns whether there is an entity with such identifier in the storage.
    bool exists(const UUID & id) const;

    /// Reads an entity. Throws an exception if not found.
    template <typename EntityType = IAccessEntity>
    std::shared_ptr<const EntityType> read(const UUID & id) const;

    template <typename EntityType = IAccessEntity>
    std::shared_ptr<const EntityType> read(const String & name) const;

    /// Reads an entity. Returns nullptr if not found.
    template <typename EntityType = IAccessEntity>
    std::shared_ptr<const EntityType> tryRead(const UUID & id) const;

    template <typename EntityType = IAccessEntity>
    std::shared_ptr<const EntityType> tryRead(const String & name) const;

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
    ext::scope_guard subscribeForChanges(std::type_index type, const OnChangedHandler & handler) const;

    template <typename EntityType>
    ext::scope_guard subscribeForChanges(OnChangedHandler handler) const { return subscribeForChanges(typeid(EntityType), handler); }

    /// Subscribes for changes of a specific entry.
    /// Can return nullptr if cannot subscribe (identifier not found) or if it doesn't make sense (the storage is read-only).
    ext::scope_guard subscribeForChanges(const UUID & id, const OnChangedHandler & handler) const;
    ext::scope_guard subscribeForChanges(const std::vector<UUID> & ids, const OnChangedHandler & handler) const;

    bool hasSubscription(std::type_index type) const;
    bool hasSubscription(const UUID & id) const;

protected:
    virtual std::optional<UUID> findImpl(std::type_index type, const String & name) const = 0;
    virtual std::vector<UUID> findAllImpl(std::type_index type) const = 0;
    virtual bool existsImpl(const UUID & id) const = 0;
    virtual AccessEntityPtr readImpl(const UUID & id) const = 0;
    virtual String readNameImpl(const UUID & id) const = 0;
    virtual bool canInsertImpl(const AccessEntityPtr & entity) const = 0;
    virtual UUID insertImpl(const AccessEntityPtr & entity, bool replace_if_exists) = 0;
    virtual void removeImpl(const UUID & id) = 0;
    virtual void updateImpl(const UUID & id, const UpdateFunc & update_func) = 0;
    virtual ext::scope_guard subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const = 0;
    virtual ext::scope_guard subscribeForChangesImpl(std::type_index type, const OnChangedHandler & handler) const = 0;
    virtual bool hasSubscriptionImpl(const UUID & id) const = 0;
    virtual bool hasSubscriptionImpl(std::type_index type) const = 0;

    static UUID generateRandomID();
    Poco::Logger * getLogger() const;
    static String getTypeName(std::type_index type) { return IAccessEntity::getTypeName(type); }
    [[noreturn]] void throwNotFound(const UUID & id) const;
    [[noreturn]] void throwNotFound(std::type_index type, const String & name) const;
    [[noreturn]] static void throwBadCast(const UUID & id, std::type_index type, const String & name, std::type_index required_type);
    [[noreturn]] void throwIDCollisionCannotInsert(
        const UUID & id, std::type_index type, const String & name, std::type_index existing_type, const String & existing_name) const;
    [[noreturn]] void throwNameCollisionCannotInsert(std::type_index type, const String & name) const;
    [[noreturn]] void throwNameCollisionCannotRename(std::type_index type, const String & old_name, const String & new_name) const;
    [[noreturn]] void throwReadonlyCannotInsert(std::type_index type, const String & name) const;
    [[noreturn]] void throwReadonlyCannotUpdate(std::type_index type, const String & name) const;
    [[noreturn]] void throwReadonlyCannotRemove(std::type_index type, const String & name) const;

    using Notification = std::tuple<OnChangedHandler, UUID, AccessEntityPtr>;
    using Notifications = std::vector<Notification>;
    static void notify(const Notifications & notifications);

private:
    AccessEntityPtr tryReadBase(const UUID & id) const;

    const String storage_name;
    mutable std::atomic<Poco::Logger *> log = nullptr;
};


template <typename EntityType>
std::shared_ptr<const EntityType> IAccessStorage::read(const UUID & id) const
{
    auto entity = readImpl(id);
    auto ptr = typeid_cast<std::shared_ptr<const EntityType>>(entity);
    if (ptr)
        return ptr;
    throwBadCast(id, entity->getType(), entity->getName(), typeid(EntityType));
}


template <typename EntityType>
std::shared_ptr<const EntityType> IAccessStorage::read(const String & name) const
{
    return read<EntityType>(getID<EntityType>(name));
}


template <typename EntityType>
std::shared_ptr<const EntityType> IAccessStorage::tryRead(const UUID & id) const
{
    auto entity = tryReadBase(id);
    if (!entity)
        return nullptr;
    return typeid_cast<std::shared_ptr<const EntityType>>(entity);
}


template <typename EntityType>
std::shared_ptr<const EntityType> IAccessStorage::tryRead(const String & name) const
{
    auto id = find<EntityType>(name);
    return id ? tryRead<EntityType>(*id) : nullptr;
}
}
