#pragma once

#include <Access/IAccessEntity.h>
#include <Core/Types.h>
#include <Core/UUID.h>
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
enum class AuthenticationType;

/// Contains entities, i.e. instances of classes derived from IAccessEntity.
/// The implementations of this class MUST be thread-safe.
class IAccessStorage : public boost::noncopyable
{
public:
    explicit IAccessStorage(const String & storage_name_) : storage_name(storage_name_) {}
    virtual ~IAccessStorage() = default;

    /// Returns the name of this storage.
    const String & getStorageName() const { return storage_name; }
    virtual const char * getStorageType() const = 0;

    /// Returns a JSON with the parameters of the storage. It's up to the storage type to fill the JSON.
    virtual String getStorageParamsJSON() const { return "{}"; }

    /// Returns true if this storage is readonly.
    virtual bool isReadOnly() const { return false; }

    /// Returns true if this entity is readonly.
    virtual bool isReadOnly(const UUID &) const { return isReadOnly(); }

    /// Reloads and updates entities in this storage. This function is used to implement SYSTEM RELOAD CONFIG.
    virtual void reload() {}

    /// Starts periodic reloading and update of entities in this storage.
    virtual void startPeriodicReloading() {}

    /// Stops periodic reloading and update of entities in this storage.
    virtual void stopPeriodicReloading() {}

    /// Returns the identifiers of all the entities of a specified type contained in the storage.
    std::vector<UUID> findAll(AccessEntityType type) const;

    template <typename EntityClassT>
    std::vector<UUID> findAll() const { return findAll(EntityClassT::TYPE); }

    /// Searches for an entity with specified type and name. Returns std::nullopt if not found.
    std::optional<UUID> find(AccessEntityType type, const String & name) const;

    template <typename EntityClassT>
    std::optional<UUID> find(const String & name) const { return find(EntityClassT::TYPE, name); }

    std::vector<UUID> find(AccessEntityType type, const Strings & names) const;

    template <typename EntityClassT>
    std::vector<UUID> find(const Strings & names) const { return find(EntityClassT::TYPE, names); }

    /// Searches for an entity with specified name and type. Throws an exception if not found.
    UUID getID(AccessEntityType type, const String & name) const;

    template <typename EntityClassT>
    UUID getID(const String & name) const { return getID(EntityClassT::TYPE, name); }

    std::vector<UUID> getIDs(AccessEntityType type, const Strings & names) const;

    template <typename EntityClassT>
    std::vector<UUID> getIDs(const Strings & names) const { return getIDs(EntityClassT::TYPE, names); }

    /// Returns whether there is an entity with such identifier in the storage.
    virtual bool exists(const UUID & id) const = 0;

    /// Reads an entity. Throws an exception if not found.
    template <typename EntityClassT = IAccessEntity>
    std::shared_ptr<const EntityClassT> read(const UUID & id, bool throw_if_not_exists = true) const;

    template <typename EntityClassT = IAccessEntity>
    std::shared_ptr<const EntityClassT> read(const String & name, bool throw_if_not_exists = true) const;

    /// Reads an entity. Returns nullptr if not found.
    template <typename EntityClassT = IAccessEntity>
    std::shared_ptr<const EntityClassT> tryRead(const UUID & id) const;

    template <typename EntityClassT = IAccessEntity>
    std::shared_ptr<const EntityClassT> tryRead(const String & name) const;

    /// Reads only name of an entity.
    String readName(const UUID & id) const;
    std::optional<String> readName(const UUID & id, bool throw_if_not_exists) const;
    Strings readNames(const std::vector<UUID> & ids, bool throw_if_not_exists = true) const;
    std::optional<String> tryReadName(const UUID & id) const;
    Strings tryReadNames(const std::vector<UUID> & ids) const;

    /// Inserts an entity to the storage. Returns ID of a new entry in the storage.
    /// Throws an exception if the specified name already exists.
    UUID insert(const AccessEntityPtr & entity);
    std::optional<UUID> insert(const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists);
    std::vector<UUID> insert(const std::vector<AccessEntityPtr> & multiple_entities, bool replace_if_exists = false, bool throw_if_exists = true);

    /// Inserts an entity to the storage. Returns ID of a new entry in the storage.
    std::optional<UUID> tryInsert(const AccessEntityPtr & entity);
    std::vector<UUID> tryInsert(const std::vector<AccessEntityPtr> & multiple_entities);

    /// Inserts an entity to the storage. Return ID of a new entry in the storage.
    /// Replaces an existing entry in the storage if the specified name already exists.
    UUID insertOrReplace(const AccessEntityPtr & entity);
    std::vector<UUID> insertOrReplace(const std::vector<AccessEntityPtr> & multiple_entities);

    /// Removes an entity from the storage. Throws an exception if couldn't remove.
    bool remove(const UUID & id, bool throw_if_not_exists = true);
    std::vector<UUID> remove(const std::vector<UUID> & ids, bool throw_if_not_exists = true);

    /// Removes an entity from the storage. Returns false if couldn't remove.
    bool tryRemove(const UUID & id);

    /// Removes multiple entities from the storage. Returns the list of successfully dropped.
    std::vector<UUID> tryRemove(const std::vector<UUID> & ids);

    using UpdateFunc = std::function<AccessEntityPtr(const AccessEntityPtr &)>;

    /// Updates an entity stored in the storage. Throws an exception if couldn't update.
    bool update(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists = true);
    std::vector<UUID> update(const std::vector<UUID> & ids, const UpdateFunc & update_func, bool throw_if_not_exists = true);

    /// Updates an entity stored in the storage. Returns false if couldn't update.
    bool tryUpdate(const UUID & id, const UpdateFunc & update_func);

    /// Updates multiple entities in the storage. Returns the list of successfully updated.
    std::vector<UUID> tryUpdate(const std::vector<UUID> & ids, const UpdateFunc & update_func);

    /// Finds a user, check the provided credentials and returns the ID of the user if they are valid.
    /// Throws an exception if no such user or credentials are invalid.
    UUID authenticate(const Credentials & credentials, const Poco::Net::IPAddress & address, const ExternalAuthenticators & external_authenticators, bool allow_no_password, bool allow_plaintext_password) const;
    std::optional<UUID> authenticate(const Credentials & credentials, const Poco::Net::IPAddress & address, const ExternalAuthenticators & external_authenticators, bool throw_if_user_not_exists, bool allow_no_password, bool allow_plaintext_password) const;

protected:
    virtual std::optional<UUID> findImpl(AccessEntityType type, const String & name) const = 0;
    virtual std::vector<UUID> findAllImpl(AccessEntityType type) const = 0;
    virtual AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const = 0;
    virtual std::optional<String> readNameImpl(const UUID & id, bool throw_if_not_exists) const;
    virtual std::optional<UUID> insertImpl(const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists);
    virtual bool removeImpl(const UUID & id, bool throw_if_not_exists);
    virtual bool updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists);
    virtual std::optional<UUID> authenticateImpl(const Credentials & credentials, const Poco::Net::IPAddress & address, const ExternalAuthenticators & external_authenticators, bool throw_if_user_not_exists, bool allow_no_password, bool allow_plaintext_password) const;
    virtual bool areCredentialsValid(const User & user, const Credentials & credentials, const ExternalAuthenticators & external_authenticators) const;
    virtual bool isAddressAllowed(const User & user, const Poco::Net::IPAddress & address) const;
    static UUID generateRandomID();
    Poco::Logger * getLogger() const;
    static String formatEntityTypeWithName(AccessEntityType type, const String & name) { return AccessEntityTypeInfo::get(type).formatEntityNameWithType(name); }
    [[noreturn]] void throwNotFound(const UUID & id) const;
    [[noreturn]] void throwNotFound(AccessEntityType type, const String & name) const;
    [[noreturn]] static void throwBadCast(const UUID & id, AccessEntityType type, const String & name, AccessEntityType required_type);
    [[noreturn]] void throwIDCollisionCannotInsert(
    const UUID & id, AccessEntityType type, const String & name, AccessEntityType existing_type, const String & existing_name) const;
    [[noreturn]] void throwNameCollisionCannotInsert(AccessEntityType type, const String & name) const;
    [[noreturn]] void throwNameCollisionCannotRename(AccessEntityType type, const String & old_name, const String & new_name) const;
    [[noreturn]] void throwReadonlyCannotInsert(AccessEntityType type, const String & name) const;
    [[noreturn]] void throwReadonlyCannotUpdate(AccessEntityType type, const String & name) const;
    [[noreturn]] void throwReadonlyCannotRemove(AccessEntityType type, const String & name) const;
    [[noreturn]] static void throwAddressNotAllowed(const Poco::Net::IPAddress & address);
    [[noreturn]] static void throwInvalidCredentials();
    [[noreturn]] static void throwAuthenticationTypeNotAllowed(AuthenticationType auth_type);

private:
    const String storage_name;
    mutable std::atomic<Poco::Logger *> log = nullptr;
};


template <typename EntityClassT>
std::shared_ptr<const EntityClassT> IAccessStorage::read(const UUID & id, bool throw_if_not_exists) const
{
    auto entity = readImpl(id, throw_if_not_exists);
    if constexpr (std::is_same_v<EntityClassT, IAccessEntity>)
        return entity;
    else
    {
        if (!entity)
            return nullptr;
        if (auto ptr = typeid_cast<std::shared_ptr<const EntityClassT>>(entity))
            return ptr;
        throwBadCast(id, entity->getType(), entity->getName(), EntityClassT::TYPE);
    }
}


template <typename EntityClassT>
std::shared_ptr<const EntityClassT> IAccessStorage::read(const String & name, bool throw_if_not_exists) const
{
    if (auto id = find<EntityClassT>(name))
        return read<EntityClassT>(*id, throw_if_not_exists);
    if (throw_if_not_exists)
        throwNotFound(EntityClassT::TYPE, name);
    else
        return nullptr;
}


template <typename EntityClassT>
std::shared_ptr<const EntityClassT> IAccessStorage::tryRead(const UUID & id) const
{
    return read<EntityClassT>(id, false);
}


template <typename EntityClassT>
std::shared_ptr<const EntityClassT> IAccessStorage::tryRead(const String & name) const
{
    return read<EntityClassT>(name, false);
}
}
