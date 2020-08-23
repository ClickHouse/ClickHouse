#pragma once

#include <Access/IAccessEntity.h>
#include <Core/UUID.h>
#include <memory>
#include <optional>


namespace DB
{
class AccessControlManager;
class ContextAccess;


/// Helper class to check visibility of access entities for the current user.
class VisibleAccessEntities
{
public:
    using EntityType = IAccessEntity::Type;

    VisibleAccessEntities(const std::shared_ptr<const ContextAccess> & access_);
    ~VisibleAccessEntities();

    std::shared_ptr<const ContextAccess> getAccess() const;
    const AccessControlManager & getAccessControlManager() const;

    /// Returns true if a specified entity can be seen by the current user.
    bool exists(const UUID & id) const;

    /// Searches for an entity with specified type and name.
    /// Returns std::nullopt if not found or cannot be seen by the current user.
    std::optional<UUID> find(EntityType type, const String & name) const;

    template <typename EntityClassT>
    std::optional<UUID> find(const String & name) const { return find(EntityClassT::TYPE, name); }

    std::vector<UUID> find(EntityType type, const Strings & names) const;

    template <typename EntityClassT>
    std::vector<UUID> find(const Strings & names) const { return find(EntityClassT::TYPE, names); }

    /// Returns the identifiers of all the entities which can be seen by the current user.
    template <typename EntityClassT>
    std::vector<UUID> findAll() const { return findAllVisible(EntityClassT::TYPE); }

    std::vector<UUID> findAll(IAccessEntity::Type type) const;

    /// Searches for an entity with specified name and type.
    /// Throws an exception if not found or cannot be seen by the current user.
    UUID getID(EntityType type, const String & name) const;

    template <typename EntityClassT>
    UUID getID(const String & name) const { return getID(EntityClassT::TYPE, name); }

    std::vector<UUID> getIDs(EntityType type, const Strings & names) const;

    template <typename EntityClassT>
    std::vector<UUID> getIDs(const Strings & names) const { return getIDs(EntityClassT::TYPE, names); }

    /// Reads an entity. Throws an exception if not found or cannot be seen by the current user.
    template <typename EntityClassT = IAccessEntity>
    std::shared_ptr<const EntityClassT> read(const String & name) const;

    /// Reads an entity. Returns nullptr if not found or cannot be seen by the current user.
    template <typename EntityClassT = IAccessEntity>
    std::shared_ptr<const EntityClassT> tryRead(const String & name) const;

private:
    AccessEntityPtr readImpl(const UUID & id) const;
    AccessEntityPtr tryReadImpl(const UUID & id) const;
    [[noreturn]] static void throwBadCast(const UUID & id, EntityType type, const String & name, EntityType required_type);

    const std::shared_ptr<const ContextAccess> access;
};


template <typename EntityClassT>
std::shared_ptr<const EntityClassT> VisibleAccessEntities::read(const String & name) const
{
    auto id = getID<EntityClassT>(name);
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
std::shared_ptr<const EntityClassT> VisibleAccessEntities::tryRead(const String & name) const
{
    auto id = find<EntityClassT>(name);
    if (!id)
        return nullptr;
    auto entity = tryReadImpl(id);
    if constexpr (std::is_same_v<EntityClassT, IAccessEntity>)
        return entity;
    else
        return typeid_cast<std::shared_ptr<const EntityClassT>>(entity);
}

}
