#pragma once

#include <Access/Common/AccessEntityType.h>
#include <Common/typeid_cast.h>
#include <base/types.h>
#include <memory>
#include <unordered_map>


namespace DB
{

/// Access entity is a set of data which have a name and a type. Access entity control something related to the access control.
/// Entities can be stored to a file or another storage, see IAccessStorage.
struct IAccessEntity
{
    IAccessEntity() = default;
    IAccessEntity(const IAccessEntity &) = default;
    virtual ~IAccessEntity() = default;
    virtual std::shared_ptr<IAccessEntity> clone() const = 0;

    virtual AccessEntityType getType() const = 0;

    const AccessEntityTypeInfo & getTypeInfo() const { return AccessEntityTypeInfo::get(getType()); }
    String formatTypeWithName() const { return getTypeInfo().formatEntityNameWithType(getName()); }

    template <typename EntityClassT>
    bool isTypeOf() const { return isTypeOf(EntityClassT::TYPE); }
    bool isTypeOf(AccessEntityType type) const { return type == getType(); }

    virtual void setName(const String & name_) { name = name_; }
    const String & getName() const { return name; }

    friend bool operator ==(const IAccessEntity & lhs, const IAccessEntity & rhs) { return lhs.equal(rhs); }
    friend bool operator !=(const IAccessEntity & lhs, const IAccessEntity & rhs) { return !(lhs == rhs); }

    struct LessByName
    {
        bool operator()(const IAccessEntity & lhs, const IAccessEntity & rhs) const { return (lhs.getName() < rhs.getName()); }
        bool operator()(const std::shared_ptr<const IAccessEntity> & lhs, const std::shared_ptr<const IAccessEntity> & rhs) const { return operator()(*lhs, *rhs); }
    };

    struct LessByTypeAndName
    {
        bool operator()(const IAccessEntity & lhs, const IAccessEntity & rhs) const { return (lhs.getType() < rhs.getType()) || ((lhs.getType() == rhs.getType()) && (lhs.getName() < rhs.getName())); }
        bool operator()(const std::shared_ptr<const IAccessEntity> & lhs, const std::shared_ptr<const IAccessEntity> & rhs) const { return operator()(*lhs, *rhs); }
    };

    /// Finds all dependencies.
    virtual std::vector<UUID> findDependencies() const { return {}; }

    /// Replaces dependencies according to a specified map.
    void replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids) { doReplaceDependencies(old_to_new_ids); }
    static void replaceDependencies(std::shared_ptr<const IAccessEntity> & entity, const std::unordered_map<UUID, UUID> & old_to_new_ids);

    /// Whether this access entity should be written to a backup.
    virtual bool isBackupAllowed() const { return false; }

protected:
    String name;

    virtual bool equal(const IAccessEntity & other) const;

    /// Helper function to define clone() in the derived classes.
    template <typename EntityClassT>
    std::shared_ptr<IAccessEntity> cloneImpl() const
    {
        return std::make_shared<EntityClassT>(typeid_cast<const EntityClassT &>(*this));
    }

    virtual void doReplaceDependencies(const std::unordered_map<UUID, UUID> & /* old_to_new_ids */) {}
};

using AccessEntityPtr = std::shared_ptr<const IAccessEntity>;

}
