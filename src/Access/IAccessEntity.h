#pragma once

#include <Core/Types.h>
#include <Common/typeid_cast.h>
#include <memory>
#include <typeindex>


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

    std::type_index getType() const { return typeid(*this); }
    static String getTypeName(std::type_index type);
    const String getTypeName() const { return getTypeName(getType()); }
    static const char * getKeyword(std::type_index type);
    const char * getKeyword() const { return getKeyword(getType()); }

    template <typename EntityType>
    bool isTypeOf() const { return isTypeOf(typeid(EntityType)); }
    bool isTypeOf(std::type_index type) const { return type == getType(); }

    virtual void setName(const String & name_) { name = name_; }
    const String & getName() const { return name; }

    friend bool operator ==(const IAccessEntity & lhs, const IAccessEntity & rhs) { return lhs.equal(rhs); }
    friend bool operator !=(const IAccessEntity & lhs, const IAccessEntity & rhs) { return !(lhs == rhs); }

protected:
    String name;

    virtual bool equal(const IAccessEntity & other) const;

    /// Helper function to define clone() in the derived classes.
    template <typename EntityType>
    std::shared_ptr<IAccessEntity> cloneImpl() const
    {
        return std::make_shared<EntityType>(typeid_cast<const EntityType &>(*this));
    }
};

using AccessEntityPtr = std::shared_ptr<const IAccessEntity>;
}
