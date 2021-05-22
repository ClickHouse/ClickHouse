#pragma once

#include <common/types.h>
#include <Common/typeid_cast.h>
#include <memory>


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

    enum class Type
    {
        USER,
        ROLE,
        SETTINGS_PROFILE,
        ROW_POLICY,
        QUOTA,

        MAX,
    };

    virtual Type getType() const = 0;

    struct TypeInfo
    {
        const char * const raw_name;
        const char * const plural_raw_name;
        const String name;  /// Uppercased with spaces instead of underscores, e.g. "SETTINGS PROFILE".
        const String alias; /// Alias of the keyword or empty string, e.g. "PROFILE".
        const String plural_name;  /// Uppercased with spaces plural name, e.g. "SETTINGS PROFILES".
        const String plural_alias; /// Uppercased with spaces plural name alias, e.g. "PROFILES".
        const String name_for_output_with_entity_name; /// Lowercased with spaces instead of underscores, e.g. "settings profile".
        const char unique_char;     /// Unique character for this type. E.g. 'P' for SETTINGS_PROFILE.
        const int not_found_error_code;

        static const TypeInfo & get(Type type_);
        String outputWithEntityName(const String & entity_name) const;
    };

    const TypeInfo & getTypeInfo() const { return TypeInfo::get(getType()); }
    String outputTypeAndName() const { return getTypeInfo().outputWithEntityName(getName()); }

    template <typename EntityClassT>
    bool isTypeOf() const { return isTypeOf(EntityClassT::TYPE); }
    bool isTypeOf(Type type) const { return type == getType(); }

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

protected:
    String name;

    virtual bool equal(const IAccessEntity & other) const;

    /// Helper function to define clone() in the derived classes.
    template <typename EntityClassT>
    std::shared_ptr<IAccessEntity> cloneImpl() const
    {
        return std::make_shared<EntityClassT>(typeid_cast<const EntityClassT &>(*this));
    }
};

using AccessEntityPtr = std::shared_ptr<const IAccessEntity>;

String toString(IAccessEntity::Type type);

}
