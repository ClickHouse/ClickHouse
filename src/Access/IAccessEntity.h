#pragma once

#include <base/types.h>
#include <Common/typeid_cast.h>
#include <Common/quoteString.h>
#include <boost/algorithm/string.hpp>
#include <memory>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_USER;
    extern const int UNKNOWN_ROLE;
    extern const int UNKNOWN_ROW_POLICY;
    extern const int UNKNOWN_QUOTA;
    extern const int THERE_IS_NO_PROFILE;
    extern const int LOGICAL_ERROR;
}


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


inline const IAccessEntity::TypeInfo & IAccessEntity::TypeInfo::get(Type type_)
{
    static constexpr auto make_info = [](const char * raw_name_, const char * plural_raw_name_, char unique_char_, int not_found_error_code_)
    {
        String init_names[2] = {raw_name_, plural_raw_name_};
        String init_aliases[2];
        for (size_t i = 0; i != std::size(init_names); ++i)
        {
            String & init_name = init_names[i];
            String & init_alias = init_aliases[i];
            boost::to_upper(init_name);
            boost::replace_all(init_name, "_", " ");
            if (auto underscore_pos = init_name.find_first_of(" "); underscore_pos != String::npos)
                init_alias = init_name.substr(underscore_pos + 1);
        }
        String init_name_for_output_with_entity_name = init_names[0];
        boost::to_lower(init_name_for_output_with_entity_name);
        return TypeInfo{raw_name_, plural_raw_name_, std::move(init_names[0]), std::move(init_aliases[0]), std::move(init_names[1]), std::move(init_aliases[1]), std::move(init_name_for_output_with_entity_name), unique_char_, not_found_error_code_};
    };

    switch (type_)
    {
        case Type::USER:
        {
            static const auto info = make_info("USER", "USERS", 'U', ErrorCodes::UNKNOWN_USER);
            return info;
        }
        case Type::ROLE:
        {
            static const auto info = make_info("ROLE", "ROLES", 'R', ErrorCodes::UNKNOWN_ROLE);
            return info;
        }
        case Type::SETTINGS_PROFILE:
        {
            static const auto info = make_info("SETTINGS_PROFILE", "SETTINGS_PROFILES", 'S', ErrorCodes::THERE_IS_NO_PROFILE);
            return info;
        }
        case Type::ROW_POLICY:
        {
            static const auto info = make_info("ROW_POLICY", "ROW_POLICIES", 'P', ErrorCodes::UNKNOWN_ROW_POLICY);
            return info;
        }
        case Type::QUOTA:
        {
            static const auto info = make_info("QUOTA", "QUOTAS", 'Q', ErrorCodes::UNKNOWN_QUOTA);
            return info;
        }
        case Type::MAX: break;
    }
    throw Exception("Unknown type: " + std::to_string(static_cast<size_t>(type_)), ErrorCodes::LOGICAL_ERROR);
}

inline String IAccessEntity::TypeInfo::outputWithEntityName(const String & entity_name) const
{
    String msg = name_for_output_with_entity_name;
    msg += " ";
    msg += backQuote(entity_name);
    return msg;
}

inline String toString(IAccessEntity::Type type)
{
    return IAccessEntity::TypeInfo::get(type).name;
}

}
