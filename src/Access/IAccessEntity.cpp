#include <Access/IAccessEntity.h>
#include <Common/quoteString.h>
#include <boost/algorithm/string.hpp>


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

bool IAccessEntity::equal(const IAccessEntity & other) const
{
    return (name == other.name) && (getType() == other.getType());
}

const IAccessEntity::TypeInfo & IAccessEntity::TypeInfo::get(Type type_)
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

String IAccessEntity::TypeInfo::outputWithEntityName(const String & entity_name) const
{
    String msg = name_for_output_with_entity_name;
    msg += " ";
    msg += backQuote(entity_name);
    return msg;
}

String toString(IAccessEntity::Type type)
{
    return IAccessEntity::TypeInfo::get(type).name;
}

}
