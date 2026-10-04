#include <Access/Common/AccessType.h>
#include <algorithm>
#include <array>
#include <vector>


namespace DB
{

namespace
{
    using Strings = std::vector<String>;

    class AccessTypeToStringConverter
    {
    public:
        static const AccessTypeToStringConverter & instance()
        {
            static const AccessTypeToStringConverter res;
            return res;
        }

        std::string_view convert(AccessType type) const
        {
            return access_type_to_string_mapping[static_cast<size_t>(type)];
        }

    private:
        AccessTypeToStringConverter()
        {
            /// The enumerators of `AccessType` are declared from this same list, in this order, so
            /// the index into the table is the access type. Expanding the conversion at each of the
            /// 258 call sites instead compiled to 11 KB.
#define ACCESS_TYPE_TO_STRING_CONVERTER_ADD_TO_MAPPING(name, aliases, node_type, parent_group_name) \
            std::string_view{#name},

            static constexpr std::array names_with_underscores
            {
                APPLY_FOR_ACCESS_TYPES(ACCESS_TYPE_TO_STRING_CONVERTER_ADD_TO_MAPPING, ACCESS_TYPE_TO_STRING_CONVERTER_ADD_TO_MAPPING)
            };

#undef ACCESS_TYPE_TO_STRING_CONVERTER_ADD_TO_MAPPING

            access_type_to_string_mapping.reserve(names_with_underscores.size());
            for (std::string_view name : names_with_underscores)
            {
                String & converted = access_type_to_string_mapping.emplace_back(name);
                std::replace(converted.begin(), converted.end(), '_', ' ');
            }
        }

        Strings access_type_to_string_mapping;
    };

    /// Indexed by the access type, like the table above.
#define ACCESS_TYPE_NOT_OBSOLETE_TABLE_ENTRY(name, aliases, node_type, parent_group_name) false,
#define ACCESS_TYPE_OBSOLETE_TABLE_ENTRY(name, aliases, node_type, parent_group_name) true,

    constexpr std::array obsolete_access_types
    {
        APPLY_FOR_ACCESS_TYPES(ACCESS_TYPE_NOT_OBSOLETE_TABLE_ENTRY, ACCESS_TYPE_OBSOLETE_TABLE_ENTRY)
    };

#undef ACCESS_TYPE_NOT_OBSOLETE_TABLE_ENTRY
#undef ACCESS_TYPE_OBSOLETE_TABLE_ENTRY
}

std::string_view toString(AccessType type)
{
    return AccessTypeToStringConverter::instance().convert(type);
}

bool isObsolete(AccessType type)
{
    return obsolete_access_types[static_cast<size_t>(type)];
}

}
