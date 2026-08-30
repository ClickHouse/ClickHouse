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
                APPLY_FOR_ACCESS_TYPES(ACCESS_TYPE_TO_STRING_CONVERTER_ADD_TO_MAPPING)
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
}

std::string_view toString(AccessType type)
{
    return AccessTypeToStringConverter::instance().convert(type);
}

}
