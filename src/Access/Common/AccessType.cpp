#include <Access/Common/AccessType.h>
#include <boost/algorithm/string/replace.hpp>
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
#define ACCESS_TYPE_TO_STRING_CONVERTER_ADD_TO_MAPPING(name, aliases, node_type, parent_group_name) \
            addToMapping(AccessType::name, #name);

            APPLY_FOR_ACCESS_TYPES(ACCESS_TYPE_TO_STRING_CONVERTER_ADD_TO_MAPPING)

#undef ACCESS_TYPE_TO_STRING_CONVERTER_ADD_TO_MAPPING
        }

        void addToMapping(AccessType type, std::string_view str)
        {
            String str2{str};
            boost::replace_all(str2, "_", " ");
            size_t index = static_cast<size_t>(type);
            access_type_to_string_mapping.resize(std::max(index + 1, access_type_to_string_mapping.size()));
            access_type_to_string_mapping[index] = str2;
        }

        Strings access_type_to_string_mapping;
    };
}

std::string_view toString(AccessType type)
{
    return AccessTypeToStringConverter::instance().convert(type);
}

}
