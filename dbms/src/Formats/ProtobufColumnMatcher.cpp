#include <Common/config.h>
#if USE_PROTOBUF

#include <Common/Exception.h>
#include <Formats/ProtobufColumnMatcher.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <Poco/String.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NO_COMMON_COLUMNS_WITH_PROTOBUF_SCHEMA;
}


namespace
{
    String columnNameToSearchableForm(const String & str)
    {
        return Poco::replace(Poco::toUpper(str), ".", "_");
    }
}

namespace ProtobufColumnMatcher
{
    namespace details
    {
        ColumnNameMatcher::ColumnNameMatcher(const std::vector<String> & column_names) : column_usage(column_names.size())
        {
            column_usage.resize(column_names.size(), false);
            for (size_t i = 0; i != column_names.size(); ++i)
                column_name_to_index_map.emplace(columnNameToSearchableForm(column_names[i]), i);
        }

        size_t ColumnNameMatcher::findColumn(const String & field_name)
        {
            auto it = column_name_to_index_map.find(columnNameToSearchableForm(field_name));
            if (it == column_name_to_index_map.end())
                return -1;
            size_t column_index = it->second;
            if (column_usage[column_index])
                return -1;
            column_usage[column_index] = true;
            return column_index;
        }

        void throwNoCommonColumns()
        {
            throw Exception("No common columns with provided protobuf schema", ErrorCodes::NO_COMMON_COLUMNS_WITH_PROTOBUF_SCHEMA);
        }
    }
}

}
#endif
