#include "Storages/ColumnsDescription.h"
#include <base/getFQDNOrHostName.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeMap.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Interpreters/FilesystemCacheLog.h>


namespace DB
{

static String typeToString(FilesystemCacheLogElement::CacheType type)
{
    return String(magic_enum::enum_name(type));
}

ColumnsDescription FilesystemCacheLogElement::getColumnsDescription()
{
    DataTypes types{
        std::make_shared<DataTypeNumber<UInt64>>(),
        std::make_shared<DataTypeNumber<UInt64>>(),
    };

    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    return ColumnsDescription
    {
        {"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Hostname"},
        {"event_date", std::make_shared<DataTypeDate>(), "Event date"},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Event time"},
        {"query_id", std::make_shared<DataTypeString>(), "Id of the query"},
        {"source_file_path", std::make_shared<DataTypeString>(), "File segment path on filesystem"},
        {"file_segment_range", std::make_shared<DataTypeTuple>(types), "File segment range"},
        {"total_requested_range", std::make_shared<DataTypeTuple>(types), "Full read range"},
        {"key", std::make_shared<DataTypeString>(), "File segment key"},
        {"offset", std::make_shared<DataTypeUInt64>(), "File segment offset"},
        {"size", std::make_shared<DataTypeUInt64>(), "Read size"},
        {"read_type", std::make_shared<DataTypeString>(), "Read type: READ_FROM_CACHE, READ_FROM_FS_AND_DOWNLOADED_TO_CACHE, READ_FROM_FS_BYPASSING_CACHE"},
        {"read_from_cache_attempted", std::make_shared<DataTypeUInt8>(), "Whether reading from cache was attempted"},
        {"ProfileEvents", std::make_shared<DataTypeMap>(low_cardinality_string, std::make_shared<DataTypeUInt64>()), "Profile events collected while reading this file segment"},
        {"read_buffer_id", std::make_shared<DataTypeString>(), "Internal implementation read buffer id"},
    };
}

void FilesystemCacheLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);

    columns[i++]->insert(query_id);

    columns[i++]->insert(source_file_path);
    columns[i++]->insert(Tuple{file_segment_range.first, file_segment_range.second});
    columns[i++]->insert(Tuple{requested_range.first, requested_range.second});
    columns[i++]->insert(file_segment_key);
    columns[i++]->insert(file_segment_offset);
    columns[i++]->insert(file_segment_size);
    columns[i++]->insert(typeToString(cache_type));
    columns[i++]->insert(read_from_cache_attempted);

    if (profile_counters)
    {
        auto * column = columns[i++].get();
        ProfileEvents::dumpToMapColumn(*profile_counters, column, true);
    }
    else
    {
        columns[i++]->insertDefault();
    }

    columns[i++]->insert(read_buffer_id);
}

}
