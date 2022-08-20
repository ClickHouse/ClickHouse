#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeMap.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Interpreters/FilesystemCacheLog.h>


namespace DB
{

static String typeToString(FilesystemCacheLogElement::CacheType type)
{
    switch (type)
    {
        case FilesystemCacheLogElement::CacheType::READ_FROM_CACHE:
            return "READ_FROM_CACHE";
        case FilesystemCacheLogElement::CacheType::READ_FROM_FS_AND_DOWNLOADED_TO_CACHE:
            return "READ_FROM_FS_AND_DOWNLOADED_TO_CACHE";
        case FilesystemCacheLogElement::CacheType::READ_FROM_FS_BYPASSING_CACHE:
            return "READ_FROM_FS_BYPASSING_CACHE";
        case FilesystemCacheLogElement::CacheType::WRITE_THROUGH_CACHE:
            return "READ_FROM_FS_BYPASSING_CACHE";
    }
    __builtin_unreachable();
}

NamesAndTypesList FilesystemCacheLogElement::getNamesAndTypes()
{
    DataTypes types{
        std::make_shared<DataTypeNumber<UInt64>>(),
        std::make_shared<DataTypeNumber<UInt64>>(),
    };

    return {
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"query_id", std::make_shared<DataTypeString>()},
        {"source_file_path", std::make_shared<DataTypeString>()},
        {"file_segment_range", std::make_shared<DataTypeTuple>(types)},
        {"total_requested_range", std::make_shared<DataTypeTuple>(types)},
        {"size", std::make_shared<DataTypeUInt64>()},
        {"read_type", std::make_shared<DataTypeString>()},
        {"cache_attempted", std::make_shared<DataTypeUInt8>()},
        {"ProfileEvents", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeUInt64>())},
        {"read_buffer_id", std::make_shared<DataTypeString>()},
    };
}

void FilesystemCacheLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);

    columns[i++]->insert(query_id);

    columns[i++]->insert(source_file_path);
    columns[i++]->insert(Tuple{file_segment_range.first, file_segment_range.second});
    columns[i++]->insert(Tuple{requested_range.first, requested_range.second});
    columns[i++]->insert(file_segment_size);
    columns[i++]->insert(typeToString(cache_type));
    columns[i++]->insert(cache_attempted);

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
