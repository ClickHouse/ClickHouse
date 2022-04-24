#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/CacheLog.h>


namespace DB
{

NamesAndTypesList CacheLogElement::getNamesAndTypes()
{
    return {
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"query_id", std::make_shared<DataTypeString>()},
        {"remote_file_path", std::make_shared<DataTypeString>()},
        {"hit_count", std::make_shared<DataTypeUInt64>()},
        {"miss_count", std::make_shared<DataTypeUInt64>()},
        /// {"hit_ratio", std::make_shared<DataTypeFloat64>()},
    };
}

inline UInt64 time_in_milliseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(timepoint.time_since_epoch()).count();
}

inline UInt64 time_in_microseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(timepoint.time_since_epoch()).count();
}

inline UInt64 time_in_seconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::seconds>(timepoint.time_since_epoch()).count();
}

void CacheLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    const auto current_time = std::chrono::system_clock::now();
    auto event_time = std::chrono::system_clock::to_time_t(current_time);

    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);

    columns[i++]->insert(query_id);
    columns[i++]->insert(remote_file_path);
    columns[i++]->insert(hit_count);
    columns[i++]->insert(miss_count);

    /// if (hit_count + miss_count)
    /// {
    ///     double hit_ratio = 1.0 * hit_count / (hit_count + miss_count);
    ///     columns[i++]->insert(hit_ratio);
    /// }
    /// else
    ///     columns[i++]->insert(0);
}

};
