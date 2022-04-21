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
        {"hit_count", std::make_shared<DataTypeUInt64>()},
        {"miss_count", std::make_shared<DataTypeUInt64>()}};
}

void CacheLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    const auto current_time = std::chrono::system_clock::now();

    auto event_time = std::chrono::system_clock::to_time_t(current_time);
    auto event_time_microseconds = time_in_microseconds(current_time);

    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time_microseconds);

    columns[i++]->insert(query_id);
    columns[i++]->insert(hit_count);
    columns[i++]->insert(miss_count);
}

};
