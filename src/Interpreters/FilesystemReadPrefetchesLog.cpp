#include <Interpreters/FilesystemReadPrefetchesLog.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

NamesAndTypesList FilesystemReadPrefetchesLogElement::getNamesAndTypes()
{
    DataTypes types{
        std::make_shared<DataTypeNumber<UInt64>>(),
        std::make_shared<DataTypeNumber<UInt64>>(),
    };

    return {
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"query_id", std::make_shared<DataTypeString>()},
        {"path", std::make_shared<DataTypeString>()},
        {"offset", std::make_shared<DataTypeUInt64>()},
        {"size", std::make_shared<DataTypeInt64>()},
        {"prefetch_time_ms", std::make_shared<DataTypeDateTime64>(6)},
        {"state", std::make_shared<DataTypeString>()}, /// Was this prefetch used or we downloaded it in vain?
        {"thread_id", std::make_shared<DataTypeUInt64>()},
        {"reader_id", std::make_shared<DataTypeString>()},
    };
}

void FilesystemReadPrefetchesLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(query_id);
    columns[i++]->insert(path);
    columns[i++]->insert(offset);
    columns[i++]->insert(size);
    columns[i++]->insert(prefetch_start_time);
    columns[i++]->insert(magic_enum::enum_name(state));
    columns[i++]->insert(thread_id);
    columns[i++]->insert(reader_id);
}

}
