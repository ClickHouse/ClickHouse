#include <Interpreters/TextLog.h>
#include <DataTypes/DataTypeEnum.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Common/ClickHouseRevision.h>
#include <Poco/Net/IPAddress.h>
#include <array>

namespace DB
{

template <> struct NearestFieldTypeImpl<Message::Priority> { using Type = UInt64; };

Block TextLogElement::createBlock()
{
    auto priority_datatype = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
                {"FATAL",          static_cast<Int8>(Message::PRIO_FATAL)},
                {"CRITICAL",       static_cast<Int8>(Message::PRIO_CRITICAL)},
                {"ERROR",          static_cast<Int8>(Message::PRIO_ERROR)},
                {"WARNING",        static_cast<Int8>(Message::PRIO_WARNING)},
                {"NOTICE",         static_cast<Int8>(Message::PRIO_NOTICE)},
                {"INFORMATION",    static_cast<Int8>(Message::PRIO_INFORMATION)},
                {"DEBUG",          static_cast<Int8>(Message::PRIO_DEBUG)},
                {"TRACE",          static_cast<Int8>(Message::PRIO_TRACE)}
        });

    return
    {
        {std::make_shared<DataTypeDate>(),                                                    "event_date"},
        {std::make_shared<DataTypeDateTime>(),                                                "event_time"},
        {std::make_shared<DataTypeUInt32>(),                                                  "microseconds"},

        {std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),        "thread_name"},
        {std::make_shared<DataTypeUInt32>(),                                                  "thread_number"},
        {std::make_shared<DataTypeUInt32>(),                                                  "os_thread_id"},

        {std::move(priority_datatype),                                                        "level"},
        {std::make_shared<DataTypeString>(),                                                  "query_id"},
        {std::make_shared<DataTypeString>(),                                                  "logger_name"},
        {std::make_shared<DataTypeString>(),                                                  "message"},

        {std::make_shared<DataTypeUInt32>(),                                                  "revision"},

        {std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),        "source_file"},
        {std::make_shared<DataTypeUInt64>(),                                                  "source_line"}
    };
}

void TextLogElement::appendToBlock(Block & block) const
{
    MutableColumns columns = block.mutateColumns();

    size_t i = 0;

    columns[i++]->insert(DateLUT::instance().toDayNum(event_time));
    columns[i++]->insert(event_time);
    columns[i++]->insert(microseconds);

    columns[i++]->insertData(thread_name.data(), thread_name.size());
    columns[i++]->insert(thread_number);
    columns[i++]->insert(os_thread_id);

    columns[i++]->insert(level);
    columns[i++]->insert(query_id);
    columns[i++]->insert(logger_name);
    columns[i++]->insert(message);

    columns[i++]->insert(ClickHouseRevision::get());

    columns[i++]->insert(source_file);
    columns[i++]->insert(source_line);
}

}
