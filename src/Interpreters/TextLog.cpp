#include <Interpreters/TextLog.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeString.h>
#include <Common/ClickHouseRevision.h>
#include <array>

namespace DB
{

Block TextLogElement::createBlock()
{
    auto priority_datatype = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
                {"Fatal",          static_cast<Int8>(Message::PRIO_FATAL)},
                {"Critical",       static_cast<Int8>(Message::PRIO_CRITICAL)},
                {"Error",          static_cast<Int8>(Message::PRIO_ERROR)},
                {"Warning",        static_cast<Int8>(Message::PRIO_WARNING)},
                {"Notice",         static_cast<Int8>(Message::PRIO_NOTICE)},
                {"Information",    static_cast<Int8>(Message::PRIO_INFORMATION)},
                {"Debug",          static_cast<Int8>(Message::PRIO_DEBUG)},
                {"Trace",          static_cast<Int8>(Message::PRIO_TRACE)}
        });

    return
    {
        {std::make_shared<DataTypeDate>(),                                                    "event_date"},
        {std::make_shared<DataTypeDateTime>(),                                                "event_time"},
        {std::make_shared<DataTypeUInt32>(),                                                  "microseconds"},

        {std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),        "thread_name"},
        {std::make_shared<DataTypeUInt64>(),                                                  "thread_id"},

        {std::move(priority_datatype),                                                        "level"},
        {std::make_shared<DataTypeString>(),                                                  "query_id"},
        {std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),        "logger_name"},
        {std::make_shared<DataTypeString>(),                                                  "message"},

        {std::make_shared<DataTypeUInt32>(),                                                  "revision"},

        {std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),        "source_file"},
        {std::make_shared<DataTypeUInt64>(),                                                  "source_line"}
    };
}

void TextLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(DateLUT::instance().toDayNum(event_time));
    columns[i++]->insert(event_time);
    columns[i++]->insert(microseconds);

    columns[i++]->insertData(thread_name.data(), thread_name.size());
    columns[i++]->insert(thread_id);

    columns[i++]->insert(level);
    columns[i++]->insert(query_id);
    columns[i++]->insert(logger_name);
    columns[i++]->insert(message);

    columns[i++]->insert(ClickHouseRevision::get());

    columns[i++]->insert(source_file);
    columns[i++]->insert(source_line);
}

TextLog::TextLog(Context & context_, const String & database_name_,
        const String & table_name_, const String & storage_def_,
        size_t flush_interval_milliseconds_)
  : SystemLog<TextLogElement>(context_, database_name_, table_name_,
        storage_def_, flush_interval_milliseconds_)
{
    // SystemLog methods may write text logs, so we disable logging for the text
    // log table to avoid recursion.
    log->setLevel(0);
}

}
