#include <base/getFQDNOrHostName.h>
#include <Interpreters/TextLog.h>

#include <Common/ClickHouseRevision.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/logger_useful.h>

#include <array>

namespace DB
{

ColumnsDescription TextLogElement::getColumnsDescription()
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
                {"Trace",          static_cast<Int8>(Message::PRIO_TRACE)},
                {"Test",           static_cast<Int8>(Message::PRIO_TEST)},
        });

    return ColumnsDescription
    {
        {"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Hostname of the server executing the query."},
        {"event_date", std::make_shared<DataTypeDate>(), "Date of the entry."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Time of the entry."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Time of the entry with microseconds precision."},

        {"thread_name", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Name of the thread from which the logging was done."},
        {"thread_id", std::make_shared<DataTypeUInt64>(), "OS thread ID."},

        {"level", std::move(priority_datatype), "Entry level. Possible values: 1 or 'Fatal', 2 or 'Critical', 3 or 'Error', 4 or 'Warning', 5 or 'Notice', 6 or 'Information', 7 or 'Debug', 8 or 'Trace'."},
        {"query_id", std::make_shared<DataTypeString>(), "ID of the query."},
        {"logger_name", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Name of the logger (i.e. DDLWorker)."},
        {"message", std::make_shared<DataTypeString>(), "The message itself."},

        {"revision", std::make_shared<DataTypeUInt32>(), "ClickHouse revision."},

        {"source_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Source file from which the logging was done."},
        {"source_line", std::make_shared<DataTypeUInt64>(), "Source line from which the logging was done."},

        {"message_format_string", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "A format string that was used to format the message."},
        {"value1", std::make_shared<DataTypeString>(), "Argument 1 that was used to format the message."},
        {"value2", std::make_shared<DataTypeString>(), "Argument 2 that was used to format the message."},
        {"value3", std::make_shared<DataTypeString>(), "Argument 3 that was used to format the message."},
        {"value4", std::make_shared<DataTypeString>(), "Argument 4 that was used to format the message."},
        {"value5", std::make_shared<DataTypeString>(), "Argument 5 that was used to format the message."},
        {"value6", std::make_shared<DataTypeString>(), "Argument 6 that was used to format the message."},
        {"value7", std::make_shared<DataTypeString>(), "Argument 7 that was used to format the message."},
        {"value8", std::make_shared<DataTypeString>(), "Argument 8 that was used to format the message."},
        {"value9", std::make_shared<DataTypeString>(), "Argument 9 that was used to format the message."},
        {"value10", std::make_shared<DataTypeString>(), "Argument 10 that was used to format the message."},
    };
}

void TextLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);

    columns[i++]->insertData(thread_name.data(), thread_name.size());
    columns[i++]->insert(thread_id);

    columns[i++]->insert(level);
    columns[i++]->insert(query_id);
    columns[i++]->insert(logger_name);
    columns[i++]->insert(message);

    columns[i++]->insert(ClickHouseRevision::getVersionRevision());

    columns[i++]->insert(source_file);
    columns[i++]->insert(source_line);

    columns[i++]->insert(message_format_string);
    columns[i++]->insert(value1);
    columns[i++]->insert(value2);
    columns[i++]->insert(value3);
    columns[i++]->insert(value4);
    columns[i++]->insert(value5);
    columns[i++]->insert(value6);
    columns[i++]->insert(value7);
    columns[i++]->insert(value8);
    columns[i++]->insert(value9);
    columns[i++]->insert(value10);
}

TextLog::TextLog(ContextPtr context_,
                 const SystemLogSettings & settings)
    : SystemLog<TextLogElement>(context_, settings, getLogQueue(settings.queue_settings))
{
}

}
