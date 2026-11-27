#include <Interpreters/LLMQueryLog.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>
#include <Common/config_version.h>

namespace DB
{

ColumnsDescription LLMQueryLogElement::getColumnsDescription()
{
    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    return ColumnsDescription{
        {"hostname", low_cardinality_string, "Hostname of the server executing the request."},
        {"clickhouse_version", low_cardinality_string, "Version of the ClickHouse server that produced the row."},
        {"system_processor", low_cardinality_string, "CPU architecture of the ClickHouse server that produced the row."},
        {"event_date", std::make_shared<DataTypeDate>(), "Date of the event."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Time of the event."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Time of the event with microsecond precision."},
        {"type",
         std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
             {"RequestStart", RequestStart}, {"RequestFinish", RequestFinish}, {"RequestException", RequestException}}),
         "Request lifecycle event."},
        {"request_id", std::make_shared<DataTypeUUID>(), "Identifier shared by the start and terminal events."},
        {"query_id", std::make_shared<DataTypeString>(), "Query identifier on this server."},
        {"initial_query_id", std::make_shared<DataTypeString>(), "Initial query identifier."},
        {"user", std::make_shared<DataTypeString>(), "User executing the request."},
        {"route", std::make_shared<DataTypeString>(), "Logical model route."},
        {"deployment", std::make_shared<DataTypeString>(), "Selected deployment."},
        {"model", std::make_shared<DataTypeString>(), "Upstream model."},
        {"config_version", std::make_shared<DataTypeUInt64>(), "Keeper configuration version used by this request."},
        {"prompt", std::make_shared<DataTypeString>(), "User prompt, recorded on the start event."},
        {"system_prompt", std::make_shared<DataTypeString>(), "System prompt, recorded on the start event."},
        {"max_tokens", std::make_shared<DataTypeUInt64>(), "Requested output token limit."},
        {"temperature", std::make_shared<DataTypeFloat64>(), "Requested sampling temperature."},
        {"response", std::make_shared<DataTypeString>(), "Model response, recorded on the terminal event."},
        {"finish_reason", std::make_shared<DataTypeString>(), "Provider finish reason, recorded on the terminal event."},
        {"input_tokens", std::make_shared<DataTypeUInt64>(), "Input tokens reported by the provider."},
        {"output_tokens", std::make_shared<DataTypeUInt64>(), "Output tokens reported by the provider."},
        {"duration_microseconds", std::make_shared<DataTypeUInt64>(), "Request duration in microseconds."},
        {"exception_code", std::make_shared<DataTypeInt32>(), "Exception code, or zero on success."},
    };
}

void LLMQueryLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(VERSION_STRING);
    columns[i++]->insert(SYSTEM_PROCESSOR);
    columns[i++]->insert(UInt64(DateLUT::instance().toDayNum(event_time).toUnderType()));
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);
    columns[i++]->insert(type);
    columns[i++]->insert(request_id);
    columns[i++]->insert(query_id);
    columns[i++]->insert(initial_query_id);
    columns[i++]->insert(user);
    columns[i++]->insert(route);
    columns[i++]->insert(deployment);
    columns[i++]->insert(model);
    columns[i++]->insert(config_version);
    columns[i++]->insert(prompt);
    columns[i++]->insert(system_prompt);
    columns[i++]->insert(max_tokens);
    columns[i++]->insert(temperature);
    columns[i++]->insert(response);
    columns[i++]->insert(finish_reason);
    columns[i++]->insert(input_tokens);
    columns[i++]->insert(output_tokens);
    columns[i++]->insert(duration_microseconds);
    columns[i++]->insert(exception_code);
}

}
