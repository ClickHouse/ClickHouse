#include "OpenTelemetryLog.h"

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>

namespace DB
{

Block OpenTelemetrySpanLogElement::createBlock()
{
    return {
        // event_date is the date part of event_time, used for indexing.
        {std::make_shared<DataTypeDate>(), "event_date"},
        // event_time is the span start time, named so to be compatible with
        // the standard ClickHouse system log column names.
        {std::make_shared<DataTypeDateTime>(), "event_time"},
        {std::make_shared<DataTypeUUID>(), "trace_id"},
        {std::make_shared<DataTypeUInt64>(), "span_id"},
        {std::make_shared<DataTypeUInt64>(), "parent_span_id"},
        {std::make_shared<DataTypeString>(), "operation_name"},
        {std::make_shared<DataTypeDateTime>(), "finish_time"},
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "attribute.names"},
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "attribute.values"}
    };
}

void OpenTelemetrySpanLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(DateLUT::instance().toDayNum(start_time));
    columns[i++]->insert(start_time);
    columns[i++]->insert(UInt128(Int128(trace_id)));
    columns[i++]->insert(span_id);
    columns[i++]->insert(parent_span_id);
    columns[i++]->insert(operation_name);
    columns[i++]->insert(finish_time);
    columns[i++]->insert(attribute_names);
    columns[i++]->insert(attribute_values);
}

}

