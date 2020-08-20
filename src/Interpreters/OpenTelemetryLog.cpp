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
        {std::make_shared<DataTypeUUID>(), "trace_id"},
        {std::make_shared<DataTypeUInt64>(), "span_id"},
        {std::make_shared<DataTypeUInt64>(), "parent_span_id"},
        {std::make_shared<DataTypeString>(), "operation_name"},
        {std::make_shared<DataTypeDate>(), "start_date"},
        {std::make_shared<DataTypeDateTime>(), "start_time"},
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

    columns[i++]->insert(trace_id);
    columns[i++]->insert(span_id);
    columns[i++]->insert(parent_span_id);
    columns[i++]->insert(operation_name);
    columns[i++]->insert(DateLUT::instance().toDayNum(start_time));
    columns[i++]->insert(start_time);
    columns[i++]->insert(finish_time);
    columns[i++]->insert(attribute_names);
    columns[i++]->insert(attribute_values);
}

}

