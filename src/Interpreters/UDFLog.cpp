#include <Interpreters/UDFLog.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDynamic.h>
#include <Common/DateLUTImpl.h>

namespace DB
{
ColumnsDescription UDFLogElement::getColumnsDescription()
{
    return {
        {"function_name", std::make_shared<DataTypeString>(), "Name of the user-defined function."},
        {"event_date", std::make_shared<DataTypeDate>(), "Date when the function was invoked."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Time when the function was invoked."},
        {"number_of_processed_rows", std::make_shared<DataTypeUInt64>(), "Number of rows processed by the function."},
        {"number_of_processed_bytes", std::make_shared<DataTypeUInt64>(), "Number of bytes processed by the function."},
        {"duration_ms", std::make_shared<DataTypeUInt64>(), "Wall clock time taken by the function."},
        {"constant_function_args", std::make_shared<DataTypeArray>(std::make_shared<DataTypeDynamic>()), "Constant arguments passed to the function."}
    };
}

void UDFLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insertData(function_name.data(), function_name.size());
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(number_of_processed_rows);
    columns[i++]->insert(number_of_processed_bytes);
    columns[i++]->insert(duration_ms);
    columns[i++]->insert(constant_function_args);
}
}
