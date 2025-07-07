#include <Storages/System/SystemStorageInstrumentation.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Access/User.h>
#include <Access/EnabledRolesInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/XRayInstrumentationManager.h>
#include <DataTypes/DataTypeDynamic.h>
#include "Columns/ColumnDynamic.h"

#if USE_XRAY

namespace DB
{

ColumnsDescription SystemStorageInstrumentation::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"instrumentation_point_id", std::make_shared<DataTypeUInt32>(), "ID of the instrumentation point"},
        {"function_id", std::make_shared<DataTypeUInt32>(), "ID assigned to the function in xray_instr_map section of elf-binary."},
        {"function_name", std::make_shared<DataTypeString>(), "Name of the instrumented function."},
        {"handler_name", std::make_shared<DataTypeString>(), "Handler that was patched into instrumentation points of the function."},
        {"parameters", std::make_shared<DataTypeDynamic>(), "Parameters for the handler call"},
    };
}


void SystemStorageInstrumentation::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto functions_to_instrument = XRayInstrumentationManager::instance().getInstrumentedFunctions();

    size_t column_index = 0;
    auto & column_instrumentation_point_id = assert_cast<ColumnUInt32 &>(*res_columns[column_index++]).getData();
    auto & column_function_id = assert_cast<ColumnUInt32 &>(*res_columns[column_index++]).getData();
    auto & column_function_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_handler_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_parameters = assert_cast<ColumnDynamic&>(*res_columns[column_index++]);

    auto add_row = [&](UInt32 instrumentation_point_id, UInt32 function_id, const String & function_name, const String & handler_name, std::optional<std::vector<InstrumentParameter>> parameters)
    {
        column_instrumentation_point_id.push_back(instrumentation_point_id);
        column_function_id.push_back(function_id);
        column_function_name.insertData(function_name.data(), function_name.size());
        column_handler_name.insertData(handler_name.data(), handler_name.size());
        if (parameters.has_value() && !parameters->empty())
        {
            const auto & param = (*parameters)[0];

            Field field = Field(); // fallback
            if (std::holds_alternative<std::string>(param))
                field = Field(std::get<std::string>(param));
            else if (std::holds_alternative<Int64>(param))
                field = Field(std::get<Int64>(param));
            else if (std::holds_alternative<Float64>(param))
                field = Field(std::get<Float64>(param));

            column_parameters.insert(field);
        }
        else
            column_parameters.insert(Field()); // NULL
    };

    for (const auto & function : functions_to_instrument)
    {
        UInt32 instrumentation_point_id = function.instrumentation_point_id;
        UInt32 function_id = function.function_id;
        const String & function_name = function.function_name;
        const String & handler_name = function.handler_name;
        std::optional<std::vector<InstrumentParameter>> parameters = function.parameters;
        add_row(instrumentation_point_id, function_id, function_name, handler_name, parameters);
    }
}

}
#endif
