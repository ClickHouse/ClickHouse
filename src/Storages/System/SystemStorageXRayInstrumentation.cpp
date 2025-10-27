#include <Storages/System/SystemStorageXRayInstrumentation.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Access/User.h>
#include <Access/EnabledRolesInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/XRayInstrumentationManager.h>
#include <xray/xray_interface.h>

#if USE_XRAY

namespace DB
{

ColumnsDescription SystemStorageXRayInstrumentation::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"id", std::make_shared<DataTypeUInt32>(), "ID of the instrumentation point"},
        {"function_id", std::make_shared<DataTypeInt32>(), "ID assigned to the function in xray_instr_map section of elf-binary."},
        {"function_name", std::make_shared<DataTypeString>(), "Name of the instrumented function."},
        {"handler", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Handler that was patched into instrumentation points of the function."},
        {"entry_type", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())), "Entry type for the patch."},
        {"parameters", std::make_shared<DataTypeDynamic>(), "Parameters for the handler call."},
    };
}


void SystemStorageXRayInstrumentation::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto instrumented_points = XRayInstrumentationManager::instance().getInstrumentedPoints();

    size_t column_index = 0;
    auto & column_id = assert_cast<ColumnUInt32 &>(*res_columns[column_index++]).getData();
    auto & column_function_id = assert_cast<ColumnInt32 &>(*res_columns[column_index++]).getData();
    auto & column_function_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_handler_name = assert_cast<ColumnLowCardinality &>(*res_columns[column_index++]);
    auto & column_entry_type = assert_cast<ColumnLowCardinality &>(*res_columns[column_index++]);
    auto & column_parameters = assert_cast<ColumnDynamic&>(*res_columns[column_index++]);

    auto add_row = [&](UInt32 id, Int32 function_id, const String & function_name, const String & handler_name, std::optional<XRayEntryType> entry_type, std::optional<std::vector<XRayInstrumentationManager::InstrumentedParameter>> parameters)
    {
        column_id.push_back(id);
        column_function_id.push_back(function_id);
        column_function_name.insertData(function_name.data(), function_name.size());
        column_handler_name.insertData(handler_name.data(), handler_name.size());

        if (entry_type.has_value())
        {
            const String entry_type_string = entry_type == XRayEntryType::ENTRY ? "entry" : "exit";
            column_entry_type.insertData(entry_type_string.data(), entry_type_string.size());
        }
        else
            column_entry_type.insert(Field());

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
            column_parameters.insert(Field()); /// NULL
    };

    for (const auto & ip : instrumented_points)
        add_row(ip.id, ip.function_id, ip.function_name, ip.handler_name, ip.entry_type, ip.parameters);
}

}
#endif
