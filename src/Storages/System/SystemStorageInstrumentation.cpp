#include <Storages/System/StorageSystemCurrentRoles.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Access/User.h>
#include <Access/EnabledRolesInfo.h>
#include <Interpreters/Context.h>


namespace DB
{

ColumnsDescription SystemStorageInstrumentation::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"function_id", std::make_shared<DataTypeUInt32>(), "ID assigned to the function in xray_instr_map section of elf-binary."},
        {"function_name", std::make_shared<DataTypeString>(), "Name of theinstrumented function."},
        {"handler_name", std::make_shared<DataTypeString>(), "Handler that was patched into instrumentation points of the function."},
    };
}


void SystemStorageInstrumentation::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto functions_to_instrument = XRayInstrumentationManager::instance().getFunctionsToInstrument();

    size_t column_index = 0;
    auto & column_function_id = assert_cast<ColumnUInt32 &>(*res_columns[column_index++]).getData();
    auto & column_function_name = assert_cast<ColumnString &>(*res_columns[column_index++]).getData();
    auto & column_handler_name = assert_cast<ColumnString &>(*res_columns[column_index++]).getData();

    auto add_row = [&](UInt32 function_id, const String & function_name, const String & handler_name)
    {
        column_function_id.push_back(function_id);
        column_function_name.push_back(function_name);
        column_handler_name.push_back(handler_name);
    };

    for (const auto & function : functions_to_instrument)
    {
        UInt32 function_id = function->id;
        const String & function_name = function->function_name;
        const String & handler_name = function->handler_name;
        add_row(function_id, function_name, handler_name);
    }
}

}
