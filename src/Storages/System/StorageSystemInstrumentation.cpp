#include <Storages/System/StorageSystemInstrumentation.h>

#if USE_XRAY

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Access/User.h>
#include <Access/EnabledRolesInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/InstrumentationManager.h>

namespace DB
{

ColumnsDescription StorageSystemInstrumentation::getColumnsDescription()
{
    auto entry_type_enum = std::make_shared<DataTypeEnum8> (
        DataTypeEnum8::Values
        {
            {"Entry", static_cast<Int8>(Instrumentation::EntryType::ENTRY)},
            {"Exit", static_cast<Int8>(Instrumentation::EntryType::EXIT)},
            {"EntryAndExit", static_cast<Int8>(Instrumentation::EntryType::ENTRY_AND_EXIT)},
        });

    return ColumnsDescription
    {
        {"id", std::make_shared<DataTypeUInt32>(), "ID of the instrumentation point"},
        {"function_id", std::make_shared<DataTypeInt32>(), "ID assigned to the function in xray_instr_map section of elf-binary."},
        {"function_name", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Name used to instrument the function."},
        {"handler", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Handler that was patched into instrumentation points of the function."},
        {"entry_type", entry_type_enum, "Entry type for the patch."},
        {"symbol", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Complete and demangled symbol name."},
        {"parameters", std::make_shared<DataTypeArray>(std::make_shared<DataTypeDynamic>()), "Parameters for the handler call."},
    };
}


void StorageSystemInstrumentation::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto instrumented_points = InstrumentationManager::instance().getInstrumentedPoints();

    size_t column_index = 0;
    auto & column_id = assert_cast<ColumnUInt32 &>(*res_columns[column_index++]).getData();
    auto & column_function_id = assert_cast<ColumnInt32 &>(*res_columns[column_index++]).getData();
    auto & column_function_name = assert_cast<ColumnLowCardinality &>(*res_columns[column_index++]);
    auto & column_handler_name = assert_cast<ColumnLowCardinality &>(*res_columns[column_index++]);
    auto & column_entry_type = *res_columns[column_index++];
    auto & column_symbol = assert_cast<ColumnLowCardinality &>(*res_columns[column_index++]);
    auto & column_parameters = assert_cast<ColumnArray &>(*res_columns[column_index++]);

    for (const auto & ip : instrumented_points)
    {
        column_id.push_back(static_cast<UInt32>(ip.id));
        column_function_id.push_back(ip.function_id);
        column_function_name.insert(ip.function_name);
        column_handler_name.insert(ip.handler_name);
        column_entry_type.insert(ip.entry_type);
        column_symbol.insert(ip.symbol);

        Array array;
        for (const auto & param : ip.parameters)
        {
            Field field = Field();
            if (std::holds_alternative<std::string>(param))
                field = Field(std::get<std::string>(param));
            else if (std::holds_alternative<Int64>(param))
                field = Field(std::get<Int64>(param));
            else if (std::holds_alternative<Float64>(param))
                field = Field(std::get<Float64>(param));

            array.emplace_back(field);
        }
        column_parameters.insert(array);
    }
}

}
#endif
