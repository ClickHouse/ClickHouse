#include <Storages/System/StorageSystemInstrumentation.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Access/User.h>
#include <Access/EnabledRolesInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/InstrumentationManager.h>
#include <xray/xray_interface.h>

#if USE_XRAY

namespace DB
{

static const Int8 ENTRY_AND_EXIT = XRayEntryType::EXIT + 1;

ColumnsDescription StorageSystemInstrumentation::getColumnsDescription()
{
    auto entry_type_enum = std::make_shared<DataTypeEnum8> (
        DataTypeEnum8::Values
        {
            {"Entry", static_cast<Int8>(XRayEntryType::ENTRY)},
            {"Exit", static_cast<Int8>(XRayEntryType::EXIT)},
            {"EntryAndExit", static_cast<Int8>(ENTRY_AND_EXIT)},
        });

    return ColumnsDescription
    {
        {"id", std::make_shared<DataTypeUInt32>(), "ID of the instrumentation point"},
        {"function_id", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeInt32>()), "ID assigned to the function in xray_instr_map section of elf-binary."},
        {"function_name", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Name used to instrument the function."},
        {"handler", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Handler that was patched into instrumentation points of the function."},
        {"entry_type", entry_type_enum, "Entry type for the patch."},
        {"symbol", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Complete and demangled symbol name."},
        {"parameters", std::make_shared<DataTypeDynamic>(), "Parameters for the handler call."},
    };
}


void StorageSystemInstrumentation::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto instrumented_points = InstrumentationManager::instance().getInstrumentedPoints();

    size_t column_index = 0;
    auto & column_id = assert_cast<ColumnUInt32 &>(*res_columns[column_index++]).getData();
    auto & column_function_id = assert_cast<ColumnLowCardinality &>(*res_columns[column_index++]);
    auto & column_function_name = assert_cast<ColumnLowCardinality &>(*res_columns[column_index++]);
    auto & column_handler_name = assert_cast<ColumnLowCardinality &>(*res_columns[column_index++]);
    auto & column_entry_type = *res_columns[column_index++];
    auto & column_symbol = assert_cast<ColumnLowCardinality &>(*res_columns[column_index++]);
    auto & column_parameters = assert_cast<ColumnDynamic&>(*res_columns[column_index++]);

    auto add_row = [&](UInt32 id, Int32 function_id, const String & function_name, const String & handler_name, std::optional<XRayEntryType> entry_type, const String & symbol, std::optional<std::vector<InstrumentationManager::InstrumentedParameter>> parameters)
    {
        column_id.push_back(id);
        column_function_id.insert(function_id);
        column_function_name.insert(function_name);
        column_handler_name.insert(handler_name);

        if (entry_type.has_value())
            column_entry_type.insert(entry_type.value());
        else
            column_entry_type.insert(ENTRY_AND_EXIT);

        column_symbol.insert(symbol);

        if (parameters.has_value() && !parameters->empty())
        {
            const auto & param = (*parameters)[0];

            Field field = Field();
            if (std::holds_alternative<std::string>(param))
                field = Field(std::get<std::string>(param));
            else if (std::holds_alternative<Int64>(param))
                field = Field(std::get<Int64>(param));
            else if (std::holds_alternative<Float64>(param))
                field = Field(std::get<Float64>(param));

            column_parameters.insert(field);
        }
        else
            column_parameters.insert(Field());
    };

    for (const auto & ip : instrumented_points)
        add_row(ip.id, ip.function_id, ip.function_name, ip.handler_name, ip.entry_type, ip.symbol, ip.parameters);
}

}
#endif
