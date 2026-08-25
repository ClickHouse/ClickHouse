#include <Storages/System/StorageSystemInstrumentation.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>

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
        {"arguments", std::make_shared<DataTypeArray>(std::make_shared<DataTypeDynamic>()), "Arguments for the handler call."},
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
    auto & column_arguments = assert_cast<ColumnArray &>(*res_columns[column_index++]);

    for (const auto & ip : instrumented_points)
    {
        column_id.push_back(static_cast<UInt32>(ip.id));
        column_function_id.push_back(ip.function_id);
        column_function_name.insert(ip.function_name);
        column_handler_name.insert(ip.handler_name);
        column_entry_type.insert(ip.entry_type);
        column_symbol.insert(ip.symbol);

        Array array;
        for (const auto & arg : ip.arguments)
        {
            Field field = Field();
            if (std::holds_alternative<std::string>(arg))
                field = Field(std::get<std::string>(arg));
            else if (std::holds_alternative<Int64>(arg))
                field = Field(std::get<Int64>(arg));
            else if (std::holds_alternative<Float64>(arg))
                field = Field(std::get<Float64>(arg));

            array.emplace_back(field);
        }
        column_arguments.insert(array);
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemInstrumentation) }

#endif

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "instrumentation",
    .description = R"DOCS_MD(
Contains the instrumentation points using LLVM's XRay feature.
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql
SELECT * FROM system.instrumentation FORMAT Vertical;
```

```text
Row 1:
──────
id:            0
function_id:   231280
function_name: QueryMetricLog::startQuery
handler:       log
entry_type:    Entry
symbol:        DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)
arguments:     ['test']

Row 2:
──────
id:            1
function_id:   231280
function_name: QueryMetricLog::startQuery
handler:       profile
entry_type:    EntryAndExit
symbol:        DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)
arguments:     []

Row 3:
──────
id:            2
function_id:   231280
function_name: QueryMetricLog::startQuery
handler:       sleep
entry_type:    Exit
symbol:        DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)
arguments:     [0.3]

3 rows in set. Elapsed: 0.302 sec.
```
)DOCS_MD",
    .see_also = R"DOCS_MD(
- [SYSTEM INSTRUMENT](/reference/statements/system#instrument) — Add or remove instrumentation points.
- [system.trace_log](/reference/system-tables/trace_log) — Inspect profiling log.
- [system.symbols](/reference/system-tables/symbols) — Inspect symbols to add instrumentation points.
)DOCS_MD")

}
