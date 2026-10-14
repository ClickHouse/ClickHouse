#include <Storages/System/StorageSystemErrors.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/StackTrace.h>
#include <Common/SymbolsHelper.h>
#include <Common/logger_useful.h>

#include <mutex>
#include <optional>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PARSE_DWARF;
}

namespace Setting
{
    extern const SettingsBool system_events_show_zero_values;
}

ColumnsDescription StorageSystemErrors::getColumnsDescription()
{
    DataTypePtr symbolized_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()));

    return ColumnsDescription
    {
        { "name",                     std::make_shared<DataTypeString>(), "Name of the error (errorCodeToName)."},
        { "code",                     std::make_shared<DataTypeInt32>(), "Code number of the error."},
        { "value",                    std::make_shared<DataTypeUInt64>(), "The number of times this error happened."},
        { "last_error_time",          std::make_shared<DataTypeDateTime>(), "The time when the last error happened."},
        { "last_error_message",       std::make_shared<DataTypeString>(), "Message for the last error."},
        { "last_error_format_string", std::make_shared<DataTypeString>(), "Format string for the last error."},
        { "last_error_trace",         std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "A stack trace of the last error. On ELF platforms except FreeBSD, addresses inside the main ClickHouse binary are stored as physical file offsets, and other addresses are virtual memory addresses inside the ClickHouse server process."},
        { "remote",                   std::make_shared<DataTypeUInt8>(), "Remote exception (i.e. received during one of the distributed queries)."},
        { "query_id",                 std::make_shared<DataTypeString>(), "Id of a query that caused an error (if available)." },
        { "last_error_symbols", symbolized_type, "Demangled symbol names corresponding to last_error_trace." },
        { "last_error_lines",   symbolized_type, "File names with line numbers corresponding to last_error_trace." },
    };
}

void StorageSystemErrors::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8> columns_mask) const
{
    auto add_row = [&](std::string_view name, size_t code, const auto & error, bool remote)
    {
        if (error.count || context->getSettingsRef()[Setting::system_events_show_zero_values])
        {
            size_t src_index = 0;
            size_t res_index = 0;

            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(name);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(code);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(error.count);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(error.error_time_ms / 1000);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(error.message);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(error.format_string);
            if (columns_mask[src_index++])
            {
                Array trace_array;
                trace_array.reserve(error.trace.size());
                for (size_t i = 0; i < error.trace.size(); ++i)
                    trace_array.emplace_back(StackTrace::resolveAddressForStorage(error.trace[i]));

                res_columns[res_index++]->insert(trace_array);
            }
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(remote);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(error.query_id);

            /// `last_error_symbols` and `last_error_lines` require expensive symbolization
            /// (DWARF lookups), so resolve them only when at least one of the columns is requested.
            const bool need_symbols = columns_mask[src_index++];
            const bool need_lines = columns_mask[src_index++];
            if (need_symbols || need_lines)
            {
                IColumn * symbols_column = need_symbols ? res_columns[res_index++].get() : nullptr;
                IColumn * lines_column = need_lines ? res_columns[res_index++].get() : nullptr;
                const size_t symbols_old_size = symbols_column ? symbols_column->size() : 0;
                const size_t lines_old_size = lines_column ? lines_column->size() : 0;

#if (defined(__ELF__) && !defined(OS_FREEBSD)) || defined(OS_DARWIN)
                /// These two columns are diagnostic sugar: a failure to parse DWARF debug info must not
                /// make the whole `system.errors` table unreadable, so it is reported to the server log
                /// and the columns are left empty. Any other exception propagates.
                std::optional<std::pair<std::vector<String>, std::vector<String>>> symbolized;
                if (!error.trace.empty())
                {
                    try
                    {
                        symbolized = symbolizeTrace(error.trace.data(), error.trace.size(), need_symbols, need_lines);
                    }
                    catch (const Exception & e)
                    {
                        if (e.code() != ErrorCodes::CANNOT_PARSE_DWARF)
                            throw;

                        /// Symbolization fails for the whole binary rather than for a single address,
                        /// so report it only once instead of flooding the log on every query.
                        static std::once_flag reported;
                        std::call_once(reported, []
                        {
                            tryLogCurrentException(
                                getLogger("StorageSystemErrors"),
                                "Cannot symbolize the stack trace for system.errors, "
                                "last_error_symbols and/or last_error_lines will be empty");
                        });
                    }
                }

                /// Insert outside of the `try`: `ColumnArray::insert` appends nested elements before the offset,
                /// so an exception (e.g. `MEMORY_LIMIT_EXCEEDED`) thrown here must propagate, not leave a partial row.
                if (symbolized)
                {
                    if (symbols_column)
                        symbols_column->insert(Array(symbolized->first.begin(), symbolized->first.end()));
                    if (lines_column)
                        lines_column->insert(Array(symbolized->second.begin(), symbolized->second.end()));
                }
#endif
                /// Fill whatever was not inserted above (no trace, unsupported platform, or a DWARF failure),
                /// keeping all columns of the row the same size.
                if (symbols_column && symbols_column->size() == symbols_old_size)
                    symbols_column->insertDefault();
                if (lines_column && lines_column->size() == lines_old_size)
                    lines_column->insertDefault();
            }
        }
    };

    for (const auto code : ErrorCodes::getCodes())
    {
        std::string_view name = ErrorCodes::getName(code);

        /// Custom error codes have no name, and are not shown here.
        if (name.empty())
            continue;

        const auto & error = ErrorCodes::values[code].get();
        add_row(name, code, error.local, /* remote= */ false);
        add_row(name, code, error.remote, /* remote= */ true);
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemErrors) }
