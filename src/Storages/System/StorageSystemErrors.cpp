#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemErrors.h>
#include <Interpreters/Context.h>
#include <Common/SymbolsHelper.h>
#include <Common/ErrorCodes.h>
#include <Core/Settings.h>

namespace DB
{
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
        { "last_error_trace",         std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "A stack trace that represents a list of physical addresses where the called methods are stored."},
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
                    trace_array.emplace_back(reinterpret_cast<intptr_t>(error.trace[i]));

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
#if (defined(__ELF__) && !defined(OS_FREEBSD)) || defined(OS_DARWIN)
                if (!error.trace.empty())
                {
                    auto [symbols, lines] = symbolizeTrace(error.trace.data(), error.trace.size(), need_symbols, need_lines);
                    if (need_symbols)
                        res_columns[res_index++]->insert(Array(symbols.begin(), symbols.end()));
                    if (need_lines)
                        res_columns[res_index++]->insert(Array(lines.begin(), lines.end()));
                }
                else
#endif
                {
                    if (need_symbols)
                        res_columns[res_index++]->insertDefault();
                    if (need_lines)
                        res_columns[res_index++]->insertDefault();
                }
            }
        }
    };

    for (size_t i = 0, end = ErrorCodes::end(); i < end; ++i)
    {
        const auto & error = ErrorCodes::values[i].get();
        std::string_view name = ErrorCodes::getName(static_cast<ErrorCodes::ErrorCode>(i));

        if (name.empty())
            continue;

        add_row(name, i, error.local,  /* remote= */ false);
        add_row(name, i, error.remote, /* remote= */ true);
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemErrors) }
