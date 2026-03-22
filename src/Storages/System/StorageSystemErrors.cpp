#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
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


void StorageSystemErrors::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto add_row = [&](std::string_view name, size_t code, const auto & error, bool remote)
    {
        if (error.count || context->getSettingsRef()[Setting::system_events_show_zero_values])
        {
            size_t col_num = 0;
            res_columns[col_num++]->insert(name);
            res_columns[col_num++]->insert(code);
            res_columns[col_num++]->insert(error.count);
            res_columns[col_num++]->insert(error.error_time_ms / 1000);
            res_columns[col_num++]->insert(error.message);
            res_columns[col_num++]->insert(error.format_string);
            {
                Array trace_array;
                trace_array.reserve(error.trace.size());
                for (size_t i = 0; i < error.trace.size(); ++i)
                    trace_array.emplace_back(reinterpret_cast<intptr_t>(error.trace[i]));

                res_columns[col_num++]->insert(trace_array);
            }
            res_columns[col_num++]->insert(remote);
            res_columns[col_num++]->insert(error.query_id);
#if defined(__ELF__) && !defined(OS_FREEBSD)
            if (!error.trace.empty())
            {
                auto [symbols, lines] = symbolizeTrace(error.trace.data(), error.trace.size());
                res_columns[col_num++]->insert(Array(symbols.begin(), symbols.end()));
                res_columns[col_num++]->insert(Array(lines.begin(), lines.end()));
            }
            else
#endif
            {
                res_columns[col_num++]->insertDefault();
                res_columns[col_num++]->insertDefault();
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
