#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Storages/System/StorageSystemErrors.h>
#include <Common/ErrorCodes.h>
#include <Interpreters/Context.h>

namespace DB
{

NamesAndTypesList StorageSystemErrors::getNamesAndTypes()
{
    return {
        { "name",                    std::make_shared<DataTypeString>() },
        { "code",                    std::make_shared<DataTypeInt32>() },
        { "value",                   std::make_shared<DataTypeUInt64>() },
        { "last_error_time",         std::make_shared<DataTypeDateTime>() },
        { "last_error_message",      std::make_shared<DataTypeString>() },
        { "last_error_stacktrace",   std::make_shared<DataTypeString>() },
        { "remote",                  std::make_shared<DataTypeUInt8>() },
    };
}


void StorageSystemErrors::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    auto add_row = [&](std::string_view name, size_t code, size_t value, UInt64 error_time_ms, const std::string & message, const std::string & stacktrace, bool remote)
    {
        if (value || context.getSettingsRef().system_events_show_zero_values)
        {
            size_t col_num = 0;
            res_columns[col_num++]->insert(name);
            res_columns[col_num++]->insert(code);
            res_columns[col_num++]->insert(value);
            res_columns[col_num++]->insert(error_time_ms / 1000);
            res_columns[col_num++]->insert(message);
            res_columns[col_num++]->insert(stacktrace);
            res_columns[col_num++]->insert(remote);
        }
    };

    for (size_t i = 0, end = ErrorCodes::end(); i < end; ++i)
    {
        const auto & error = ErrorCodes::values[i].get();
        std::string_view name = ErrorCodes::getName(i);

        if (name.empty())
            continue;

        add_row(name, i, error.local,  error.error_time_ms, error.message, error.stacktrace, 0 /* remote=0 */);
        add_row(name, i, error.remote, error.error_time_ms, error.message, error.stacktrace, 1 /* remote=1 */);
    }
}

}
