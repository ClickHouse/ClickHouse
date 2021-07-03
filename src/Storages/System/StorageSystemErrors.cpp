#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeArray.h>
#include <Storages/System/StorageSystemErrors.h>
#include <Interpreters/Context.h>
#include <Common/ErrorCodes.h>


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
        { "last_error_trace",        std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()) },
        { "remote",                  std::make_shared<DataTypeUInt8>() },
    };
}


void StorageSystemErrors::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    auto add_row = [&](std::string_view name, size_t code, const auto & error, bool remote)
    {
        if (error.count || context->getSettingsRef().system_events_show_zero_values)
        {
            size_t col_num = 0;
            res_columns[col_num++]->insert(name);
            res_columns[col_num++]->insert(code);
            res_columns[col_num++]->insert(error.count);
            res_columns[col_num++]->insert(error.error_time_ms / 1000);
            res_columns[col_num++]->insert(error.message);
            {
                Array trace_array;
                trace_array.reserve(error.trace.size());
                for (size_t i = 0; i < error.trace.size(); ++i)
                    trace_array.emplace_back(reinterpret_cast<intptr_t>(error.trace[i]));

                res_columns[col_num++]->insert(trace_array);
            }
            res_columns[col_num++]->insert(remote);
        }
    };

    for (size_t i = 0, end = ErrorCodes::end(); i < end; ++i)
    {
        const auto & error = ErrorCodes::values[i].get();
        std::string_view name = ErrorCodes::getName(i);

        if (name.empty())
            continue;

        add_row(name, i, error.local,  /* remote= */ false);
        add_row(name, i, error.remote, /* remote= */ true);
    }
}

}
