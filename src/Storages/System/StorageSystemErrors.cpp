#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemErrors.h>
#include <Common/ErrorCodes.h>
#include <Interpreters/Context.h>

namespace DB
{

NamesAndTypesList StorageSystemErrors::getNamesAndTypes()
{
    return {
        { "name",              std::make_shared<DataTypeString>() },
        { "code",              std::make_shared<DataTypeInt32>() },
        { "value",             std::make_shared<DataTypeUInt64>() },
        { "remote",            std::make_shared<DataTypeUInt8>() },
    };
}


void StorageSystemErrors::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    auto add_row = [&](std::string_view name, size_t code, size_t value, bool remote)
    {
        if (value || context.getSettingsRef().system_events_show_zero_values)
        {
            size_t col_num = 0;
            res_columns[col_num++]->insert(name);
            res_columns[col_num++]->insert(code);
            res_columns[col_num++]->insert(value);
            res_columns[col_num++]->insert(remote);
        }
    };

    for (size_t i = 0, end = ErrorCodes::end(); i < end; ++i)
    {
        const auto & error = ErrorCodes::values[i].get();
        std::string_view name = ErrorCodes::getName(i);

        if (name.empty())
            continue;

        add_row(name, i, error.local,  0 /* remote=0 */);
        add_row(name, i, error.remote, 1 /* remote=1 */);
    }
}

}
