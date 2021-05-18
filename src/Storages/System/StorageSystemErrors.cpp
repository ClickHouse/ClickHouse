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
    };
}


void StorageSystemErrors::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    for (size_t i = 0, end = ErrorCodes::end(); i < end; ++i)
    {
        UInt64 value = ErrorCodes::values[i];
        std::string_view name = ErrorCodes::getName(i);

        if (name.empty())
            continue;

        if (value || context.getSettingsRef().system_events_show_zero_values)
        {
            size_t col_num = 0;
            res_columns[col_num++]->insert(name);
            res_columns[col_num++]->insert(i);
            res_columns[col_num++]->insert(value);
        }
    }
}

}
