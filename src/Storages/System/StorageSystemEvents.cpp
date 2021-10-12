#include <Common/ProfileEvents.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemEvents.h>

namespace DB
{

NamesAndTypesList StorageSystemEvents::getNamesAndTypes()
{
    return {
        {"event", std::make_shared<DataTypeString>()},
        {"value", std::make_shared<DataTypeUInt64>()},
        {"description", std::make_shared<DataTypeString>()},
    };
}

void StorageSystemEvents::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
    {
        UInt64 value = ProfileEvents::global_counters[i];

        if (0 != value || context.getSettingsRef().system_events_show_zero_values)
        {
            res_columns[0]->insert(ProfileEvents::getName(ProfileEvents::Event(i)));
            res_columns[1]->insert(value);
            res_columns[2]->insert(ProfileEvents::getDocumentation(ProfileEvents::Event(i)));
        }
    }
}

}
