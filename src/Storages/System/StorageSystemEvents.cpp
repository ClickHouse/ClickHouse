#include <Common/ProfileEvents.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemEvents.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool system_events_show_zero_values;
}

ColumnsDescription StorageSystemEvents::getColumnsDescription()
{
    auto description = ColumnsDescription
    {
        {"event", std::make_shared<DataTypeString>(), "Event name."},
        {"value", std::make_shared<DataTypeUInt64>(), "Number of events occurred."},
        {"description", std::make_shared<DataTypeString>(), "Event description."},
    };

    description.setAliases({
        {"name", std::make_shared<DataTypeString>(), "event"}
    });

    return description;
}

void StorageSystemEvents::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
    {
        UInt64 value = ProfileEvents::global_counters[i];

        if (0 != value || context->getSettingsRef()[Setting::system_events_show_zero_values])
        {
            res_columns[0]->insert(ProfileEvents::getName(ProfileEvents::Event(i)));
            res_columns[1]->insert(value);
            res_columns[2]->insert(ProfileEvents::getDocumentation(ProfileEvents::Event(i)));
        }
    }
}

}
