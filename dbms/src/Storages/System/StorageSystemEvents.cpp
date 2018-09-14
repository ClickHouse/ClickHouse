#include <Common/ProfileEvents.h>
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

void StorageSystemEvents::fillData(MutableColumns & res_columns, const Context &, const SelectQueryInfo &) const
{
    for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
    {
        UInt64 value = ProfileEvents::global_counters[i];

        if (0 != value)
        {
            res_columns[0]->insert(String(ProfileEvents::getName(ProfileEvents::Event(i))));
            res_columns[1]->insert(value);
            res_columns[2]->insert(String(ProfileEvents::getDocumentation(ProfileEvents::Event(i))));
        }
    }
}

}
