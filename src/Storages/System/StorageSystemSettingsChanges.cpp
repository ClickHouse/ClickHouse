#include <Storages/System/StorageSystemSettingsChanges.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/Context.h>
#include <Core/SettingsChangesHistory.h>

namespace DB
{
NamesAndTypesList StorageSystemSettingsChanges::getNamesAndTypes()
{
    return {
        {"version", std::make_shared<DataTypeString>()},
        {"changes",
         std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
             DataTypes{
                 std::make_shared<DataTypeString>(),
                 std::make_shared<DataTypeString>(),
                 std::make_shared<DataTypeString>(),
                 std::make_shared<DataTypeString>()},
             Names{"name", "previous_value", "new_value", "reason"}))},
    };
}

void StorageSystemSettingsChanges::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    for (auto it = settings_changes_history.rbegin(); it != settings_changes_history.rend(); ++it)
    {
        res_columns[0]->insert(it->first.toString());
        Array changes;
        for (const auto & change : it->second)
            changes.push_back(Tuple{change.name, toString(change.previous_value), toString(change.new_value), change.reason});
        res_columns[1]->insert(changes);
    }
}

}
