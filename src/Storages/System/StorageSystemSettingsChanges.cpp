#include <Storages/System/StorageSystemSettingsChanges.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/Context.h>
#include <Core/SettingsChangesHistory.h>

namespace DB
{
ColumnsDescription StorageSystemSettingsChanges::getColumnsDescription()
{
    /// TODO: Fill in all the comments
    return ColumnsDescription
    {
        {"version", std::make_shared<DataTypeString>(), "The ClickHouse server version."},
        {"changes",
         std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
             DataTypes{
                 std::make_shared<DataTypeString>(),
                 std::make_shared<DataTypeString>(),
                 std::make_shared<DataTypeString>(),
                 std::make_shared<DataTypeString>()},
             Names{"name", "previous_value", "new_value", "reason"})), "The list of changes in settings which changed the behaviour of ClickHouse."},
    };
}

void StorageSystemSettingsChanges::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & settings_changes_history = getSettingsChangesHistory();
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
