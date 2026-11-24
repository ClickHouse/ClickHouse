#include <Storages/tryGetOwnerTable.h>

#include <Databases/IDatabase.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageTimeSeries.h>

namespace DB
{

namespace
{

bool ownsInnerTable(const IStorage & owner, const StorageID & table_id, const ContextPtr & context)
{
    if (const auto * view = owner.as<StorageMaterializedView>())
        return view->hasInnerTable() && table_id == view->getTargetTableId();

    if (const auto * time_series = owner.as<StorageTimeSeries>())
    {
        for (auto target_kind : StorageTimeSeries::getTargetKinds())
        {
            if (time_series->isInnerTable(target_kind) && table_id == time_series->tryGetTargetTableID(target_kind, context))
                return true;
        }
    }
    return false;
}

}

String tryGetOwnerTable(const DatabasePtr & database, const String & table_name, const ContextPtr & context)
{
    /// An external database (e.g. `Remote`, `MySQL`) cannot own inner tables, and resolving a candidate in it
    /// could fetch a remote schema the user is not allowed to see.
    if (database->isExternal())
        return {};

    std::string_view name = table_name;
    StoragePtr candidates[2];
    if (name.starts_with(".inner_id."))
    {
        /// `.inner_id.<uuid>` or `.inner_id.<kind>.<uuid>`
        UUID uuid;
        if (name.size() >= 36 && tryParse(uuid, name.substr(name.size() - 36)) && uuid != UUIDHelpers::Nil)
        {
            /// The owner must be in the same database: an inner table moved to another database by `RENAME` has no owner there.
            auto owner = DatabaseCatalog::instance().tryGetByUUID(uuid).second;
            if (owner && owner->getStorageID().database_name == database->getDatabaseName())
                candidates[0] = owner;
        }
    }
    else if (name.starts_with(".inner."))
    {
        /// `.inner.<name>` or `.inner.<kind>.<name>`
        name.remove_prefix(strlen(".inner."));
        candidates[0] = database->tryGetTable(String{name}, context);
        if (size_t pos = name.find('.'); pos != std::string_view::npos)
            candidates[1] = database->tryGetTable(String{name.substr(pos + 1)}, context);
    }

    const StorageID table_id{database->getDatabaseName(), table_name};
    for (const auto & candidate : candidates)
    {
        if (candidate && ownsInnerTable(*candidate, table_id, context))
            return candidate->getStorageID().table_name;
    }
    return {};
}

}
