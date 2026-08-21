#include <Storages/System/StorageSystemFilesystemCacheSettings.h>

#include <Columns/IColumn.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Storages/System/MutableColumnsAndConstraints.h>
#include <Access/SettingsConstraintsAndProfileIDs.h>
#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>
#include <Disks/IDisk.h>


namespace DB
{

ColumnsDescription StorageSystemFilesystemCacheSettings::getColumnsDescription()
{
    return FileCacheSettings::getColumnsDescription();
}

StorageSystemFilesystemCacheSettings::StorageSystemFilesystemCacheSettings(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_, getColumnsDescription())
{
}

void StorageSystemFilesystemCacheSettings::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    context->checkAccess(AccessType::SHOW_FILESYSTEM_CACHES);

    auto caches = FileCacheFactory::instance().getAll();
    auto constraints_and_current_profiles = context->getSettingsConstraintsAndCurrentProfiles();
    const auto & constraints = constraints_and_current_profiles->constraints;

    for (const auto & [cache_name, cache_data] : caches)
    {
        const auto & settings = cache_data->getSettings();
        MutableColumnsAndConstraints params(res_columns, constraints);
        settings.dumpToSystemSettingsColumns(params, cache_name, cache_data->cache);
    }
}

}
