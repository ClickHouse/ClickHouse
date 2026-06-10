#include <Storages/ObjectStorage/DataLakes/DataLakeConfigurationHelpers.h>

#include "config.h"

#include <Common/Exception.h>
#include <Common/filesystemHelpers.h>
#include <Databases/IDatabase.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/StorageID.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>

#if USE_AVRO && USE_PARQUET
#include <Databases/DataLake/DatabaseDataLake.h>
#include <Databases/DataLake/ICatalog.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int PATH_ACCESS_DENIED;
}

namespace DataLakeStorageSetting
{
    extern const DataLakeStorageSettingsDatabaseDataLakeCatalogType storage_catalog_type;
    extern const DataLakeStorageSettingsString storage_aws_access_key_id;
}

std::shared_ptr<DataLake::ICatalog> tryGetDataLakeCatalog(
    [[maybe_unused]] const DataLakeStorageSettings & settings,
    [[maybe_unused]] ContextPtr context,
    [[maybe_unused]] const StorageID & table_id)
{
#if USE_AVRO && USE_PARQUET
    if (settings[DataLakeStorageSetting::storage_catalog_type].changed
        || settings[DataLakeStorageSetting::storage_aws_access_key_id].changed)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Don't use deprecated settings storage_catalog_type and storage_catalog_url");
    }
    const String db_name = table_id.hasDatabase() ? table_id.database_name : context->getCurrentDatabase();
    DatabasePtr database = DatabaseCatalog::instance().tryGetDatabase(db_name);
    if (!database)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Database {} not found", db_name);
    auto datalake_database = std::dynamic_pointer_cast<DatabaseDataLake>(database);
    if (!datalake_database)
        return nullptr;
    return datalake_database->getCatalog();
#else
    return nullptr;
#endif
}

ObjectStoragePtr resolveAllowedDiskObjectStorage(ContextPtr context, const String & disk_name)
{
    if (!Context::getGlobalContextInstance()->getAllowedDisksForTableEngines().contains(disk_name))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Disk {} is not allowed for usage in storage engines. "
            "The list of allowed disks is defined by `allowed_disks_for_table_engines`",
            disk_name);
    }

    auto disk = context->getDisk(disk_name);
    return disk->getObjectStorage();
}

void assertLocalDataLakePathInUserFiles(
    const ObjectStoragePtr & object_storage, ContextPtr context, const String & read_path)
{
    if (object_storage->getType() != ObjectStorageType::Local)
        return;

    auto user_files_path = context->getUserFilesPath();
    if (!fileOrSymlinkPathStartsWith(read_path, user_files_path))
    {
        throw Exception(
            ErrorCodes::PATH_ACCESS_DENIED,
            "File path {} is not inside {}",
            read_path,
            user_files_path);
    }
}

}
