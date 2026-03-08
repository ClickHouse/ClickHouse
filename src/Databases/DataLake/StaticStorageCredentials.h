#pragma once

#include <memory>

#include <Databases/DataLake/DatabaseDataLakeSettings.h>
#include <Databases/DataLake/DatabaseDataLakeStorageType.h>
#include <Databases/DataLake/StorageCredentials.h>

namespace DataLake
{

/// Returns static credentials from DatabaseDataLake settings for a storage type.
/// Returns nullptr when settings do not provide credentials for this storage.
std::shared_ptr<IStorageCredentials> tryGetStaticStorageCredentials(
    DB::DatabaseDataLakeStorageType storage_type,
    const DB::DatabaseDataLakeSettings & settings);

}
