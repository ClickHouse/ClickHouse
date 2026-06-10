#pragma once

#include "config.h"

#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>

#include <memory>

namespace DataLake
{
class ICatalog;
}

namespace DB
{

struct DataLakeStorageSettings;
struct StorageID;

/// Out-of-line helpers for `DataLakeConfiguration<Base, Meta>` template members that need
/// the full `Interpreters/Context` definition. Keeping these as non-template free
/// functions lets the template header avoid `Interpreters/Context.h`, which the style
/// check rejects as too broad in public headers.

/// Look up the `DataLake::ICatalog` for `table_id` by resolving the database name
/// against `DatabaseCatalog` and, if the database is a `DatabaseDataLake`, returning
/// its catalog. Returns nullptr when the database is not a data-lake database.
///
/// `storage_catalog_type` and `storage_aws_access_key_id` on `settings` are checked for
/// the deprecated `engine = DataLakeCatalog` path and the call throws `BAD_ARGUMENTS`
/// when either has been changed by the user.
std::shared_ptr<DataLake::ICatalog> tryGetDataLakeCatalog(
    const DataLakeStorageSettings & settings,
    ContextPtr context,
    const StorageID & table_id);

/// Verify the disk is on the allow-list `allowed_disks_for_table_engines` and return its
/// `ObjectStorage`. Throws `BAD_ARGUMENTS` if not allowed.
ObjectStoragePtr resolveAllowedDiskObjectStorage(
    ContextPtr context,
    const String & disk_name);

/// For data-lake configurations pointing at a `Local` object storage, verify that the
/// read path is inside `user_files_path`. Throws `PATH_ACCESS_DENIED` otherwise.
void assertLocalDataLakePathInUserFiles(
    const ObjectStoragePtr & object_storage,
    ContextPtr context,
    const String & read_path);

}
