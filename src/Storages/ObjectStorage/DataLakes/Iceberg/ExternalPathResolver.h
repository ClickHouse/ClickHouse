#pragma once

#include "config.h"

#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Interpreters/Context_fwd.h>

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

namespace DB
{

struct ObjectInfo;
using ObjectInfoPtr = std::shared_ptr<ObjectInfo>;

struct SchemeAuthorityKey
{
    explicit SchemeAuthorityKey(const std::string & uri);

    std::string scheme;
    std::string authority;
    std::string key;
};

#if USE_AVRO

namespace Iceberg { class IcebergPathResolver; }

struct ExternalStorageCache
{
    mutable std::mutex mutex;
    std::map<std::string, ObjectStoragePtr> storages;
};

/// Local directories below a table directory already found not to be symlinks by the checks of one query,
/// so that each file of the query checks only its own path components that are not shared with earlier files.
struct VerifiedLocalDirectories
{
    std::mutex mutex;
    UnorderedSetWithMemoryTracking<std::string> paths;
};

/// Checks grants without opening storage. Unreadable paths remain visible in `system.iceberg_files`.
bool isPathReadGranted(
    const std::string & table_location,
    const std::string & path,
    const ObjectStoragePtr & base_storage,
    const ContextPtr & context,
    const Iceberg::IcebergPathResolver & path_resolver,
    VerifiedLocalDirectories * verified_directories = nullptr);

/// Checks the `WRITE` grant the same way, for deleting a file referenced by the table.
bool isPathWriteGranted(
    const std::string & table_location,
    const std::string & path,
    const ObjectStoragePtr & base_storage,
    const ContextPtr & context,
    const Iceberg::IcebergPathResolver & path_resolver);

/// Resolves the storage of `path` as a scan does, so a metadata-only answer fails where the scan would:
/// on a missing grant, a rejected path, or the credential restrictions of a new `S3` client.
void checkPathReadable(
    const std::string & table_location,
    const std::string & path,
    const ObjectStoragePtr & base_storage,
    ExternalStorageCache & external_storages,
    const ContextPtr & context,
    const Iceberg::IcebergPathResolver & path_resolver,
    VerifiedLocalDirectories * verified_directories = nullptr);

/// Sets `in_table_directory` if the resolution established whether the key is inside the table directory.
std::pair<ObjectStoragePtr, std::string> resolveObjectStorageForPath(
    const std::string & table_location,
    const std::string & path,
    const ObjectStoragePtr & base_storage,
    ExternalStorageCache & external_storages,
    const ContextPtr & context,
    const Iceberg::IcebergPathResolver & path_resolver,
    std::optional<bool> * in_table_directory = nullptr,
    VerifiedLocalDirectories * verified_directories = nullptr);

/// Resolves the storage of a new file at `path` as a scan resolves it, but checks the `WRITE` grant instead of `READ`.
std::pair<ObjectStoragePtr, std::string> resolveObjectStorageForWrite(
    const std::string & table_location,
    const std::string & path,
    const ObjectStoragePtr & base_storage,
    ExternalStorageCache & external_storages,
    const ContextPtr & context,
    const Iceberg::IcebergPathResolver & path_resolver);

bool isObjectInTableDirectory(
    const ObjectStoragePtr & storage,
    const std::string & key,
    const ObjectStoragePtr & table_storage,
    const std::string & table_path,
    VerifiedLocalDirectories * verified_directories = nullptr);

void resolveObjectStorageFromDataLakeMetadata(
    const ObjectInfoPtr & object,
    const std::string & table_location,
    const ObjectStoragePtr & base_storage,
    ExternalStorageCache & external_storages,
    const ContextPtr & context);

#endif

}
