#pragma once

#include "config.h"

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Interpreters/Context_fwd.h>

#include <map>
#include <memory>
#include <mutex>
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

/// Checks grants without opening storage. Unreadable paths remain visible in `system.iceberg_files`.
bool isPathReadGranted(
    const std::string & table_location,
    const std::string & path,
    const ObjectStoragePtr & base_storage,
    const ContextPtr & context);

/// Also checks path restrictions, for metadata-only answers that must not bypass a failing scan.
bool isPathReadable(
    const std::string & table_location,
    const std::string & path,
    const ObjectStoragePtr & base_storage,
    const ContextPtr & context);

std::pair<ObjectStoragePtr, std::string> resolveObjectStorageForPath(
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
    const std::string & table_path);

void resolveObjectStorageFromDataLakeMetadata(
    const ObjectInfoPtr & object,
    const std::string & table_location,
    const ObjectStoragePtr & base_storage,
    ExternalStorageCache & external_storages,
    const ContextPtr & context);

#endif

}
