#pragma once
#include <functional>
#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace DB
{

class IObjectStorage;
std::vector<String> listFiles(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix, const String & suffix);

std::vector<String> listFiles(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const std::function<bool(const RelativePathWithMetadata &)> & check_need);

/// True if a `_delta_log/` with any entry (not just `*.json`) exists at `path`, so a checkpoint-only log still counts as an existing table.
bool deltaLogExists(const IObjectStorage & object_storage, const String & path);

String resolvePathInsideTable(const String & table_path, const String & relative_path);
}
