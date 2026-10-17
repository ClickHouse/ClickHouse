#pragma once
#include <functional>
#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace DB
{

class IObjectStorage;

/// Objects at any depth inside the directory `path`/`prefix` (all of `path` for an empty `prefix`).
std::vector<String> listFiles(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix, const String & suffix);

std::vector<String> listFiles(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const std::function<bool(const RelativePathWithMetadata &)> & check_need);

/// Only the objects directly inside the directory `path`/`prefix`.
std::vector<String> listFilesNonRecursive(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix, const String & suffix);

std::vector<String> listFilesNonRecursive(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const std::function<bool(const RelativePathWithMetadata &)> & check_need);

/// True if a `_delta_log/` with any entry (not just `*.json`) exists at `path`, so a checkpoint-only log still counts as an existing table.
bool deltaLogExists(const IObjectStorage & object_storage, const String & path);

String resolvePathInsideTable(const String & table_path, const String & relative_path);
}
