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

/// Same as `listFiles`, but keeps the per-object metadata the listing already reported (size and
/// modification time), so a caller can tell a file apart from a later rewrite of itself without
/// paying for an extra request.
RelativePathsWithMetadata listFilesWithMetadata(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix, const String & suffix);

String resolvePathInsideTable(const String & table_path, const String & relative_path);
}
