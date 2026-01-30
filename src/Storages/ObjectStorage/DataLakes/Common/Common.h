#pragma once
#include <functional>
#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace DB
{

class IObjectStorage;
std::vector<RelativePathWithMetadataPtr> listFiles(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix, const String & suffix);

std::vector<RelativePathWithMetadataPtr> listFiles(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const std::function<bool(const RelativePathWithMetadata &)> & check_need);
}
