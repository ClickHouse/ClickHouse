#pragma once
#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace DB
{

class IObjectStorage;

std::vector<String>
listFiles(const IObjectStorage & object_storage, const std::string & path, const String & prefix, const String & suffix);
}
