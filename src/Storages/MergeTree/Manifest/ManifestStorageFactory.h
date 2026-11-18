#pragma once

#include <Storages/MergeTree/Manifest/IManifestStorage.h>

namespace DB
{

class ManifestStorageFactory
{
public:
    static ManifestStoragePtr create(const String & type_str, const String & dir);
    static bool isTypeSupported(const String & type_str);
    static std::vector<String> getSupportedTypes();
};

}
