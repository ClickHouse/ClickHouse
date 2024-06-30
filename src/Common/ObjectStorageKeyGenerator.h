#pragma once

#include <memory>
#include "ObjectStorageKey.h"

namespace DB
{

class IObjectStorageKeysGenerator
{
public:
    virtual ~IObjectStorageKeysGenerator() = default;

    virtual ObjectStorageKey generate(const String & path, bool is_directory) const = 0;
};

using ObjectStorageKeysGeneratorPtr = std::shared_ptr<IObjectStorageKeysGenerator>;

ObjectStorageKeysGeneratorPtr createObjectStorageKeysGeneratorAsIsWithPrefix(String key_prefix);
ObjectStorageKeysGeneratorPtr createObjectStorageKeysGeneratorByPrefix(String key_prefix);
ObjectStorageKeysGeneratorPtr createObjectStorageKeysGeneratorByTemplate(String key_template);

}
