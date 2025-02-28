#pragma once

#include "ObjectStorageKey.h"
#include <memory>

namespace DB
{

class IObjectStorageKeysGenerator
{
public:
    virtual ObjectStorageKey generate(const String & path) const = 0;
    virtual ~IObjectStorageKeysGenerator() = default;
};

using ObjectStorageKeysGeneratorPtr = std::shared_ptr<IObjectStorageKeysGenerator>;

ObjectStorageKeysGeneratorPtr createObjectStorageKeysGeneratorAsIsWithPrefix(String key_prefix);
ObjectStorageKeysGeneratorPtr createObjectStorageKeysGeneratorByPrefix(String key_prefix);
ObjectStorageKeysGeneratorPtr createObjectStorageKeysGeneratorByTemplate(String key_template);

}
