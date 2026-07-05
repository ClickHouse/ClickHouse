#include <Common/ObjectStorageKey.h>

#include <Common/Exception.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

const String & ObjectStorageKey::getPrefix() const
{
    if (!is_relative)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "object key has no prefix, key: {}", key);

    return prefix;
}

const String & ObjectStorageKey::getSuffix() const
{
    if (!is_relative)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "object key has no suffix, key: {}", key);
    return suffix;
}

const String & ObjectStorageKey::serialize() const
{
    return key;
}

ObjectStorageKey ObjectStorageKey::createAsRelative(String key_)
{
    ObjectStorageKey object_key;
    object_key.suffix = std::move(key_);
    object_key.key = object_key.suffix;
    object_key.is_relative = true;
    return object_key;
}

ObjectStorageKey ObjectStorageKey::createAsRelative(String prefix_, String suffix_)
{
    ObjectStorageKey object_key;
    object_key.prefix = std::move(prefix_);
    object_key.suffix = std::move(suffix_);

    if (object_key.prefix.empty())
        object_key.key = object_key.suffix;
    else
        object_key.key = fs::path(object_key.prefix) / object_key.suffix;

    object_key.is_relative = true;
    return object_key;
}

ObjectStorageKey ObjectStorageKey::createAsAbsolute(String key_)
{
    ObjectStorageKey object_key;
    object_key.key = std::move(key_);
    object_key.is_relative = false;
    return object_key;
}

}
