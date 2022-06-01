#include "IFileCache.h"

#include <Common/hex.h>
#include <Common/CurrentThread.h>
#include <Common/SipHash.h>
#include <Common/FileCacheSettings.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int REMOTE_FS_OBJECT_CACHE_ERROR;
}

IFileCache::IFileCache(
    const String & cache_base_path_,
    const FileCacheSettings & cache_settings_)
    : cache_base_path(cache_base_path_)
    , max_size(cache_settings_.max_size)
    , max_element_size(cache_settings_.max_elements)
    , max_file_segment_size(cache_settings_.max_file_segment_size)
{
}

String IFileCache::Key::toString() const
{
    return getHexUIntLowercase(key);
}

IFileCache::Key IFileCache::hash(const String & path)
{
    return Key(sipHash128(path.data(), path.size()));
}

String IFileCache::getPathInLocalCache(const Key & key, size_t offset, bool is_persistent) const
{
    auto key_str = key.toString();
    return fs::path(cache_base_path)
        / key_str.substr(0, 3)
        / key_str
        / (std::to_string(offset) + (is_persistent ? "_persistent" : ""));
}

String IFileCache::getPathInLocalCache(const Key & key) const
{
    auto key_str = key.toString();
    return fs::path(cache_base_path) / key_str.substr(0, 3) / key_str;
}

bool IFileCache::isReadOnly()
{
    return !CurrentThread::isInitialized()
        || !CurrentThread::get().getQueryContext()
        || CurrentThread::getQueryId().size == 0;
}

void IFileCache::assertInitialized() const
{
    if (!is_initialized)
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Cache not initialized");
}

}
