#pragma once

#include <IO/ICacheProvider.h>
#include <IO/ReadSettings.h>
#include <Interpreters/FileCache/FileCache.h>

#include <Common/logger_useful.h>

namespace DB
{

/// ICacheHandle for FileCache (filesystem/disk cache).
/// Holds a FileSegmentsHolder — segments stay pinned until the handle is destroyed.
class DiskCacheHandle : public ICacheHandle
{
public:
    DiskCacheHandle(
        FileCachePtr cache,
        FileCacheKey cache_key,
        Range requested,
        size_t file_size,
        const FilesystemCacheSettings & cache_settings);

    CacheLookupResult status() const override;
    Rope get(Range range) override;
    bool put(Range range, Rope data) override;

private:
    FileCachePtr cache;
    FileCacheKey cache_key;
    size_t file_size;
    FilesystemCacheSettings cache_settings;
    FileSegmentsHolderPtr holder;
    LoggerPtr log = getLogger("DiskCacheHandle");
};


/// ICacheProvider wrapping FileCache.
class DiskCacheProvider : public ICacheProvider
{
public:
    DiskCacheProvider(
        FileCachePtr cache_,
        size_t file_size_,
        const FilesystemCacheSettings & cache_settings_)
        : cache(std::move(cache_))
        , file_size(file_size_)
        , cache_settings(cache_settings_)
    {
    }

    std::unique_ptr<ICacheHandle> lookup(CacheKey key, Range range) override;
    String name() const override { return "DiskCache"; }

private:
    FileCachePtr cache;
    size_t file_size;
    FilesystemCacheSettings cache_settings;
};

}
