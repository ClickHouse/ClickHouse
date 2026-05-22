#pragma once

#include <IO/ICacheProvider.h>
#include <IO/ReadSettings.h>
#include <Interpreters/FileCache/FileCache.h>

#include <Common/logger_useful.h>

namespace DB
{

class FilesystemCacheLog;

/// ICacheHandle for FileCache (filesystem/disk cache).
/// Holds a FileSegmentsHolder — segments stay pinned until the handle is destroyed.
class DiskCacheHandle : public ICacheHandle
{
public:
    DiskCacheHandle(
        FileCachePtr cache,
        FileCacheKey cache_key,
        ByteRange requested,
        size_t file_size,
        const FilesystemCacheSettings & cache_settings,
        std::shared_ptr<FilesystemCacheLog> cache_log,
        String source_file_path);

    CacheLookupResult status() const override;
    Rope get(ByteRange range) override;
    bool put(ByteRange range, Rope data) override;

private:
    FileCachePtr cache;
    FileCacheKey cache_key;
    size_t file_size;
    FilesystemCacheSettings cache_settings;
    std::shared_ptr<FilesystemCacheLog> cache_log;
    String source_file_path;
    ByteRange requested_range;
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
        const FilesystemCacheSettings & cache_settings_,
        std::shared_ptr<FilesystemCacheLog> cache_log_ = nullptr)
        : cache(std::move(cache_))
        , file_size(file_size_)
        , cache_settings(cache_settings_)
        , cache_log(std::move(cache_log_))
    {
    }

    std::unique_ptr<ICacheHandle> lookup(CacheKey key, ByteRange range) override;
    String name() const override { return "DiskCache"; }

private:
    FileCachePtr cache;
    size_t file_size;
    FilesystemCacheSettings cache_settings;
    std::shared_ptr<FilesystemCacheLog> cache_log;
};

}
