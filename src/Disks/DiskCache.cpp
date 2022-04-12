#include "DiskCache.h"

#include <Disks/DiskFactory.h>
#include <Common/FileCache.h>
#include <Common/FileCacheFactory.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFileDecorator.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/FileCache.h>
#include <filesystem>

namespace fs = std::filesystem;


namespace ProfileEvents
{
    extern const Event RemoteFSCacheDownloadBytes;
}

namespace DB
{

class CachedWriteBuffer final : public WriteBufferFromFileDecorator
{
public:
    CachedWriteBuffer(
        std::unique_ptr<WriteBuffer> impl_,
        FileCachePtr cache_,
        const String & path_)
        : WriteBufferFromFileDecorator(std::move(impl_))
        , cache(cache_)
        , key(cache_->hash(path_))
    {
    }

    void nextImpl() override
    {
        size_t size = offset();

        swap(*impl);

        impl->next();

        swap(*impl);

        size_t remaining_size = size;

        auto file_segments_holder = cache->setDownloading(key, current_download_offset, size);
        auto & file_segments = file_segments_holder.file_segments;

        for (auto file_segment_it = file_segments.begin(); file_segment_it != file_segments.end(); ++file_segment_it)
        {
            auto & file_segment = *file_segment_it;
            size_t current_size = std::min(file_segment->range().size(), remaining_size);
            remaining_size -= current_size;

            if (file_segment->reserve(current_size))
            {
                file_segment->write(working_buffer.begin(), current_size, current_download_offset, true);
                ProfileEvents::increment(ProfileEvents::RemoteFSCacheDownloadBytes, current_size);
            }
            else
            {
                file_segments.erase(file_segment_it, file_segments.end());
                break;
            }
        }

        current_download_offset += size;
    }

private:
    FileCachePtr cache;
    IFileCache::Key key;

    size_t current_download_offset = 0;
};

DiskCache::DiskCache(
    const String & disk_name_,
    const String & path_,
    std::shared_ptr<IDisk> delegate_,
    FileCachePtr cache_)
    : DiskDecorator(delegate_)
    , cache_disk_name(disk_name_)
    , cache_base_path(path_)
    , cache(cache_)
{
}

std::unique_ptr<ReadBufferFromFileBase> DiskCache::readFile(
    const String & path, const ReadSettings & settings, std::optional<size_t> read_hint, std::optional<size_t> file_size) const
{
    ReadSettings read_settings{settings};
    read_settings.remote_fs_cache = cache;

    if (IFileCache::isReadOnly())
        read_settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache = true;

    return DiskDecorator::readFile(path, read_settings, read_hint, file_size);
}

std::unique_ptr<WriteBufferFromFileBase> DiskCache::writeFile(
    const String & path, size_t buf_size, WriteMode mode, const WriteSettings & settings)
{
    auto impl = DiskDecorator::writeFile(path, buf_size, mode, settings);

    bool cache_on_write = fs::path(path).extension() != ".tmp"
        && settings.enable_filesystem_cache_on_write_operations;

    if (cache_on_write)
        return std::make_unique<CachedWriteBuffer>(std::move(impl), cache, impl->getFileName());

    return impl;
}

void DiskCache::removeCache(const String & path)
{
    auto remote_paths = getRemotePaths(path);
    for (const auto & remote_path : remote_paths)
    {
        auto key = cache->hash(remote_path);
        cache->remove(key);
    }
}

void DiskCache::removeCacheRecursive(const String & path)
{
    std::vector<LocalPathWithRemotePaths> remote_paths_per_local_path;
    getRemotePathsRecursive(path, remote_paths_per_local_path);

    for (const auto & [_, remote_paths] : remote_paths_per_local_path)
    {
        for (const auto & remote_path : remote_paths)
        {
            auto key = cache->hash(remote_path);
            cache->remove(key);
        }
    }
}

bool DiskCache::removeFile(const String & path)
{
    removeCache(path);
    return DiskDecorator::removeFile(path);
}

bool DiskCache::removeFileIfExists(const String & path)
{
    removeCache(path);
    return DiskDecorator::removeFileIfExists(path);
}

void DiskCache::removeDirectory(const String & path)
{
    removeCacheRecursive(path);
    DiskDecorator::removeDirectory(path);
}

void DiskCache::removeRecursive(const String & path)
{
    removeCacheRecursive(path);
    DiskDecorator::removeRecursive(path);
}

ReservationPtr DiskCache::reserve(UInt64 bytes)
{
    auto ptr = DiskDecorator::reserve(bytes);
    if (ptr)
    {
        auto disk_ptr = std::static_pointer_cast<DiskCache>(shared_from_this());
        return std::make_unique<ReservationDelegate>(std::move(ptr), disk_ptr);
    }
    return ptr;
}

void registerDiskCache(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr context,
                      const DisksMap & map) -> DiskPtr
    {
        auto disk_name = config.getString(config_prefix + ".disk", "");
        if (disk_name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk Cache requires `disk` field in config");

        auto path = config.getString(config_prefix + ".path", "");
        if (path.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk Cache requires `path` field (cache base path) in config");

        auto disk_it = map.find(disk_name);
        if (disk_it == map.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is not disk with name `{}`, disk name should be initialized before cache disk", disk_name);

        auto cache_base_path = config.getString(config_prefix + ".path", fs::path(context->getPath()) / "disks" / name / "data_cache/");
        if (!fs::exists(cache_base_path))
            fs::create_directories(cache_base_path);

        FileCacheSettings file_cache_settings;
        file_cache_settings.loadFromConfig(config, config_prefix);

        // auto check_non_relesable_path = [] (const String & path)
        // {
        //     return path.ends_with("idx") // index files.
        //             || path.ends_with("mrk") || path.ends_with("mrk2") || path.ends_with("mrk3") /// mark files.
        //             || path.ends_with("txt") || path.ends_with("dat");
        // };

        auto cache = FileCacheFactory::instance().getOrCreate(cache_base_path, file_cache_settings);
        cache->initialize();

        return std::make_shared<DiskCache>(name, path, disk_it->second, cache);
    };

    factory.registerDiskType("cache", creator);
}

}
