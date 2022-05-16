#include "DiskCache.h"

#include <Common/FileCacheFactory.h>
#include <Common/IFileCache.h>
#include <Common/FileSegment.h>
#include <Common/logger_useful.h>
#include <Disks/DiskFactory.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFileDecorator.h>
#include <Interpreters/FilesystemCacheLog.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <filesystem>

namespace fs = std::filesystem;


namespace ProfileEvents
{
    extern const Event CachedWriteBufferCacheWriteBytes;
    extern const Event CachedWriteBufferCacheWriteMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class CachedWriteBuffer final : public WriteBufferFromFileDecorator
{
public:
    CachedWriteBuffer(
        std::unique_ptr<WriteBuffer> impl_,
        FileCachePtr cache_,
        const String & path_,
        bool is_persistent_cache_file_,
        const String & query_id_,
        const WriteSettings & settings_)
        : WriteBufferFromFileDecorator(std::move(impl_))
        , cache(cache_)
        , source_path(path_)
        , key(cache_->hash(path_))
        , is_persistent_cache_file(is_persistent_cache_file_)
        , query_id(query_id_)
        , enable_cache_log(!query_id_.empty() && settings_.enable_filesystem_cache_log)
        , writer(cache_.get(), key, [this](const FileSegmentPtr & file_segment) { appendFilesystemCacheLog(file_segment); })
    {
    }

    void appendFilesystemCacheLog(const FileSegmentPtr & file_segment)
    {
        auto file_segment_range = file_segment->range();
        FilesystemCacheLogElement elem
        {
            .event_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()),
            .query_id = query_id,
            .source_file_path = source_path,
            .file_segment_range = { file_segment_range.left, file_segment_range.right },
            .read_type = FilesystemCacheLogElement::ReadType::READ_FROM_FS_AND_DOWNLOADED_TO_CACHE,
            .requested_range = {},
            .file_segment_size = file_segment_range.size(),
            .cache_attempted = false,
            .read_buffer_id = {},
            .profile_counters = std::make_shared<ProfileEvents::Counters::Snapshot>(current_file_segment_counters.getPartiallyAtomicSnapshot()),
        };

        current_file_segment_counters.reset();

        if (auto cache_log = Context::getGlobalContextInstance()->getFilesystemCacheLog())
            cache_log->add(elem);
    }

    void nextImpl() override
    {
        size_t size = offset();

        swap(*impl);

        impl->next();

        swap(*impl);

        if (caching_stopped)
            return;

        Stopwatch watch(CLOCK_MONOTONIC);

        auto enough_space_in_cache = writer.write(working_buffer.begin(), size, current_download_offset, is_persistent_cache_file);
        if (!enough_space_in_cache)
        {
            caching_stopped = true;
            return;
        }

        current_download_offset += size;
        ProfileEvents::increment(ProfileEvents::CachedWriteBufferCacheWriteBytes, size);
        ProfileEvents::increment(ProfileEvents::CachedWriteBufferCacheWriteMicroseconds, watch.elapsedMicroseconds());
    }

    void preFinalize() override
    {
        writer.finalize();
    }

private:
    FileCachePtr cache;
    String source_path;
    IFileCache::Key key;

    bool is_persistent_cache_file;
    size_t current_download_offset = 0;
    const String query_id;
    bool enable_cache_log;

    FileSegmentRangeWriter writer;
    bool caching_stopped = false;
    ProfileEvents::Counters current_file_segment_counters;
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
    , log(&Poco::Logger::get("DiskCache(" + disk_name_ + ")"))
{
}

static bool isFilePersistent(const String & path)
{
    return path.ends_with("idx") // index files.
            || path.ends_with("mrk") || path.ends_with("mrk2") || path.ends_with("mrk3") /// mark files.
            || path.ends_with("txt") || path.ends_with("dat");
}

std::unique_ptr<ReadBufferFromFileBase> DiskCache::readFile(
    const String & path, const ReadSettings & settings, std::optional<size_t> read_hint, std::optional<size_t> file_size) const
{
    ReadSettings read_settings{settings};
    read_settings.remote_fs_cache = cache;

    if (IFileCache::isReadOnly())
        read_settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache = true;

    if (settings.filesystem_cache_do_not_evict_index_and_marks_files && isFilePersistent(path))
        read_settings.cache_file_as_persistent = true;

    return DiskDecorator::readFile(path, read_settings, read_hint, file_size);
}

std::unique_ptr<WriteBufferFromFileBase> DiskCache::writeFile(
    const String & path, size_t buf_size, WriteMode mode, const WriteSettings & settings)
{
    auto impl = DiskDecorator::writeFile(path, buf_size, mode, settings);

    bool cache_on_write = fs::path(path).extension() != ".tmp"
        && settings.enable_filesystem_cache_on_write_operations
        && FileCacheFactory::instance().getSettings(cache_base_path).cache_on_write_operations;

    String query_id = CurrentThread::isInitialized() && CurrentThread::get().getQueryContext() != nullptr
        ? CurrentThread::getQueryId().toString() : "";

    LOG_TEST(log, "Caching file `{}` to `{}`", impl->getFileName(), cache->hash(impl->getFileName()).toString());

    if (cache_on_write)
    {
        return std::make_unique<CachedWriteBuffer>(
            std::move(impl), cache, impl->getFileName(), isFilePersistent(path), query_id, settings);
    }

    return impl;
}

void DiskCache::removeCache(const String & path)
{
    auto remote_paths = getRemotePaths(path);
    for (const auto & remote_path : remote_paths)
    {
        auto key = cache->hash(remote_path);
        cache->removeIfExists(key);
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
            cache->removeIfExists(key);
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

        auto cache = FileCacheFactory::instance().getOrCreate(cache_base_path, file_cache_settings);
        cache->initialize();

        return std::make_shared<DiskCache>(name, path, disk_it->second, cache);
    };

    factory.registerDiskType("cache", creator);
}

}
