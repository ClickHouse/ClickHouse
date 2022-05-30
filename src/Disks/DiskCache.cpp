#include "DiskCache.h"

#include <Common/FileCacheFactory.h>
#include <Common/IFileCache.h>
#include <Common/FileSegment.h>
#include <Common/logger_useful.h>
#include <Common/filesystemHelpers.h>

#include <Disks/DiskFactory.h>
#include <Disks/IO/CachedReadBufferFromFile.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFileDecorator.h>
#include <IO/BoundedReadBuffer.h>

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
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_USE_CACHE;
    extern const int INCORRECT_DISK_INDEX;
}

class CachedWriteBuffer final : public WriteBufferFromFileDecorator
{
public:
    CachedWriteBuffer(
        std::unique_ptr<WriteBuffer> impl_,
        FileCachePtr cache_,
        const String & source_path_,
        const IFileCache::Key & key_,
        bool is_persistent_cache_file_,
        const String & query_id_,
        const WriteSettings & settings_)
        : WriteBufferFromFileDecorator(std::move(impl_))
        , cache(cache_)
        , source_path(source_path_)
        , key(key_)
        , is_persistent_cache_file(is_persistent_cache_file_)
        , query_id(query_id_)
        , enable_cache_log(!query_id_.empty() && settings_.enable_filesystem_cache_log)
    {
    }

    void nextImpl() override
    {
        size_t size = offset();
        swap(*impl);

        try
        {
            /// Write data to the underlying buffer.
            impl->next();
        }
        catch (...)
        {
            /// If something was already written to cache, remove it.
            cache_writer->clear();
            throw;
        }

        swap(*impl);

        /// Write data to cache.
        cacheData(working_buffer.begin(), size);
        current_download_offset += size;
    }

    void cacheData(char * data, size_t size)
    {
        if (!cache_writer)
        {
            cache_writer = std::make_unique<FileSegmentRangeWriter>(
                cache.get(), key, [this](const FileSegmentPtr & file_segment) { appendFilesystemCacheLog(file_segment); });
        }

        Stopwatch watch(CLOCK_MONOTONIC);

        bool cached;
        try
        {
            cached = cache_writer->write(data, size, current_download_offset, is_persistent_cache_file);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            return;
        }

        if (!cached)
            return;

        ProfileEvents::increment(ProfileEvents::CachedWriteBufferCacheWriteBytes, size);
        ProfileEvents::increment(ProfileEvents::CachedWriteBufferCacheWriteMicroseconds, watch.elapsedMicroseconds());
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
            .requested_range = {},
            .read_type = FilesystemCacheLogElement::ReadType::READ_FROM_FS_AND_DOWNLOADED_TO_CACHE,
            .file_segment_size = file_segment_range.size(),
            .cache_attempted = false,
            .read_buffer_id = {},
            .profile_counters = std::make_shared<ProfileEvents::Counters::Snapshot>(current_file_segment_counters.getPartiallyAtomicSnapshot()),
        };

        current_file_segment_counters.reset();

        if (auto cache_log = Context::getGlobalContextInstance()->getFilesystemCacheLog())
            cache_log->add(elem);
    }

    void preFinalize() override
    {
        if (cache_writer)
            cache_writer->finalize();
    }

private:
    FileCachePtr cache;
    String source_path;
    IFileCache::Key key;

    bool is_persistent_cache_file;
    size_t current_download_offset = 0;
    const String query_id;
    bool enable_cache_log;

    ProfileEvents::Counters current_file_segment_counters;
    std::unique_ptr<FileSegmentRangeWriter> cache_writer;
};

class DiskCacheReservation : public IReservation
{
public:
    DiskCacheReservation(std::shared_ptr<DiskCache> disk_, std::unique_ptr<IReservation> reservation_)
        : disk(std::move(disk_)), reservation(std::move(reservation_))
    {
    }

    UInt64 getSize() const override { return reservation->getSize(); }
    UInt64 getUnreservedSpace() const override { return reservation->getUnreservedSpace(); }

    DiskPtr getDisk(size_t i) const override
    {
        if (i != 0)
            throw Exception(
                "Can't use i != 0 with single disk reservation",
                ErrorCodes::INCORRECT_DISK_INDEX);
        return disk;
    }

    Disks getDisks() const override { return {disk}; }

    void update(UInt64 new_size) override { reservation->update(new_size); }

private:
    std::shared_ptr<DiskCache> disk;
    std::unique_ptr<IReservation> reservation;
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

    auto impl = DiskDecorator::readFile(path, read_settings, read_hint, file_size);

    LOG_TRACE(log, "Read file: {}", path);

    /// If underlying read buffer does caching on its own, do not wrap it in caching buffer.
    if (impl->isIntegratedWithFilesystemCache() && settings.enable_filesystem_cache_on_lower_level)
    {
        return impl;
    }
    else
    {
        if (!file_size)
        {
            file_size = impl->getFileSize();
            if (!file_size)
                throw Exception(ErrorCodes::CANNOT_USE_CACHE, "Failed to find out file size for: {}", path);
        }

        auto implementation_buffer_creator = [=, this]()
        {
            auto implemenetation_buffer = DiskDecorator::readFile(path, read_settings, read_hint, file_size);
            return std::make_unique<BoundedReadBuffer>(std::move(implemenetation_buffer));
        };

        auto full_path = fs::path(getPath()) / path;
        auto file_id = toString(getINodeNumberFromPath(full_path));
        auto key = cache->hash(file_id);

        String query_id =
            CurrentThread::isInitialized() && CurrentThread::get().getQueryContext() ? CurrentThread::getQueryId().toString() : "";

        return std::make_unique<CachedReadBufferFromFile>(
            full_path,
            key,
            cache,
            implementation_buffer_creator,
            read_settings,
            query_id,
            file_size.value(),
            /* allow_seeks */true,
            /* use_external_buffer */false);
    }
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

    if (cache_on_write)
    {
        LOG_TRACE(
            log, "Caching file `{}` ({}) to `{}`, query_id: {}",
            impl->getFileName(), path, cache->hash(impl->getFileName()).toString(), query_id);

        IFileCache::Key key;
        if (isRemote())
        {
            key = cache->hash(impl->getFileName());
        }
        else
        {
            String full_path = fs::path(getPath()) / path;
            auto file_id = toString(getINodeNumberFromPath(full_path));
            key = cache->hash(file_id);
        }
        return std::make_unique<CachedWriteBuffer>(
            std::move(impl),
            cache,
            impl->getFileName(),
            key,
            isFilePersistent(path),
            query_id,
            settings);
    }

    return impl;
}

void DiskCache::removeCacheIfExists(const String & path)
{
    try
    {
        if (isRemote())
        {
            auto remote_paths = getRemotePaths(path);
            for (const auto & remote_path : remote_paths)
            {
                auto key = cache->hash(remote_path);
                cache->removeIfExists(key);
            }
        }
        else
        {
            String full_path = fs::path(getPath()) / path;
            auto file_id = toString(getINodeNumberFromPath(full_path));
            auto key = cache->hash(file_id);
            cache->removeIfExists(key);
        }
    }
    catch ([[maybe_unused]] const Exception & e)
    {
#ifdef NDEBUG
        /// Protect against concurrent file delition.
        if (e.code() == ErrorCodes::FILE_DOESNT_EXIST)
        {
            LOG_WARNING(
                log,
                "Cache file for path {} does not exist. "
                "Possibly because of a concurrent attempt to delete it",
                path);
            return;
        }
#endif
        throw;
    }
}

void DiskCache::removeCacheIfExistsRecursive(const String & path)
{
    iterateRecursively(path, [this](const String & disk_path)
    {
        removeCacheIfExists(disk_path);
    });
}

bool DiskCache::removeFile(const String & path)
{
    removeCacheIfExists(path);
    return DiskDecorator::removeFile(path);
}

bool DiskCache::removeFileIfExists(const String & path)
{
    removeCacheIfExists(path);
    return DiskDecorator::removeFileIfExists(path);
}

void DiskCache::removeDirectory(const String & path)
{
    removeCacheIfExistsRecursive(path);
    DiskDecorator::removeDirectory(path);
}

void DiskCache::removeRecursive(const String & path)
{
    removeCacheIfExistsRecursive(path);
    DiskDecorator::removeRecursive(path);
}

ReservationPtr DiskCache::reserve(UInt64 bytes)
{
    auto reservation = delegate->reserve(bytes);
    if (!reservation)
        return {};
    return std::make_unique<DiskCacheReservation>(std::static_pointer_cast<DiskCache>(shared_from_this()), std::move(reservation));
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
