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
#include <Disks/IO/CachedWriteBufferFromFile.h>

#include <Interpreters/FilesystemCacheLog.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <filesystem>

namespace fs = std::filesystem;


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_USE_CACHE;
    extern const int INCORRECT_DISK_INDEX;
}

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

static bool isFilePersistent(const String & path)
{
    return path.ends_with("idx") // index files.
            || path.ends_with("mrk") || path.ends_with("mrk2") || path.ends_with("mrk3") /// mark files.
            || path.ends_with("txt") || path.ends_with("dat");
}

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

static String getLocalFilesystemBasedFileId(const String & path)
{
    // auto stat_vfs = getStatVFS(path);
    // auto file_id = fmt::format("{}:{}", stat_vfs.f_fsid, getINodeNumberFromPath(path));
    auto file_id = toString(getINodeNumberFromPath(path));
    return file_id;
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
        auto file_id = getLocalFilesystemBasedFileId(full_path);
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
            auto file_id = getLocalFilesystemBasedFileId(full_path);
            key = cache->hash(file_id);
        }

        return std::make_unique<CachedWriteBufferFromFile>(
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
    /// TODO: normal check for existence
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
            auto file_id = getLocalFilesystemBasedFileId(full_path);
            auto key = cache->hash(file_id);
            cache->removeIfExists(key);
        }
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::FILE_DOESNT_EXIST)
            return;
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
    bool removed = DiskDecorator::removeFile(path);
    return removed;
}

bool DiskCache::removeFileIfExists(const String & path)
{
    removeCacheIfExists(path);
    bool removed = DiskDecorator::removeFileIfExists(path);
    return removed;
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

bool DiskCache::removeSharedFile(const String & path, bool keep_in_fs)
{
    removeCacheIfExists(path);
    bool removed = DiskDecorator::removeSharedFile(path, keep_in_fs);
    return removed;
}

bool DiskCache::removeSharedFileIfExists(const String & path, bool keep_in_fs)
{
    removeCacheIfExists(path);
    bool removed = DiskDecorator::removeSharedFileIfExists(path, keep_in_fs);
    return removed;
}

void DiskCache::removeSharedFiles(
    const RemoveBatchRequest & requests, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only)
{
    for (const auto & req : requests)
        removeCacheIfExists(req.path);
    DiskDecorator::removeSharedFiles(requests, keep_all_batch_data, file_names_remove_metadata_only);
}

void DiskCache::removeSharedRecursive(
    const String & path, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only)
{
    removeCacheIfExistsRecursive(path);
    DiskDecorator::removeSharedRecursive(path, keep_all_batch_data, file_names_remove_metadata_only);
}

void DiskCache::clearDirectory(const String & path)
{
    removeCacheIfExistsRecursive(path);
    DiskDecorator::clearDirectory(path);
}

ReservationPtr DiskCache::reserve(UInt64 bytes)
{
    auto reservation = delegate->reserve(bytes);

    if (!reservation)
        return {};

    return std::make_unique<DiskCacheReservation>(
        std::static_pointer_cast<DiskCache>(shared_from_this()), std::move(reservation));
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
