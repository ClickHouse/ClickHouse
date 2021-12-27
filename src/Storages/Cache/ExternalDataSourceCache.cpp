#include <memory>
#include <unistd.h>
#include <functional>
#include <Core/BackgroundSchedulePool.h>
#include <Poco/Logger.h>
#include <base/logger_useful.h>
#include <base/sleep.h>
#include <base/errnoToString.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/hex.h>
#include <Common/Exception.h>
#include <Storages/Cache/ExternalDataSourceCache.h>
#include <Storages/Cache/RemoteFileMetadataFactory.h>
#include <IO/WriteHelpers.h>

namespace ProfileEvents
{
    extern const Event ExternalDataSourceLocalCacheReadBytes;
}
namespace DB
{
namespace fs = std::filesystem;
namespace ErrorCodes
{
    extern const int OK;
    extern const int LOGICAL_ERROR;
    extern const int NOT_INIT;
    extern const int DISK_OVERFLOW;
}


RemoteReadBuffer::RemoteReadBuffer(size_t buff_size) : BufferWithOwnMemory<SeekableReadBufferWithSize>(buff_size)
{
}

RemoteReadBuffer::~RemoteReadBuffer()
{
    if (file_cache_controller)
        file_cache_controller->deallocFile(std::move(file_buffer));
}

std::unique_ptr<ReadBuffer> RemoteReadBuffer::create(ContextPtr context, IRemoteFileMetadataPtr remote_file_metadata, std::unique_ptr<ReadBuffer> read_buffer, size_t buff_size)
{
    auto remote_path = remote_file_metadata->remote_path;
    auto remote_read_buffer = std::make_unique<RemoteReadBuffer>(buff_size);
    ErrorCodes::ErrorCode error;

    std::tie(remote_read_buffer->file_cache_controller, read_buffer, error) = ExternalDataSourceCache::instance().createReader(context, remote_file_metadata, read_buffer);
    if (remote_read_buffer->file_cache_controller == nullptr)
    {
        return read_buffer;
    }
    else
    {
        remote_read_buffer->file_buffer = remote_read_buffer->file_cache_controller->allocFile();
        if (!remote_read_buffer->file_buffer)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Create file readbuffer failed. {}",
                    remote_read_buffer->file_cache_controller->getLocalPath().string());
    }
    remote_read_buffer->remote_file_size = remote_file_metadata->file_size;
    return remote_read_buffer;
}

bool RemoteReadBuffer::nextImpl()
{
    auto start_offset = file_buffer->getPosition();
    auto end_offset = start_offset + file_buffer->internalBuffer().size();
    file_cache_controller->waitMoreData(start_offset, end_offset);

    auto status = file_buffer->next();
    if (status)
    {
        BufferBase::set(file_buffer->buffer().begin(),
                file_buffer->buffer().size(),
                file_buffer->offset());
        ProfileEvents::increment(ProfileEvents::ExternalDataSourceLocalCacheReadBytes, file_buffer->available());
    }
    return status;
}

off_t RemoteReadBuffer::seek(off_t offset, int whence)
{
    if (!file_buffer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot call seek() in this buffer. It's a bug!");
    /*
     * Need to wait here. For example, the current file has been download at position X, but here we try to seek to
     * postition Y (Y > X), it would fail.
     */
    file_cache_controller->waitMoreData(offset, offset + file_buffer->internalBuffer().size());
    auto ret = file_buffer->seek(offset, whence);
    BufferBase::set(file_buffer->buffer().begin(),
            file_buffer->buffer().size(),
            file_buffer->offset());
    return ret;
}

off_t RemoteReadBuffer::getPosition()
{
    return file_buffer->getPosition();
}

ExternalDataSourceCache::ExternalDataSourceCache() = default;

ExternalDataSourceCache::~ExternalDataSourceCache()
{
    recover_task_holder->deactivate();
}

ExternalDataSourceCache & ExternalDataSourceCache::instance()
{
    static ExternalDataSourceCache instance;
    return instance;
}

void ExternalDataSourceCache::recoverCachedFilesMetadata(
    const fs::path & current_path,
    size_t current_depth,
    size_t max_depth)
{
    if (current_depth >= max_depth)
    {
        std::vector<fs::path> invalid_paths;
        for (auto const & dir : fs::directory_iterator{current_path})
        {
            String path = dir.path();
            auto cache_controller = RemoteCacheController::recover(path);
            if (!cache_controller)
            {
                invalid_paths.emplace_back(path);
                continue;
            }
            if (!lru_caches->trySet(path, cache_controller))
            {
                invalid_paths.emplace_back(path);
            }
        }
        for (auto & path : invalid_paths)
        {
            fs::remove_all(path);
        }
        return;
    }

    for (auto const & dir : fs::directory_iterator{current_path})
    {
        recoverCachedFilesMetadata(dir.path(), current_depth + 1, max_depth);
    }
}

void ExternalDataSourceCache::recoverTask()
{
    recoverCachedFilesMetadata(root_dir, 1, 2);
    initialized = true;
    LOG_INFO(log, "Recovered from directory:{}", root_dir);
}

void ExternalDataSourceCache::initOnce(
    ContextPtr context,
    const String & root_dir_, size_t limit_size_, size_t bytes_read_before_flush_)
{
    std::lock_guard lock(mutex);
    if (isInitialized())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot initialize ExternalDataSourceCache twice");
    }
    LOG_INFO(
        log, "Initializing local cache for remote data sources. Local cache root path: {}, cache size limit: {}", root_dir_, limit_size_);
    root_dir = root_dir_;
    local_cache_bytes_read_before_flush = bytes_read_before_flush_;
    lru_caches = std::make_unique<CacheType>(limit_size_);

    /// create if root_dir not exists
    if (!fs::exists(fs::path(root_dir)))
    {
        fs::create_directories(fs::path(root_dir));
    }

    recover_task_holder = context->getSchedulePool().createTask("recover local cache metadata for remote files", [this]{ recoverTask(); });
    recover_task_holder->activateAndSchedule();
}

String ExternalDataSourceCache::calculateLocalPath(IRemoteFileMetadataPtr metadata) const
{
    // add version into the full_path, and not block to read the new version
    String full_path = metadata->getName() + ":" + metadata->remote_path
        + ":" + metadata->getVersion();
    UInt128 hashcode = sipHash128(full_path.c_str(), full_path.size());
    String hashcode_str = getHexUIntLowercase(hashcode);
    return fs::path(root_dir) / hashcode_str.substr(0, 3) / hashcode_str;
}

std::tuple<RemoteCacheControllerPtr, std::unique_ptr<ReadBuffer>, ErrorCodes::ErrorCode>
ExternalDataSourceCache::createReader(ContextPtr context, IRemoteFileMetadataPtr remote_file_metadata, std::unique_ptr<ReadBuffer> & read_buffer)
{
    // If something is wrong on startup, rollback to read from the original ReadBuffer
    if (!isInitialized())
    {
        LOG_ERROR(log, "ExternalDataSourceCache has not been initialized");
        return {nullptr, std::move(read_buffer), ErrorCodes::NOT_INIT};
    }

    auto remote_path = remote_file_metadata->remote_path;
    const auto & last_modification_timestamp = remote_file_metadata->last_modification_timestamp;
    auto local_path = calculateLocalPath(remote_file_metadata);
    std::lock_guard lock(mutex);
    auto cache = lru_caches->get(local_path);
    if (cache)
    {
        // the remote file has been updated, need to redownload
        if (!cache->isValid() || cache->isModified(remote_file_metadata))
        {
            LOG_TRACE(
                log,
                "Remote file ({}) has been updated. Last saved modification time: {}, actual last modification time: {}",
                remote_path,
                std::to_string(cache->getLastModificationTimestamp()),
                std::to_string(last_modification_timestamp));
            cache->markInvalid();
        }
        else
        {
            return {cache, nullptr, ErrorCodes::OK};
        }
    }

    if (!fs::exists(local_path))
        fs::create_directories(local_path);

    // cache is not found or is invalid
    auto new_cache = std::make_shared<RemoteCacheController>(remote_file_metadata, local_path, local_cache_bytes_read_before_flush);
    if (!lru_caches->trySet(local_path, new_cache))
    {
        LOG_ERROR(log, "Insert the new cache failed. new file size:{}, current total size:{}",
                remote_file_metadata->file_size,
                lru_caches->weight());
        return {nullptr, std::move(read_buffer), ErrorCodes::DISK_OVERFLOW};
    }
    new_cache->startBackgroundDownload(std::move(read_buffer), context->getSchedulePool());
    return {new_cache, nullptr, ErrorCodes::OK};
}

}
