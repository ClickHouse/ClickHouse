#include <algorithm>
#include <functional>
#include <memory>
#include <unistd.h>
#include <Core/BackgroundSchedulePool.h>
#include <IO/WriteHelpers.h>
#include <Storages/Cache/ExternalDataSourceCache.h>
#include <Storages/Cache/RemoteFileMetadataFactory.h>
#include <base/errnoToString.h>
#include <base/sort.h>
#include <Common/logger_useful.h>
#include <base/sleep.h>
#include <Poco/Logger.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <base/hex.h>
#include <Core/Types.h>
#include <base/types.h>
#include <consistent_hashing.h>
#include <base/find_symbols.h>

namespace ProfileEvents
{
extern const Event ExternalDataSourceLocalCacheReadBytes;
}
namespace DB
{
namespace fs = std::filesystem;
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

LocalFileHolder::LocalFileHolder(RemoteFileCacheType::MappedHolderPtr cache_controller)
    : file_cache_controller(std::move(cache_controller)), original_readbuffer(nullptr), thread_pool(nullptr)
{
    file_buffer = file_cache_controller->value().allocFile();
    if (!file_buffer)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Create file readbuffer failed. {}", file_cache_controller->value().getLocalPath().string());
}

LocalFileHolder::LocalFileHolder(
    RemoteFileCacheType::MappedHolderPtr cache_controller,
    std::unique_ptr<ReadBuffer> original_readbuffer_,
    BackgroundSchedulePool * thread_pool_)
    : file_cache_controller(std::move(cache_controller))
    , file_buffer(nullptr)
    , original_readbuffer(std::move(original_readbuffer_))
    , thread_pool(thread_pool_)
{
}

LocalFileHolder::~LocalFileHolder()
{
    if (original_readbuffer)
    {
        try
        {
            assert_cast<SeekableReadBuffer *>(original_readbuffer.get())->seek(0, SEEK_SET);
            file_cache_controller->value().startBackgroundDownload(std::move(original_readbuffer), *thread_pool);
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("LocalFileHolder"), "Exception during destructor of LocalFileHolder.");
        }
    }
}

RemoteReadBuffer::RemoteReadBuffer(size_t buff_size) : BufferWithOwnMemory<SeekableReadBuffer>(buff_size)
{
}

std::unique_ptr<ReadBuffer> RemoteReadBuffer::create(
    ContextPtr context,
    IRemoteFileMetadataPtr remote_file_metadata,
    std::unique_ptr<ReadBuffer> read_buffer,
    size_t buff_size,
    bool is_random_accessed)

{
    auto remote_read_buffer = std::make_unique<RemoteReadBuffer>(buff_size);

    std::tie(remote_read_buffer->local_file_holder, read_buffer)
        = ExternalDataSourceCache::instance().createReader(context, remote_file_metadata, read_buffer, is_random_accessed);
    if (remote_read_buffer->local_file_holder == nullptr)
        return read_buffer;
    remote_read_buffer->remote_file_size = remote_file_metadata->file_size;
    return remote_read_buffer;
}

bool RemoteReadBuffer::nextImpl()
{
    if (local_file_holder->original_readbuffer)
    {
        auto status = local_file_holder->original_readbuffer->next();
        if (status)
        {
            BufferBase::set(
                local_file_holder->original_readbuffer->buffer().begin(),
                local_file_holder->original_readbuffer->buffer().size(),
                local_file_holder->original_readbuffer->offset());
        }
        return status;
    }

    /// file_buffer::pos should increase correspondingly when RemoteReadBuffer is consumed, otherwise start_offset will be incorrect.
    local_file_holder->file_buffer->position() = local_file_holder->file_buffer->buffer().begin() + BufferBase::offset();
    auto start_offset = local_file_holder->file_buffer->getPosition();
    auto end_offset = start_offset + local_file_holder->file_buffer->internalBuffer().size();
    local_file_holder->file_cache_controller->value().waitMoreData(start_offset, end_offset);

    auto status = local_file_holder->file_buffer->next();
    if (status)
    {
        BufferBase::set(
            local_file_holder->file_buffer->buffer().begin(),
            local_file_holder->file_buffer->buffer().size(),
            local_file_holder->file_buffer->offset());
        ProfileEvents::increment(ProfileEvents::ExternalDataSourceLocalCacheReadBytes, local_file_holder->file_buffer->available());
    }
    return status;
}

off_t RemoteReadBuffer::seek(off_t offset, int whence)
{
    if (local_file_holder->original_readbuffer)
    {
        auto ret = assert_cast<SeekableReadBuffer *>(local_file_holder->original_readbuffer.get())->seek(offset, whence);
        BufferBase::set(
            local_file_holder->original_readbuffer->buffer().begin(),
            local_file_holder->original_readbuffer->buffer().size(),
            local_file_holder->original_readbuffer->offset());
        return ret;
    }

    if (!local_file_holder->file_buffer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot call seek() in this buffer. It's a bug!");
    /*
     * Need to wait here. For example, the current file has been download at position X, but here we try to seek to
     * position Y (Y > X), it would fail.
     */
    auto & file_buffer = local_file_holder->file_buffer;
    local_file_holder->file_cache_controller->value().waitMoreData(offset, offset + file_buffer->internalBuffer().size());
    auto ret = file_buffer->seek(offset, whence);
    BufferBase::set(file_buffer->buffer().begin(), file_buffer->buffer().size(), file_buffer->offset());
    return ret;
}

off_t RemoteReadBuffer::getPosition()
{
    if (local_file_holder->original_readbuffer)
    {
        return assert_cast<SeekableReadBuffer *>(local_file_holder->original_readbuffer.get())->getPosition();
    }
    return local_file_holder->file_buffer->getPosition();
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

void ExternalDataSourceCache::recoverTask()
{
    std::vector<fs::path> invalid_paths;
    for (size_t i = 0, sz = root_dirs.size(); i < sz; ++i)
    {
        const auto & root_dir = root_dirs[i];
        for (auto const & group_dir : fs::directory_iterator{root_dir})
        {
            for (auto const & cache_dir : fs::directory_iterator{group_dir.path()})
            {
                String subpath = cache_dir.path().stem();
                String path = cache_dir.path();
                size_t root_dir_idx = ConsistentHashing(sipHash64(subpath.c_str(), subpath.size()), sz);
                if (root_dir_idx != i)
                {
                    // When the root_dirs has been changed, to simplify just delete the old cached files.
                    LOG_TRACE(
                        log,
                        "Drop file({}) since root_dir is not match. prev dir is {}, and it should be {}",
                        path,
                        root_dirs[i],
                        root_dirs[root_dir_idx]);
                    invalid_paths.emplace_back(path);
                    continue;
                }
                auto cache_controller = RemoteCacheController::recover(path);
                if (!cache_controller)
                {
                    invalid_paths.emplace_back(path);
                    continue;
                }
                auto cache_load_func = [&] { return cache_controller; };
                if (!lru_caches->getOrSet(path, cache_load_func))
                {
                    invalid_paths.emplace_back(path);
                }
            }
        }
    }
    for (auto & path : invalid_paths)
        (void)fs::remove_all(path);
    initialized = true;

    auto root_dirs_to_string = [&]()
    {
        String res;
        for (const auto & root_dir : root_dirs)
        {
            res += root_dir + ",";
        }
        return res;
    };
    LOG_INFO(log, "Recovered from directory:{}", root_dirs_to_string());
}

void ExternalDataSourceCache::initOnce(ContextPtr context, const String & root_dir_, size_t limit_size_, size_t bytes_read_before_flush_)
{
    std::lock_guard lock(mutex);
    if (isInitialized())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot initialize ExternalDataSourceCache twice");
    }
    LOG_INFO(
        log, "Initializing local cache for remote data sources. Local cache root path: {}, cache size limit: {}", root_dir_, limit_size_);
    splitInto<','>(root_dirs, root_dir_);
    ::sort(root_dirs.begin(), root_dirs.end());
    local_cache_bytes_read_before_flush = bytes_read_before_flush_;
    lru_caches = std::make_unique<RemoteFileCacheType>(limit_size_);

    /// Create if root_dir not exists.
    for (const auto & root_dir : root_dirs)
    {
        if (!fs::exists(fs::path(root_dir)))
        {
            fs::create_directories(fs::path(root_dir));
        }
    }

    recover_task_holder = context->getSchedulePool().createTask("recover local cache metadata for remote files", [this] { recoverTask(); });
    recover_task_holder->activateAndSchedule();
}

String ExternalDataSourceCache::calculateLocalPath(IRemoteFileMetadataPtr metadata) const
{
    // Add version into the full_path, and not block to read the new version.
    String full_path = metadata->getName() + ":" + metadata->remote_path + ":" + metadata->getVersion();
    UInt128 hashcode = sipHash128(full_path.c_str(), full_path.size());
    String hashcode_str = getHexUIntLowercase(hashcode);
    size_t root_dir_idx = ConsistentHashing(sipHash64(hashcode_str.c_str(), hashcode_str.size()), root_dirs.size());
    return fs::path(root_dirs[root_dir_idx]) / hashcode_str.substr(0, 3) / hashcode_str;
}

std::pair<std::unique_ptr<LocalFileHolder>, std::unique_ptr<ReadBuffer>> ExternalDataSourceCache::createReader(
    ContextPtr context, IRemoteFileMetadataPtr remote_file_metadata, std::unique_ptr<ReadBuffer> & read_buffer, bool is_random_accessed)
{
    // If something is wrong on startup, rollback to read from the original ReadBuffer.
    if (!isInitialized())
    {
        LOG_ERROR(log, "ExternalDataSourceCache has not been initialized");
        return {nullptr, std::move(read_buffer)};
    }

    auto remote_path = remote_file_metadata->remote_path;
    const auto & last_modification_timestamp = remote_file_metadata->last_modification_timestamp;
    auto local_path = calculateLocalPath(remote_file_metadata);
    std::lock_guard lock(mutex);
    auto cache = lru_caches->get(local_path);
    if (cache)
    {
        if (!cache->value().isEnable())
        {
            return {nullptr, std::move(read_buffer)};
        }

        // The remote file has been updated, need to redownload.
        if (!cache->value().isValid() || cache->value().isModified(remote_file_metadata))
        {
            LOG_TRACE(
                log,
                "Remote file ({}) has been updated. Last saved modification time: {}, actual last modification time: {}",
                remote_path,
                std::to_string(cache->value().getLastModificationTimestamp()),
                std::to_string(last_modification_timestamp));
            cache->value().markInvalid();
            cache.reset();
        }
        else
        {
            return {std::make_unique<LocalFileHolder>(std::move(cache)), nullptr};
        }
    }

    if (!fs::exists(local_path))
        fs::create_directories(local_path);

    // Cache is not found or is invalid, try to remove it at first.
    lru_caches->tryRemove(local_path);

    auto new_cache_controller
        = std::make_shared<RemoteCacheController>(remote_file_metadata, local_path, local_cache_bytes_read_before_flush);
    auto new_cache = lru_caches->getOrSet(local_path, [&] { return new_cache_controller; });
    if (!new_cache)
    {
        LOG_ERROR(
            log,
            "Insert the new cache failed. new file size:{}, current total size:{}",
            remote_file_metadata->file_size,
            lru_caches->weight());
        return {nullptr, std::move(read_buffer)};
    }
    /*
      If read_buffer is seekable, use read_buffer directly inside LocalFileHolder. And once LocalFileHolder is released,
      start the download process in background.
      The cache is marked disable until the download process finish.
      For reading parquet files from hdfs, with this optimization, the speedup can reach 3x.
    */
    if (dynamic_cast<SeekableReadBuffer *>(read_buffer.get()) && is_random_accessed)
    {
        new_cache->value().disable();
        return {std::make_unique<LocalFileHolder>(std::move(new_cache), std::move(read_buffer), &context->getSchedulePool()), nullptr};
    }
    new_cache->value().startBackgroundDownload(std::move(read_buffer), context->getSchedulePool());
    return {std::make_unique<LocalFileHolder>(std::move(new_cache)), nullptr};
}

}
