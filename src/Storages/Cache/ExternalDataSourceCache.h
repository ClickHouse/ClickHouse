#pragma once
#include <mutex>
#include <list>
#include <set>
#include <map>
#include <memory>
#include <filesystem>
#include <Core/BackgroundSchedulePool.h>
#include <Poco/Logger.h>
#include <Common/LRUCache.h>
#include <Common/ErrorCodes.h>
#include <Common/ThreadPool.h>
#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/createReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <IO/SeekableReadBuffer.h>
#include <Storages/IRemoteFileMetadata.h>
#include <condition_variable>
#include <Interpreters/Context.h>
#include <boost/core/noncopyable.hpp>
#include <Storages/Cache/RemoteCacheController.h>
#include <Storages/Cache/RemoteFileCachePolicy.h>


namespace DB
{

/*
 * FIXME:RemoteReadBuffer derive from SeekableReadBufferWithSize may cause some risks, since it's not seekable in some cases
 * But SeekableReadBuffer is not a interface which make it hard to fixup.
 */
class RemoteReadBuffer : public BufferWithOwnMemory<SeekableReadBufferWithSize>
{
public:
    explicit RemoteReadBuffer(size_t buff_size);
    ~RemoteReadBuffer() override;
    static std::unique_ptr<ReadBuffer> create(ContextPtr contex, IRemoteFileMetadataPtr remote_file_metadata, std::unique_ptr<ReadBuffer> read_buffer);

    bool nextImpl() override;
    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;
    std::optional<size_t> getTotalSize() override { return remote_file_size; }

private:
    std::shared_ptr<RemoteCacheController> file_cache_controller;
    std::unique_ptr<ReadBufferFromFileBase> file_buffer;
    size_t remote_file_size = 0;
};

class ExternalDataSourceCache : private boost::noncopyable
{
public:
    using CacheType = LRUCache<String, RemoteCacheController, std::hash<String>,
          RemoteFileCacheWeightFunction, RemoteFileCacheEvictPolicy>;
    ~ExternalDataSourceCache();
    // global instance
    static ExternalDataSourceCache & instance();

    void initOnce(ContextPtr context, const String & root_dir_, size_t limit_size_, size_t bytes_read_before_flush_);

    inline bool isInitialized() const { return initialized; }

    std::tuple<RemoteCacheControllerPtr, std::unique_ptr<ReadBuffer>, ErrorCodes::ErrorCode>
    createReader(ContextPtr context, IRemoteFileMetadataPtr remote_file_metadata, std::unique_ptr<ReadBuffer> & read_buffer);

    void updateTotalSize(size_t size) { total_size += size; }

protected:
    ExternalDataSourceCache();

private:
    // root directory of local cache for remote filesystem
    String root_dir;
    size_t local_cache_bytes_read_before_flush = 0;

    std::atomic<bool> initialized = false;
    std::atomic<size_t> total_size;
    std::mutex mutex;
    std::unique_ptr<CacheType> lru_caches;

    Poco::Logger * log = &Poco::Logger::get("ExternalDataSourceCache");

    String calculateLocalPath(IRemoteFileMetadataPtr meta) const;

    BackgroundSchedulePool::TaskHolder recover_task_holder;
    void recoverTask();
    void recoverCachedFilesMetadata(
        const std::filesystem::path & current_path,
        size_t current_depth,
        size_t max_depth);
};

}
