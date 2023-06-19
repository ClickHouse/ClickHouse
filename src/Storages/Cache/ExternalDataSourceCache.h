#pragma once
#include <condition_variable>
#include <filesystem>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <Core/BackgroundSchedulePool.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Interpreters/Context.h>
#include <Storages/Cache/IRemoteFileMetadata.h>
#include <Storages/Cache/RemoteCacheController.h>
#include <Storages/Cache/RemoteFileCachePolicy.h>
#include <boost/core/noncopyable.hpp>
#include <Poco/Logger.h>
#include <Common/ErrorCodes.h>
#include <Common/LRUResourceCache.h>
#include <Common/ThreadPool.h>


namespace DB
{
using RemoteFileCacheType = LRUResourceCache<String, RemoteCacheController, RemoteFileCacheWeightFunction, RemoteFileCacheReleaseFunction>;

class LocalFileHolder
{
public:
    explicit LocalFileHolder(RemoteFileCacheType::MappedHolderPtr cache_controller);
    explicit LocalFileHolder(RemoteFileCacheType::MappedHolderPtr cache_controller, std::unique_ptr<ReadBuffer> original_readbuffer_, BackgroundSchedulePool * thread_pool_);
    ~LocalFileHolder();

    RemoteFileCacheType::MappedHolderPtr file_cache_controller;
    std::unique_ptr<ReadBufferFromFileBase> file_buffer;
    std::unique_ptr<ReadBuffer> original_readbuffer;
    BackgroundSchedulePool * thread_pool;
};

class RemoteReadBuffer : public BufferWithOwnMemory<SeekableReadBuffer>, public WithFileSize
{
public:
    explicit RemoteReadBuffer(size_t buff_size);
    ~RemoteReadBuffer() override = default;
    static std::unique_ptr<ReadBuffer> create(ContextPtr context, IRemoteFileMetadataPtr remote_file_metadata, std::unique_ptr<ReadBuffer> read_buffer, size_t buff_size, bool is_random_accessed = false);

    bool nextImpl() override;
    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;
    size_t getFileSize() override { return remote_file_size; }

private:
    std::unique_ptr<LocalFileHolder> local_file_holder;
    size_t remote_file_size = 0;
};


class ExternalDataSourceCache : private boost::noncopyable
{
public:
    ~ExternalDataSourceCache();
    // global instance
    static ExternalDataSourceCache & instance();

    void initOnce(ContextPtr context, const String & root_dir_, size_t limit_size_, size_t bytes_read_before_flush_);

    inline bool isInitialized() const { return initialized; }

    std::pair<std::unique_ptr<LocalFileHolder>, std::unique_ptr<ReadBuffer>>
    createReader(ContextPtr context, IRemoteFileMetadataPtr remote_file_metadata, std::unique_ptr<ReadBuffer> & read_buffer, bool is_random_accessed);


    void updateTotalSize(size_t size) { total_size += size; }

protected:
    ExternalDataSourceCache();

private:
    // Root directory of local cache for remote filesystem.
    Strings root_dirs;
    size_t local_cache_bytes_read_before_flush = 0;

    std::atomic<bool> initialized = false;
    std::atomic<size_t> total_size;
    std::mutex mutex;
    std::unique_ptr<RemoteFileCacheType> lru_caches;

    Poco::Logger * log = &Poco::Logger::get("ExternalDataSourceCache");

    String calculateLocalPath(IRemoteFileMetadataPtr meta) const;

    BackgroundSchedulePool::TaskHolder recover_task_holder;
    void recoverTask();
};
}
