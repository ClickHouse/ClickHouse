#pragma once
#include <mutex>
#include <list>
#include <set>
#include <map>
#include <memory>
#include <filesystem>
#include <Core/BackgroundSchedulePool.h>
#include <Poco/Logger.h>
#include <Common/UnreleasableLRUCache.h>
#include <Common/ThreadPool.h>
#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/createReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/RemoteFileMetaDataBase.h>
#include <condition_variable>
#include <Interpreters/Context.h>


namespace DB
{
enum class RemoteReadBufferCacheError : int8_t
{
    OK,
    NOT_INIT = 10,
    DISK_FULL = 11,
    FILE_INVALID = 12,
    END_OF_FILE = 20,
};

class RemoteCacheController
{
public:
    enum LocalFileStatus
    {
        TO_DOWNLOAD = 0,
        DOWNLOADING = 1,
        DOWNLOADED  = 2,
    };

    RemoteCacheController(
        RemoteFileMetaDataBasePtr file_meta_data_,
        const std::filesystem::path & local_path_,
        size_t cache_bytes_before_flush_);
    ~RemoteCacheController();

    // recover from local disk
    static std::shared_ptr<RemoteCacheController>
    recover(const std::filesystem::path & local_path);

    /**
     * Called by LocalCachedFileReader, must be used in pair
     * The second value of the return tuple is the local_path to store file.
     */
    std::unique_ptr<ReadBufferFromFileBase> allocFile();
    void deallocFile(std::unique_ptr<ReadBufferFromFileBase> buffer);

    /**
     * when allocFile be called, count++. deallocFile be called, count--.
     * the local file could be deleted only count==0
     */
    inline bool closable()
    {
        std::lock_guard lock{mutex};
        //return opened_file_streams.empty() && remote_read_buffer == nullptr;
        return opened_file_buffer_refs.empty() && file_status == DOWNLOADED;
    }
    void close();

    /**
     * called in LocalCachedFileReader read(), the reading process would be blocked until
     * enough data be downloaded.
     * If the file has finished download, the process would unblocked
     */
    RemoteReadBufferCacheError waitMoreData(size_t start_offset_, size_t end_offset_);

    inline size_t size() const { return current_offset; }

    inline const std::filesystem::path & getLocalPath() { return local_path; }
    inline String getRemotePath() const { return file_meta_data_ptr->getRemotePath(); }

    inline UInt64 getLastModificationTimestamp() const { return file_meta_data_ptr->getLastModificationTimestamp(); }
    bool checkFileChanged(RemoteFileMetaDataBasePtr file_meta_data_);
    inline void markInvalid()
    {
        std::lock_guard lock(mutex);
        valid = false;
    }
    inline bool isValid()
    {
        std::lock_guard lock(mutex);
        return valid;
    }
    RemoteFileMetaDataBasePtr getFileMetaData() { return file_meta_data_ptr; }
    inline size_t getFileSize() const { return file_meta_data_ptr->getFileSize(); }

    void startBackgroundDownload(std::shared_ptr<ReadBuffer> input_readbuffer, BackgroundSchedulePool & thread_pool);

private:
    // flush file and status information
    void flush(bool need_flush_status = false);
    bool loadInnerInformation(const std::filesystem::path & file_path);

    BackgroundSchedulePool::TaskHolder download_task_holder;
    void backgroundDownload(std::shared_ptr<ReadBuffer> remote_read_buffer);

    std::mutex mutex;
    std::condition_variable more_data_signal;

    std::set<uintptr_t> opened_file_buffer_refs; // refer to a buffer address

    String meta_data_class;
    LocalFileStatus file_status = TO_DOWNLOAD; // for tracking download process
    RemoteFileMetaDataBasePtr file_meta_data_ptr;
    std::filesystem::path local_path;

    bool valid;
    size_t local_cache_bytes_read_before_flush;
    size_t current_offset;

    //std::shared_ptr<ReadBuffer> remote_read_buffer;
    std::unique_ptr<WriteBufferFromFileBase> data_file_writer;

    Poco::Logger * log = &Poco::Logger::get("RemoteCacheController");
};
using RemoteCacheControllerPtr = std::shared_ptr<RemoteCacheController>;

/*
 * FIXME:RemoteReadBuffer derive from SeekableReadBufferWithSize may cause some risks, since it's not seekable in some cases
 * But SeekableReadBuffer is not a interface which make it hard to fixup.
 */
class RemoteReadBuffer : public BufferWithOwnMemory<SeekableReadBufferWithSize>
{
public:
    explicit RemoteReadBuffer(size_t buff_size);
    ~RemoteReadBuffer() override;
    static std::unique_ptr<ReadBuffer> create(ContextPtr contex, RemoteFileMetaDataBasePtr remote_file_meta_data, std::unique_ptr<ReadBuffer> read_buffer);

    bool nextImpl() override;
    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;
    std::optional<size_t> getTotalSize() override { return remote_file_size; }

private:
    std::shared_ptr<RemoteCacheController> file_cache_controller;
    std::unique_ptr<ReadBufferFromFileBase> file_buffer;
    size_t remote_file_size = 0;
};

struct RemoteFileCacheWeightFunction
{
    size_t operator()(const RemoteCacheController & cache) const
    {
        return cache.getFileSize();
    }
};

struct RemoteFileCacheEvictPolicy
{
    CacheEvictStatus canRelease(RemoteCacheController & cache) const
    {
        if (cache.closable())
            return CacheEvictStatus::CAN_EVITCT;
        return CacheEvictStatus::SKIP_EVICT;
    }
    void release(RemoteCacheController & cache)
    {
        cache.close();
    }
};

class RemoteReadBufferCache
{
public:
    using CacheType = UnreleasableLRUCache<String, RemoteCacheController, std::hash<String>,
          RemoteFileCacheWeightFunction, RemoteFileCacheEvictPolicy>;
    ~RemoteReadBufferCache();
    // global instance
    static RemoteReadBufferCache & instance();

    void initOnce(ContextPtr context, const String & root_dir_, size_t limit_size_, size_t bytes_read_before_flush_);

    inline bool isInitialized() const { return initialized; }

    std::tuple<RemoteCacheControllerPtr, std::unique_ptr<ReadBuffer>, RemoteReadBufferCacheError>
    createReader(ContextPtr context, RemoteFileMetaDataBasePtr remote_file_meta_data, std::unique_ptr<ReadBuffer> & read_buffer);

    void updateTotalSize(size_t size) { total_size += size; }

protected:
    RemoteReadBufferCache();

private:
    // root directory of local cache for remote filesystem
    String root_dir;
    size_t local_cache_bytes_read_before_flush = 0;

    std::atomic<bool> initialized = false;
    std::atomic<size_t> total_size;
    std::mutex mutex;
    std::unique_ptr<CacheType> lru_caches;

    Poco::Logger * log = &Poco::Logger::get("RemoteReadBufferCache");

    String calculateLocalPath(RemoteFileMetaDataBasePtr meta) const;

    BackgroundSchedulePool::TaskHolder recover_task_holder;
    void recoverTask();
    void recoverCachedFilesMetaData(
        const std::filesystem::path & current_path,
        size_t current_depth,
        size_t max_depth);
};

}
