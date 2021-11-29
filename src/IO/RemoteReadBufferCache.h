#pragma once
#include <mutex>
#include <list>
#include <set>
#include <map>
#include <memory>
#include <filesystem>
#include <Poco/Logger.h>
#include <Common/ThreadPool.h>
#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/SeekableReadBuffer.h>
#include <condition_variable>

namespace fs = std::filesystem;

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

struct RemoteFileMetadata
{
    RemoteFileMetadata(
        const String & schema_,
        const String & cluster_,
        const String & path_,
        UInt64 last_modify_time_,
        size_t file_size_)
        : schema(schema_)
        , cluster(cluster_)
        , path(path_)
        , last_modify_time(last_modify_time_)
        , file_size(file_size_)
    {
    }

    String schema; // Hive, S2 etc.
    String cluster;
    String path;
    UInt64 last_modify_time;
    size_t file_size;
};

class RemoteCacheController
{
public:
    RemoteCacheController(
        const RemoteFileMetadata & meta,
        const String & local_path_,
        size_t cache_bytes_before_flush_,
        std::shared_ptr<ReadBuffer> readbuffer_,
        std::function<void(RemoteCacheController *)> const & finish_callback);
    ~RemoteCacheController();

    // recover from local disk
    static std::shared_ptr<RemoteCacheController>
    recover(const String & local_path, std::function<void(RemoteCacheController *)> const & finish_callback);

    /**
     * Called by LocalCachedFileReader, must be used in pair
     * The second value of the return tuple is the local_path to store file.
     * It will be empty if the file has not been downloaded
     */
    std::pair<FILE *, String> allocFile();
    void deallocFile(FILE * fs_);

    /**
     * when allocFile be called, count++. deallocFile be called, count--.
     * the local file could be deleted only count==0
     */
    inline bool closable()
    {
        std::lock_guard lock{mutex};
        return opened_file_streams.empty() && remote_readbuffer == nullptr;
    }
    void close();

    /**
     * called in LocalCachedFileReader read(), the reading process would be blocked until
     * enough data be downloaded.
     * If the file has finished download, the process would unblocked
     */
    RemoteReadBufferCacheError waitMoreData(size_t start_offset_, size_t end_offset_);

    inline size_t size() const { return current_offset; }

    inline String getLocalPath() const { return local_path; }
    inline String getRemotePath() const { return remote_path; }

    inline UInt64 getLastModificationTimestamp() const { return last_modify_time; }
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

private:
    // flush file and meta info into disk
    void flush(bool need_flush_meta_ = false);

    void backgroundDownload(std::function<void(RemoteCacheController *)> const & finish_callback);

    std::mutex mutex;
    std::condition_variable more_data_signal;

    std::set<FILE *> opened_file_streams;

    // meta info
    String schema;
    String cluster;
    String remote_path;
    String local_path;
    UInt64 last_modify_time;

    bool valid;
    size_t local_cache_bytes_read_before_flush;
    bool download_finished;
    size_t current_offset;

    std::shared_ptr<ReadBuffer> remote_readbuffer;
    std::unique_ptr<std::ofstream> out_file;

    Poco::Logger * log = &Poco::Logger::get("RemoteReadBufferCache");
};

/**
 * access local cached files by RemoteCacheController, and be used in RemoteReadBuffer
 */
class LocalCachedFileReader
{
public:
    LocalCachedFileReader(RemoteCacheController * cache_controller_, size_t size_);
    ~LocalCachedFileReader();

    // expect to read size bytes into buf, return is the real bytes read
    size_t read(char * buf, size_t size);
    inline off_t getOffset() const { return static_cast<off_t>(offset); }
    size_t size();
    off_t seek(off_t offset);
    inline String getPath() const { return local_path; }

private:
    std::mutex mutex;
    size_t offset;
    size_t file_size;
    FILE * fs;
    String local_path;
    RemoteCacheController * cache_controller;

    Poco::Logger * log = &Poco::Logger::get("RemoteReadBufferCache");
};

/*
 * FIXME:RemoteReadBuffer derive from SeekableReadBuffer may case some risks, since it's not seekable in some cases
 * But SeekableReadBuffer is not a interface which make it hard to fixup.
 */
class RemoteReadBuffer : public BufferWithOwnMemory<SeekableReadBuffer>
{
public:
    explicit RemoteReadBuffer(size_t buff_size);
    ~RemoteReadBuffer() override;
    static std::unique_ptr<RemoteReadBuffer> create(const RemoteFileMetadata & remote_file_meta, std::unique_ptr<ReadBuffer> read_buffer);

    bool nextImpl() override;
    inline bool seekable() { return file_reader != nullptr && file_reader->size() > 0; }
    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;
    inline size_t size() { return file_reader->size(); }

private:
    std::shared_ptr<LocalCachedFileReader> file_reader;
    std::shared_ptr<ReadBuffer> original_readbuffer;
};

class RemoteReadBufferCache
{
public:
    ~RemoteReadBufferCache();
    // global instance
    static RemoteReadBufferCache & instance();

    std::shared_ptr<FreeThreadPool> getThreadPool() { return thread_pool; }

    void initOnce(const String & root_dir_, size_t limit_size_, size_t bytes_read_before_flush_, size_t max_threads_);

    inline bool isInitialized() const { return initialized; }

    std::pair<std::shared_ptr<LocalCachedFileReader>, RemoteReadBufferCacheError>
    createReader(const RemoteFileMetadata & remote_file_meta, std::shared_ptr<ReadBuffer> & read_buffer);

    void updateTotalSize(size_t size) { total_size += size; }

protected:
    RemoteReadBufferCache();

private:
    // root directory of local cache for remote filesystem
    String root_dir;
    size_t limit_size = 0;
    size_t local_cache_bytes_read_before_flush = 0;

    std::shared_ptr<FreeThreadPool> thread_pool;
    std::atomic<bool> initialized = false;
    std::atomic<size_t> total_size;
    std::mutex mutex;

    Poco::Logger * log = &Poco::Logger::get("RemoteReadBufferCache");

    struct CacheCell
    {
        std::list<String>::iterator key_iterator;
        std::shared_ptr<RemoteCacheController> cache_controller;
    };
    std::list<String> keys;
    std::map<String, CacheCell> caches;

    String calculateLocalPath(const RemoteFileMetadata & meta) const;

    void recoverCachedFilesMeta(
        const std::filesystem::path & current_path,
        size_t current_depth,
        size_t max_depth,
        std::function<void(RemoteCacheController *)> const & finish_callback);
    bool clearLocalCache();
};

}
