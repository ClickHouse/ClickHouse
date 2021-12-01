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
    enum LocalStatus
    {
        TO_DOWNLOAD = 0,
        DOWNLOADING = 1,
        DOWNLOADED  = 2,
    };
    RemoteFileMetadata(): last_modification_timestamp(0l), file_size(0), status(TO_DOWNLOAD){}
    RemoteFileMetadata(
        const String & schema_,
        const String & cluster_,
        const String & path_,
        UInt64 last_modification_timestamp_,
        size_t file_size_)
        : schema(schema_)
        , cluster(cluster_)
        , remote_path(path_)
        , last_modification_timestamp(last_modification_timestamp_)
        , file_size(file_size_)
        , status(TO_DOWNLOAD)
    {
    }

    bool load(const std::filesystem::path & local_path);
    void save(const std::filesystem::path & local_path) const;
    String toString() const;

    String schema; // Hive, S2 etc.
    String cluster;
    String remote_path;
    UInt64 last_modification_timestamp;
    size_t file_size;
    LocalStatus status;
};

class RemoteCacheController
{
public:
    RemoteCacheController(
        const RemoteFileMetadata & file_meta_data_,
        const std::filesystem::path & local_path_,
        size_t cache_bytes_before_flush_,
        std::shared_ptr<ReadBuffer> read_buffer_);
    ~RemoteCacheController();

    // recover from local disk
    static std::shared_ptr<RemoteCacheController>
    recover(const std::filesystem::path & local_path);

    /**
     * Called by LocalCachedFileReader, must be used in pair
     * The second value of the return tuple is the local_path to store file.
     * It will be empty if the file has not been downloaded
     */
    std::pair<FILE *, String> allocFile();
    void deallocFile(FILE * file_stream);

    /**
     * when allocFile be called, count++. deallocFile be called, count--.
     * the local file could be deleted only count==0
     */
    inline bool closable()
    {
        std::lock_guard lock{mutex};
        return opened_file_streams.empty() && remote_read_buffer == nullptr;
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
    inline const String & getRemotePath() const { return file_meta_data.remote_path; }

    inline UInt64 getLastModificationTimestamp() const { return file_meta_data.last_modification_timestamp; }
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
    void flush(bool need_flush_meta_data_ = false);

    void backgroundDownload();

    std::mutex mutex;
    std::condition_variable more_data_signal;

    std::set<FILE *> opened_file_streams;

    // meta info
    RemoteFileMetadata file_meta_data;
    std::filesystem::path local_path;

    bool valid;
    size_t local_cache_bytes_read_before_flush;
    size_t current_offset;

    std::shared_ptr<ReadBuffer> remote_read_buffer;
    std::unique_ptr<std::ofstream> out_file;

    Poco::Logger * log = &Poco::Logger::get("RemoteReadBufferCache");
};

/**
 * access local cached files by RemoteCacheController, and be used in RemoteReadBuffer
 */
class LocalCachedFileReader
{
public:
    LocalCachedFileReader(RemoteCacheController * cache_controller_, size_t file_size_);
    ~LocalCachedFileReader();

    // expect to read size bytes into buf, return is the real bytes read
    size_t read(char * buf, size_t size);
    off_t seek(off_t offset);

    inline String getPath() const { return local_path; }
    inline off_t getOffset() const { return static_cast<off_t>(offset); }
    size_t getSize();


private:
    RemoteCacheController * cache_controller;
    size_t file_size;
    size_t offset;

    std::mutex mutex;
    FILE * file_stream;
    String local_path;

    Poco::Logger * log = &Poco::Logger::get("RemoteReadBufferCache");
};

/*
 * FIXME:RemoteReadBuffer derive from SeekableReadBufferWithSize may cause some risks, since it's not seekable in some cases
 * But SeekableReadBuffer is not a interface which make it hard to fixup.
 */
class RemoteReadBuffer : public BufferWithOwnMemory<SeekableReadBufferWithSize>
{
public:
    explicit RemoteReadBuffer(size_t buff_size);
    ~RemoteReadBuffer() override;
    static std::unique_ptr<RemoteReadBuffer> create(const RemoteFileMetadata & remote_file_meta, std::unique_ptr<ReadBuffer> read_buffer);

    bool nextImpl() override;
    inline bool seekable() { return file_reader != nullptr && file_reader->getSize() > 0; }
    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;
    std::optional<size_t> getTotalSize() override { return file_reader->getSize(); }

private:
    std::shared_ptr<LocalCachedFileReader> file_reader;
    std::shared_ptr<ReadBuffer> original_read_buffer;
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

    void recoverTask();
    void recoverCachedFilesMetaData(
        const std::filesystem::path & current_path,
        size_t current_depth,
        size_t max_depth);
    bool clearLocalCache();
};

}
