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
enum class RemoteReadBufferCacheError :int8_t
{
    OK,
    NOT_INIT = 10,
    DISK_FULL = 11,
    FILE_INVALID = 12,

    END_OF_FILE = 20,
};

struct RemoteFileMeta
{
    RemoteFileMeta(
            const std::string  & schema_,
            const std::string & cluster_,
            const std::string & path_,
            UInt64 last_modification_timestamp_,
            size_t file_size_):
        schema(schema_),
        cluster(cluster_),
        path(path_),
        last_modification_timestamp(last_modification_timestamp_),
        file_size(file_size_)
    {}

    std::string schema; // Hive, S2 etc.
    std::string cluster;
    std::string path;
    UInt64 last_modification_timestamp;
    size_t file_size;
};

/**
 *
 */
class RemoteCacheController
{
public:
    RemoteCacheController(
        const RemoteFileMeta &meta,
        const std::filesystem::path & local_path_,
        size_t cache_bytes_before_flush_,
        std::shared_ptr<ReadBuffer> readbuffer_,
        std::function<void(RemoteCacheController *)> const & finish_callback);
    ~RemoteCacheController(); // the local files will be deleted in descontructor

    // recover from local disk
    static std::shared_ptr<RemoteCacheController>
    recover(const std::filesystem::path & local_path, std::function<void(RemoteCacheController *)> const & finish_callback);

    /**
     * Called by LocalCachedFileReader, must be used in pair
     * The second value of the return tuple is the local_path to store file.
     * It will be empty if the file has not been downloaded
     */
    std::tuple<FILE *, std::filesystem::path> allocFile();
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
     * If the file has finished download, the process would be block
     */
    RemoteReadBufferCacheError waitMoreData(size_t start_offset_, size_t end_offset_);

    inline size_t size() const { return current_offset; }

    inline const std::filesystem::path & getLocalPath() { return local_path; }
    inline const std::string & getRemotePath() { return remote_path; }

    inline UInt64 getLastModificationTimestamp() const { return last_modification_timestamp; }
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

    void backgroupDownload(std::function<void(RemoteCacheController *)> const & finish_callback);

    std::mutex mutex;
    std::condition_variable more_data_signal;
    std::shared_ptr<ThreadPool> download_thread;

    std::set<FILE *> opened_file_streams;

    // meta info
    bool download_finished;
    bool valid;
    size_t current_offset;
    UInt64 last_modification_timestamp;
    std::filesystem::path local_path;
    std::string remote_path;
    std::string schema;
    std::string cluster;

    size_t local_cache_bytes_read_before_flush;
    std::shared_ptr<ReadBuffer> remote_readbuffer;
    std::unique_ptr<std::ofstream> out_file;
};

/**
 * access local cached files by RemoteCacheController, and be used in RemoteReadBuffer
 */
class LocalCachedFileReader
{
public:
    LocalCachedFileReader(RemoteCacheController * cntrl_, size_t size_);
    ~LocalCachedFileReader();

    // expect to read size bytes into buf, return is the real bytes read
    size_t read(char * buf, size_t size);
    inline off_t getOffset() const { return static_cast<off_t>(offset); }
    size_t size();
    off_t seek(off_t offset);
    inline std::string getPath() { return local_path; }

private:
    std::mutex mutex;
    size_t offset;
    size_t file_size;
    FILE * fs;
    std::filesystem::path local_path;
    RemoteCacheController * controller;
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
    static std::unique_ptr<RemoteReadBuffer> create(
        const RemoteFileMeta &remote_file_meta_,
        std::unique_ptr<ReadBuffer> readbuffer);

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
protected:
    RemoteReadBufferCache();

public:
    ~RemoteReadBufferCache();
    // global instance
    static RemoteReadBufferCache & instance();

    void initOnce(const std::filesystem::path & dir, size_t limit_size, size_t bytes_read_before_flush_);
    inline bool hasInitialized() const { return inited; }

    std::tuple<std::shared_ptr<LocalCachedFileReader>, RemoteReadBufferCacheError> createReader(
        const RemoteFileMeta &remote_file_meta,
        std::shared_ptr<ReadBuffer> & readbuffer);

private:
    std::string local_path_prefix;

    std::atomic<bool> inited = false;
    std::mutex mutex;
    size_t limit_size = 0;
    size_t local_cache_bytes_read_before_flush = 0;
    std::atomic<size_t> total_size;
    Poco::Logger * log = &Poco::Logger::get("RemoteReadBufferCache");

    struct CacheCell
    {
        std::list<std::string>::iterator key_iterator;
        std::shared_ptr<RemoteCacheController> cache_controller;
    };
    std::list<std::string> keys;
    std::map<std::string, CacheCell> caches;

    std::filesystem::path calculateLocalPath(const RemoteFileMeta &meta);

    void recoverCachedFilesMeta(
            const std::filesystem::path & current_path_,
            size_t current_depth,
            size_t max_depth,
            std::function<void(RemoteCacheController *)> const & finish_callback);
    bool clearLocalCache();
};

}
