#pragma once
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <Core/BackgroundSchedulePool.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Storages/Cache/IRemoteFileMetadata.h>
#include <base/logger_useful.h>
#include <Poco/Logger.h>
#include <Common/ErrorCodes.h>

namespace DB
{
class RemoteCacheController
{
public:
    enum LocalFileStatus
    {
        TO_DOWNLOAD = 0,
        DOWNLOADING = 1,
        DOWNLOADED = 2,
    };

    RemoteCacheController(
        IRemoteFileMetadataPtr file_metadata_, const std::filesystem::path & local_path_, size_t cache_bytes_before_flush_);
    ~RemoteCacheController();

    // Recover from local disk.
    static std::shared_ptr<RemoteCacheController> recover(const std::filesystem::path & local_path);

    /**
     * Called by LocalCachedFileReader, must be used in pair
     * The second value of the return tuple is the local_path to store file.
     */
    std::unique_ptr<ReadBufferFromFileBase> allocFile();
    void close();

    /**
     * Called in LocalCachedFileReader read(), the reading process would be blocked until
     * enough data be downloaded.
     * If the file has finished download, the process would unblocked.
     */
    void waitMoreData(size_t start_offset_, size_t end_offset_);

    inline size_t size() const { return current_offset; }

    inline const std::filesystem::path & getLocalPath() { return local_path; }
    inline String getRemotePath() const { return file_metadata_ptr->remote_path; }

    inline UInt64 getLastModificationTimestamp() const { return file_metadata_ptr->last_modification_timestamp; }
    bool isModified(IRemoteFileMetadataPtr file_metadata_);
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
    IRemoteFileMetadataPtr getFileMetadata() { return file_metadata_ptr; }
    inline size_t getFileSize() const { return file_metadata_ptr->file_size; }

    void startBackgroundDownload(std::unique_ptr<ReadBuffer> in_readbuffer_, BackgroundSchedulePool & thread_pool);

private:
    // Flush file and status information.
    void flush(bool need_flush_status = false);

    BackgroundSchedulePool::TaskHolder download_task_holder;
    void backgroundDownload(ReadBufferPtr remote_read_buffer);

    std::mutex mutex;
    std::condition_variable more_data_signal;

    String metadata_class;
    LocalFileStatus file_status = TO_DOWNLOAD; // For tracking download process.
    IRemoteFileMetadataPtr file_metadata_ptr;
    std::filesystem::path local_path;

    bool valid = true;
    size_t local_cache_bytes_read_before_flush;
    size_t current_offset;

    //std::shared_ptr<ReadBuffer> remote_read_buffer;
    std::unique_ptr<WriteBufferFromFileBase> data_file_writer;

    Poco::Logger * log = &Poco::Logger::get("RemoteCacheController");
};
using RemoteCacheControllerPtr = std::shared_ptr<RemoteCacheController>;

}
