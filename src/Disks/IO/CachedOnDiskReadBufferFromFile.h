#pragma once

#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/FileCache_fwd.h>
#include <Interpreters/Cache/QueryLimit.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadSettings.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Interpreters/FilesystemCacheLog.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/FileCacheOriginInfo.h>
#include <IO/SwapHelper.h>


namespace CurrentMetrics
{
extern const Metric FilesystemCacheReadBuffers;
}

namespace DB
{

class CachedOnDiskReadBufferFromFile : public ReadBufferFromFileBase
{
public:
    using ImplementationBufferCreator = std::function<std::unique_ptr<ReadBufferFromFileBase>()>;

    CachedOnDiskReadBufferFromFile(
        const String & source_file_path_,
        const FileCacheKey & cache_key_,
        FileCachePtr cache_,
        const FileCacheOriginInfo & origin_,
        ImplementationBufferCreator implementation_buffer_creator_,
        const ReadSettings & settings_,
        const String & query_id_,
        size_t file_size_,
        bool allow_seeks_after_first_read_,
        bool use_external_buffer_,
        std::optional<size_t> read_until_position_,
        std::shared_ptr<FilesystemCacheLog> cache_log_);

    ~CachedOnDiskReadBufferFromFile() override;

    bool isCached() const override { return true; }

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

    size_t getFileOffsetOfBufferEnd() const override { return file_offset_of_buffer_end; }

    String getInfoForLog() override;

    void setReadUntilPosition(size_t position) override;

    void setReadUntilEnd() override;

    String getFileName() const override { return info.source_file_path; }

    enum class ReadType : uint8_t
    {
        CACHED,
        REMOTE_FS_READ_BYPASS_CACHE,
        REMOTE_FS_READ_AND_PUT_IN_CACHE,
        NONE,
    };

    bool isSeekCheap() override;

    bool isContentCached(size_t offset, size_t size) override;

    std::optional<size_t> tryGetFileSize() override;

    size_t getFileSize();

    bool supportsReadAt() override { return true; }

    size_t readBigAt(
        char * to,
        size_t n,
        size_t range_begin,
        const std::function<bool(size_t)> & progress_callback) const override;

    /// A read info for reading object storage file.
    struct ReadInfo
    {
        ReadInfo(
            const FileCacheKey & cache_key_,
            const std::string & source_file_path_,
            ImplementationBufferCreator impl_creator_,
            bool use_external_buffer_,
            const ReadSettings & read_settings_,
            size_t read_until_position_);

        /// The readers can be reused among different ReadFromFileSegmentState
        /// objects, therefore they are stored here.
        std::shared_ptr<ReadBufferFromFileBase> remote_file_reader;
        std::shared_ptr<ReadBufferFromFileBase> cache_file_reader;

        /// Cache key is a hash from object path.
        const FileCacheKey cache_key;
        /// Object path.
        const std::string source_file_path;
        /// Creates object storage file reader.
        const ImplementationBufferCreator implementation_buffer_creator;
        /// Whether buffer will be passed "externally",
        /// e.g. current buffer does not need to allocate its own memory.
        const bool use_external_buffer;
        /// Query read settings.
        const ReadSettings settings;

        /// Non-included range end offset.
        size_t read_until_position = 0;
        /// List of file segments which we need to read
        /// given initial [start_offset, read_until_position).
        FileSegmentsHolderPtr file_segments;

        void reset();
    };

private:
    /// A state for reading from a single file segment
    /// (part of object storage file).
    struct ReadFromFileSegmentState
    {
        ReadFromFileSegmentState(
            std::shared_ptr<ReadBufferFromFileBase> buf_,
            ReadType read_type_,
            size_t bytes_to_predownload_ = 0)
            : read_type(read_type_)
            , buf(buf_)
            , bytes_to_predownload(bytes_to_predownload_)
        {
        }

        /// Read type: CACHED, REMOTE_FS_READ_BYPASS_CACHE, REMOTE_FS_READ_AND_PUT_IN_CACHE.
        ReadType read_type = ReadType::NONE;
        /// Read buffer which either reads from local file (from cache)
        /// or from remote object storage.
        std::shared_ptr<ReadBufferFromFileBase> buf;
        /// "Predownload" bytes, e.g. the extra amount of bytes
        /// which we need to read before our current read offset
        /// either because of offset alignment
        /// or because of file segment, that we need, is partially downloaded.
        size_t bytes_to_predownload = 0;
        /// A predownload memory, because if buffer size is too small (less than 1mb)
        /// then predownload is suboptimal.
        Memory<> predownload_memory;
    };
    using ReadFromFileSegmentStatePtr = std::unique_ptr<ReadFromFileSegmentState>;

    void initialize();

    bool updateImplementationBufferIfNeeded();

    static void updateReadStateIfNeeded(
        FileSegment & file_segment,
        size_t offset,
        ReadFromFileSegmentStatePtr & state,
        ReadInfo & info,
        size_t file_size_,
        LoggerPtr log);

    static bool canStartFromCache(size_t current_offset, const FileSegment & file_segment);

    static ReadFromFileSegmentStatePtr createReadFromFileSegmentState(
        FileSegment & file_segment,
        size_t offset,
        ReadInfo & info,
        LoggerPtr log);

    static ReadFromFileSegmentStatePtr prepareReadFromFileSegmentState(
        FileSegment & file_segment,
        size_t offset,
        ReadInfo & info,
        size_t file_size_,
        LoggerPtr log);

    static bool predownloadForFileSegment(
        FileSegment & file_segment,
        size_t offset,
        ReadFromFileSegmentState & state,
        ReadInfo & info,
        LoggerPtr log);

    static size_t readFromFileSegment(
        FileSegment & file_segment,
        size_t offset,
        size_t file_size_,
        ReadFromFileSegmentState & state,
        ReadInfo & info,
        bool & implementation_buffer_can_be_reused,
        LoggerPtr log);

    static bool writeCache(
        char * data,
        size_t size,
        size_t offset,
        FileSegment & file_segment,
        LoggerPtr log);

    static std::string getInfoForLog(
        const ReadFromFileSegmentState * state,
        const ReadInfo & info,
        size_t offset);

    bool predownload(FileSegment & file_segment);

    bool nextImplStep();

    size_t getRemainingSizeToRead();

    bool nextFileSegmentsBatch();

    bool completeFileSegmentAndGetNext();

    void appendFilesystemCacheLog(const FileSegment & file_segment, ReadType read_type);

    static String toString(ReadType type);

    const LoggerPtr log;
    const FileCachePtr cache;
    const String query_id;
    const FileCacheOriginInfo origin;
    const String current_buffer_id;
    const bool allow_seeks_after_first_read;
    const bool use_external_buffer;
    const std::shared_ptr<FilesystemCacheLog> cache_log;
    const FileCacheQueryLimit::QueryContextHolderPtr query_context_holder;

    bool initialized = false;
    size_t file_offset_of_buffer_end = 0;

    ReadFromFileSegmentStatePtr state;
    ReadInfo info;

    size_t first_offset = 0;
    String nextimpl_step_log_info;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::FilesystemCacheReadBuffers};
};

}
