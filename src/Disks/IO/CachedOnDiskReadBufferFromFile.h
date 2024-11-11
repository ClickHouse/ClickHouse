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
#include <Interpreters/Cache/UserInfo.h>


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
        const FileCacheUserInfo & user_,
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

    String getFileName() const override { return source_file_path; }

    enum class ReadType : uint8_t
    {
        CACHED,
        REMOTE_FS_READ_BYPASS_CACHE,
        REMOTE_FS_READ_AND_PUT_IN_CACHE,
        NONE,
    };

    bool isSeekCheap() override;

    bool isContentCached(size_t offset, size_t size) override;

    bool supportsReadAt() override { return true; }

    size_t readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback) const override;

    using ImplementationBufferPtr = std::shared_ptr<ReadBufferFromFileBase>;
    struct ReadInfo
    {
        ImplementationBufferPtr remote_file_reader;
        ImplementationBufferPtr cache_file_reader;

        const ImplementationBufferCreator implementation_buffer_creator;
        const bool use_external_buffer;
        const ReadSettings settings;

        size_t read_until_position = 0;
        FileSegmentsHolderPtr file_segments;

        ReadInfo(ImplementationBufferCreator impl_creator_, bool use_external_buffer_, const ReadSettings & read_settings_, size_t read_until_position_)
            : implementation_buffer_creator(impl_creator_), use_external_buffer(use_external_buffer_), settings(read_settings_), read_until_position(read_until_position_)
        {
        }

    };

private:

    struct ReadFromFileSegmentState
    {
        ReadType read_type = ReadType::NONE;
        ImplementationBufferPtr buf;
        size_t bytes_to_predownload = 0;
    };
    using ReadFromFileSegmentStatePtr = std::unique_ptr<ReadFromFileSegmentState>;

    void initialize();

    /**
     * Return a list of file segments ordered in ascending order. This list represents
     * a full contiguous interval (without holes).
     */
    FileSegmentsHolderPtr getFileSegments(size_t offset, size_t size) const;

    bool updateImplementationBufferIfNeeded();

    static ReadFromFileSegmentStatePtr prepareReadFromFileSegmentState(
        FileSegment & file_segment,
        size_t offset,
        ReadInfo & info,
        LoggerPtr log);

    static ReadFromFileSegmentStatePtr getReadBufferForFileSegment(
        FileSegment & file_segment,
        size_t offset,
        ReadInfo & info,
        LoggerPtr log);

    static void predownloadForFileSegment(
        FileSegment & file_segment,
        size_t offset,
        ReadFromFileSegmentState & state,
        ReadInfo & info,
        LoggerPtr log);

    static size_t readFromFileSegment(
        FileSegment & file_segment,
        size_t & offset,
        ReadFromFileSegmentState & state,
        ReadInfo & info,
        bool reset_downloader_after_read,
        bool & implementation_buffer_can_be_reused,
        LoggerPtr log);

    bool nextImplStep();

    size_t getRemainingSizeToRead();

    bool completeFileSegmentAndGetNext();

    void appendFilesystemCacheLog(const FileSegment & file_segment, ReadType read_type);

    static bool writeCache(
        char * data,
        size_t size,
        size_t offset,
        FileSegment & file_segment,
        LoggerPtr log);

    static bool canStartFromCache(size_t current_offset, const FileSegment & file_segment);

    bool nextFileSegmentsBatch();

    const LoggerPtr log;
    const FileCacheKey cache_key;
    const String source_file_path;
    const FileCachePtr cache;

    size_t file_offset_of_buffer_end = 0;

    ReadFromFileSegmentStatePtr state;
    ReadInfo info;

    bool initialized = false;

    static String toString(ReadType type);

    size_t first_offset = 0;
    String nextimpl_step_log_info;
    String last_caller_id;

    String query_id;
    String current_buffer_id;
    FileCacheUserInfo user;

    bool allow_seeks_after_first_read;
    [[maybe_unused]]bool use_external_buffer;
    CurrentMetrics::Increment metric_increment{CurrentMetrics::FilesystemCacheReadBuffers};
    ProfileEvents::Counters current_file_segment_counters;

    FileCacheQueryLimit::QueryContextHolderPtr query_context_holder;

    std::shared_ptr<FilesystemCacheLog> cache_log;
};

}
