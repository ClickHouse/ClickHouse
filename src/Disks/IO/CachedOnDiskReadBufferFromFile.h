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

    bool supportsReadAt() override { return true; }

    size_t readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback) const override;

    struct ReadInfo
    {
        ReadInfo(
            const FileCacheKey & cache_key_,
            const std::string & source_file_path_,
            ImplementationBufferCreator impl_creator_,
            bool use_external_buffer_,
            const ReadSettings & read_settings_,
            size_t read_until_position_);

        std::shared_ptr<ReadBufferFromFileBase> remote_file_reader;
        std::shared_ptr<ReadBufferFromFileBase> cache_file_reader;

        const FileCacheKey cache_key;
        const std::string source_file_path;
        const ImplementationBufferCreator implementation_buffer_creator;
        const bool use_external_buffer;
        const ReadSettings settings;

        size_t read_until_position = 0;
        FileSegmentsHolderPtr file_segments;
        ProfileEvents::Counters current_file_segment_counters;
    };

private:
    struct ReadFromFileSegmentState
    {
        ReadType read_type = ReadType::NONE;
        std::shared_ptr<ReadBufferFromFileBase> buf;
        size_t bytes_to_predownload = 0;

        void reset()
        {
            bytes_to_predownload = 0;
            buf = nullptr;
            read_type = ReadType::NONE;
        }
    };
    using ReadFromFileSegmentStatePtr = std::unique_ptr<ReadFromFileSegmentState>;

    void initialize();

    bool updateImplementationBufferIfNeeded();

    static bool canStartFromCache(size_t current_offset, const FileSegment & file_segment);

    static void prepareReadFromFileSegmentState(
        ReadFromFileSegmentState & state,
        FileSegment & file_segment,
        size_t offset,
        ReadInfo & info,
        LoggerPtr log);

    static void getReadBufferForFileSegment(
        ReadFromFileSegmentState & state,
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

    static bool writeCache(
        char * data,
        size_t size,
        size_t offset,
        FileSegment & file_segment,
        ReadInfo & info,
        LoggerPtr log);

    static std::string getInfoForLog(
        const ReadFromFileSegmentState * state,
        const ReadInfo & info,
        size_t offset);

    bool nextImplStep();

    size_t getRemainingSizeToRead();

    bool nextFileSegmentsBatch();

    bool completeFileSegmentAndGetNext();

    void appendFilesystemCacheLog(const FileSegment & file_segment, ReadType read_type);

    const LoggerPtr log;
    const FileCachePtr cache;
    const String query_id;
    const FileCacheUserInfo user;
    const String current_buffer_id;
    const bool allow_seeks_after_first_read;
    const bool use_external_buffer;
    const std::shared_ptr<FilesystemCacheLog> cache_log;
    const FileCacheQueryLimit::QueryContextHolderPtr query_context_holder;

    bool initialized = false;
    size_t file_offset_of_buffer_end = 0;

    ReadFromFileSegmentState state;
    ReadInfo info;

    size_t first_offset = 0;
    String nextimpl_step_log_info;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::FilesystemCacheReadBuffers};
};

}
