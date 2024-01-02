#pragma once

#include <Interpreters/Cache/FileCache.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadSettings.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Interpreters/FilesystemCacheLog.h>
#include <Interpreters/Cache/FileSegment.h>


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
        const FileCache::Key & cache_key_,
        FileCachePtr cache_,
        ImplementationBufferCreator implementation_buffer_creator_,
        const ReadSettings & settings_,
        const String & query_id_,
        size_t file_size_,
        bool allow_seeks_after_first_read_,
        bool use_external_buffer_,
        std::optional<size_t> read_until_position_,
        std::shared_ptr<FilesystemCacheLog> cache_log_);

    ~CachedOnDiskReadBufferFromFile() override;

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

    size_t getFileOffsetOfBufferEnd() const override { return file_offset_of_buffer_end; }

    String getInfoForLog() override;

    void setReadUntilPosition(size_t position) override;

    void setReadUntilEnd() override;

    String getFileName() const override { return source_file_path; }

    enum class ReadType
    {
        CACHED,
        REMOTE_FS_READ_BYPASS_CACHE,
        REMOTE_FS_READ_AND_PUT_IN_CACHE,
    };

private:
    using ImplementationBufferPtr = std::shared_ptr<ReadBufferFromFileBase>;

    void initialize(size_t offset, size_t size);
    void assertCorrectness() const;

    /**
     * Return a list of file segments ordered in ascending order. This list represents
     * a full contiguous interval (without holes).
     */
    FileSegmentsHolderPtr getFileSegments(size_t offset, size_t size) const;

    ImplementationBufferPtr getImplementationBuffer(FileSegment & file_segment);

    ImplementationBufferPtr getReadBufferForFileSegment(FileSegment & file_segment);

    ImplementationBufferPtr getCacheReadBuffer(const FileSegment & file_segment);

    ImplementationBufferPtr getRemoteReadBuffer(FileSegment & file_segment, ReadType read_type_);

    bool updateImplementationBufferIfNeeded();

    void predownload(FileSegment & file_segment);

    bool nextImplStep();

    size_t getTotalSizeToRead();

    bool completeFileSegmentAndGetNext();

    void appendFilesystemCacheLog(const FileSegment & file_segment, ReadType read_type);

    bool writeCache(char * data, size_t size, size_t offset, FileSegment & file_segment);

    static bool canStartFromCache(size_t current_offset, const FileSegment & file_segment);

    Poco::Logger * log;
    FileCache::Key cache_key;
    String source_file_path;

    FileCachePtr cache;
    ReadSettings settings;

    size_t read_until_position;
    size_t file_offset_of_buffer_end = 0;
    size_t bytes_to_predownload = 0;

    ImplementationBufferCreator implementation_buffer_creator;

    /// Remote read buffer, which can only be owned by current buffer.
    ImplementationBufferPtr remote_file_reader;
    ImplementationBufferPtr cache_file_reader;

    FileSegmentsHolderPtr file_segments;

    ImplementationBufferPtr implementation_buffer;
    bool initialized = false;

    ReadType read_type = ReadType::REMOTE_FS_READ_BYPASS_CACHE;

    static String toString(ReadType type)
    {
        switch (type)
        {
            case ReadType::CACHED:
                return "CACHED";
            case ReadType::REMOTE_FS_READ_BYPASS_CACHE:
                return "REMOTE_FS_READ_BYPASS_CACHE";
            case ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE:
                return "REMOTE_FS_READ_AND_PUT_IN_CACHE";
        }
        UNREACHABLE();
    }

    size_t first_offset = 0;
    String nextimpl_step_log_info;
    String last_caller_id;

    String query_id;
    String current_buffer_id;

    bool allow_seeks_after_first_read;
    [[maybe_unused]]bool use_external_buffer;
    CurrentMetrics::Increment metric_increment{CurrentMetrics::FilesystemCacheReadBuffers};
    ProfileEvents::Counters current_file_segment_counters;

    FileCache::QueryContextHolderPtr query_context_holder;

    std::shared_ptr<FilesystemCacheLog> cache_log;
};

}
