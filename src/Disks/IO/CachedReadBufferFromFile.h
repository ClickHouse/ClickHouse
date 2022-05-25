#pragma once

#include <Common/IFileCache.h>
#include <Common/FileSegment.h>
#include <Common/logger_useful.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadSettings.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Interpreters/FilesystemCacheLog.h>


namespace CurrentMetrics
{
extern const Metric FilesystemCacheReadBuffers;
}

namespace DB
{

class CachedReadBufferFromFile : public ReadBufferFromFileBase
{
public:
    using RemoteFSFileReaderCreator = std::function<FileSegment::RemoteFileReaderPtr()>;

    CachedReadBufferFromFile(
        const String & source_file_path_,
        const IFileCache::Key & cache_key_,
        FileCachePtr cache_,
        RemoteFSFileReaderCreator remote_file_reader_creator_,
        const ReadSettings & settings_,
        const String & query_id_,
        size_t file_size_,
        std::optional<size_t> read_until_position_ = std::nullopt);

    ~CachedReadBufferFromFile() override;

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

    size_t getFileOffsetOfBufferEnd() const override { return file_offset_of_buffer_end; }

    String getInfoForLog() override;

    void setReadUntilPosition(size_t position) override;

    String getFileName() const override { return source_file_path; }

    enum class ReadType
    {
        CACHED,
        REMOTE_FS_READ_BYPASS_CACHE,
        REMOTE_FS_READ_AND_PUT_IN_CACHE,
    };

private:
    void initialize(size_t offset, size_t size);

    SeekableReadBufferPtr getImplementationBuffer(FileSegmentPtr & file_segment);

    SeekableReadBufferPtr getReadBufferForFileSegment(FileSegmentPtr & file_segment);

    SeekableReadBufferPtr getCacheReadBuffer(size_t offset) const;

    std::optional<size_t> getLastNonDownloadedOffset() const;

    bool updateImplementationBufferIfNeeded();

    void predownload(FileSegmentPtr & file_segment);

    bool nextImplStep();

    void assertCorrectness() const;

    SeekableReadBufferPtr getRemoteFSReadBuffer(FileSegmentPtr & file_segment, ReadType read_type_);

    size_t getTotalSizeToRead();
    bool completeFileSegmentAndGetNext();

    void appendFilesystemCacheLog(const FileSegment::Range & file_segment_range, ReadType read_type);

    Poco::Logger * log;
    IFileCache::Key cache_key;
    String source_file_path;
    FileCachePtr cache;
    ReadSettings settings;

    size_t read_until_position;
    size_t file_offset_of_buffer_end = 0;
    size_t bytes_to_predownload = 0;

    RemoteFSFileReaderCreator remote_file_reader_creator;

    /// Remote read buffer, which can only be owned by current buffer.
    FileSegment::RemoteFileReaderPtr remote_file_reader;

    std::optional<FileSegmentsHolder> file_segments_holder;
    FileSegments::iterator current_file_segment_it;

    SeekableReadBufferPtr implementation_buffer;
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
        __builtin_unreachable();
    }

    size_t first_offset = 0;
    String nextimpl_step_log_info;
    String last_caller_id;

    bool is_persistent = false;

    String query_id;
    bool enable_logging = false;
    String current_buffer_id;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::FilesystemCacheReadBuffers};
    ProfileEvents::Counters current_file_segment_counters;
};

}
