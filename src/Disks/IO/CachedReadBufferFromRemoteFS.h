#pragma once

#include <Common/FileCache.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadSettings.h>
#include <base/logger_useful.h>

namespace DB
{

class CachedReadBufferFromRemoteFS : public SeekableReadBuffer
{
public:
    using RemoteFSFileReaderCreator = std::function<FileSegment::RemoteFileReaderPtr()>;

    CachedReadBufferFromRemoteFS(
        const String & path,
        FileCachePtr cache_,
        RemoteFSFileReaderCreator remote_file_reader_creator_,
        const ReadSettings & settings_,
        size_t read_until_position_);

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

    size_t getFileOffsetOfBufferEnd() const override { return file_offset_of_buffer_end; }

    String getInfoForLog() override;

private:
    void initialize(size_t offset, size_t size);

    SeekableReadBufferPtr getReadBufferForFileSegment(FileSegmentPtr file_segment);

    SeekableReadBufferPtr getCacheReadBuffer(size_t offset) const;

    enum class ReadType
    {
        CACHED,
        REMOTE_FS_READ_BYPASS_CACHE,
        REMOTE_FS_READ_AND_PUT_IN_CACHE,
    };

    SeekableReadBufferPtr getRemoteFSReadBuffer(FileSegmentPtr file_segment, ReadType read_type_);

    size_t getTotalSizeToRead();
    bool completeFileSegmentAndGetNext();

    Poco::Logger * log;
    FileCache::Key key;
    String path;
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

    SeekableReadBufferPtr impl;
    bool initialized = false;

    /// Flag to identify usage of threadpool reads
    bool use_external_buffer;

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
    }

    size_t first_offset = 0;
};

}
