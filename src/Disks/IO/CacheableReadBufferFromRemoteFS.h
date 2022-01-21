#pragma once

#include <Common/FileCache.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadSettings.h>
#include <base/logger_useful.h>

namespace DB
{

class CacheableReadBufferFromRemoteFS : public SeekableReadBuffer
{
public:
    CacheableReadBufferFromRemoteFS(
        const String & path,
        FileCachePtr cache_,
        SeekableReadBufferPtr reader_,
        const ReadSettings & settings_,
        size_t read_until_position_);

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

    ~CacheableReadBufferFromRemoteFS() override;

private:
    void initialize(size_t offset, size_t size);

    SeekableReadBufferPtr createCacheReadBuffer(size_t offset) const;
    SeekableReadBufferPtr createReadBuffer(FileSegmentPtr file_segment);

    size_t getTotalSizeToRead();
    void completeFileSegmentAndGetNext();

    Poco::Logger * log;
    FileCache::Key key;
    FileCachePtr cache;
    SeekableReadBufferPtr reader;
    ReadSettings settings;

    size_t read_until_position;
    size_t file_offset_of_buffer_end = 0;

    String query_id;

    std::optional<FileSegmentsHolder> file_segments_holder;
    FileSegments::iterator current_file_segment_it;

    SeekableReadBufferPtr impl;
    bool initialized = false;
    bool download_current_segment = false;

    enum class ReadType
    {
        CACHE,
        REMOTE_FS_READ,
        REMOTE_FS_READ_AND_DOWNLOAD,
    };

    ReadType read_type = ReadType::REMOTE_FS_READ;

    static String toString(ReadType type)
    {
        switch (type)
        {
            case ReadType::CACHE:
                return "CACHE";
            case ReadType::REMOTE_FS_READ:
                return "REMOTE_FS_READ";
            case ReadType::REMOTE_FS_READ_AND_DOWNLOAD:
                return "REMOTE_FS_READ_AND_DOWNLOAD";
        }
    }
};

}
