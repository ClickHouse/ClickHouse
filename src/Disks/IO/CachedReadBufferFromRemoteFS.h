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
    CachedReadBufferFromRemoteFS(
        const String & path,
        FileCachePtr cache_,
        SeekableReadBufferPtr downloader_,
        const ReadSettings & settings_,
        size_t read_until_position_);

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

private:
    void initialize(size_t offset, size_t size);

    SeekableReadBufferPtr createCacheReadBuffer(size_t offset) const;
    SeekableReadBufferPtr createReadBuffer(FileSegmentPtr file_segment);

    size_t getTotalSizeToRead();
    bool completeFileSegmentAndGetNext();
    void checkForPartialDownload();

    Poco::Logger * log;
    FileCache::Key key;
    FileCachePtr cache;
    SeekableReadBufferPtr downloader;
    ReadSettings settings;

    size_t read_until_position;
    size_t file_offset_of_buffer_end = 0;
    size_t bytes_to_predownload = 0;

    std::optional<FileSegmentsHolder> file_segments_holder;
    FileSegments::iterator current_file_segment_it;

    SeekableReadBufferPtr impl;
    bool initialized = false;

    /// Flag to identify usage of threadpool reads
    bool use_external_buffer;

    enum class ReadType
    {
        CACHED,
        REMOTE_FS_READ,
        REMOTE_FS_READ_AND_DOWNLOAD,
    };

    ReadType read_type = ReadType::REMOTE_FS_READ;

    static String toString(ReadType type)
    {
        switch (type)
        {
            case ReadType::CACHED:
                return "CACHED";
            case ReadType::REMOTE_FS_READ:
                return "REMOTE_FS_READ";
            case ReadType::REMOTE_FS_READ_AND_DOWNLOAD:
                return "REMOTE_FS_READ_AND_DOWNLOAD";
        }
    }
};

}
