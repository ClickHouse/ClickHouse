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

private:
    void initialize(size_t offset, size_t size);
    SeekableReadBufferPtr createReadBuffer(FileSegmentPtr file_segment);
    size_t getTotalSizeToRead();

    Poco::Logger * log;
    FileCache::Key key;
    FileCachePtr cache;
    SeekableReadBufferPtr reader;
    ReadSettings settings;

    size_t read_until_position;
    size_t file_offset_of_buffer_end = 0;

    std::optional<FileSegmentsHolder> file_segments_holder;
    FileSegments::iterator current_file_segment_it;

    std::unique_ptr<WriteBufferFromFile> download_buffer;
    String download_path;

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
};

}
