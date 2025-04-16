#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <Common/PageCache.h>

namespace DB
{

class CachedInMemoryReadBufferFromFile : public ReadBufferFromFileBase
{
public:
    /// `in_` must support using external buffer. I.e. we assign its internal_buffer before each next()
    /// call and expect the read data to be put into that buffer.
    /// `in_` should be seekable and should be able to read the whole file from 0 to in_->getFileSize();
    /// if you set `in_`'s read-until-position bypassing CachedInMemoryReadBufferFromFile then
    /// CachedInMemoryReadBufferFromFile will break.
    CachedInMemoryReadBufferFromFile(FileChunkAddress cache_key_, PageCachePtr cache_, std::unique_ptr<ReadBufferFromFileBase> in_, const ReadSettings & settings_);

    String getFileName() const override;
    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;
    size_t getFileOffsetOfBufferEnd() const override;
    bool supportsRightBoundedReads() const override { return true; }
    void setReadUntilPosition(size_t position) override;
    void setReadUntilEnd() override;

private:
    FileChunkAddress cache_key; // .offset is offset of `chunk` start
    PageCachePtr cache;
    ReadSettings settings;
    std::unique_ptr<ReadBufferFromFileBase> in;

    size_t file_offset_of_buffer_end = 0;
    size_t read_until_position;

    std::optional<PinnedPageChunk> chunk;

    bool nextImpl() override;
};

}
