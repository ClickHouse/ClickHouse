#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <Common/PageCache.h>

namespace DB
{

class CachedInMemoryReadBufferFromFile : public ReadBufferFromFileBase
{
public:
    /// `in_` must support using external buffer. I.e. we assign its internal_buffer before
    /// each in_->next() call and expect the read data to be put into that buffer.
    /// `in_` should be seekable and should be able to read the whole file from 0 to in_->getFileSize();
    /// in particular, don't call setReadUntilPosition() on `in_` directly, call
    /// CachedInMemoryReadBufferFromFile::setReadUntilPosition().
    CachedInMemoryReadBufferFromFile(PageCacheKey cache_key_, PageCachePtr cache_, std::unique_ptr<ReadBufferFromFileBase> in_, const ReadSettings & settings_);

    String getFileName() const override;
    String getInfoForLog() override;
    bool isSeekCheap() override;

    /// Should we override isContentCached to do a cache lookup? It would save ThreadPoolRemoteFSReader
    /// the overhead of passing the task to another thread and back, but will add overhead of doing
    /// cache lookup twice.
    /// bool isContentCached(size_t offset, size_t size) override;

    off_t seek(off_t off, int whence) override;
    off_t getPosition() override;
    size_t getFileOffsetOfBufferEnd() const override;
    bool supportsRightBoundedReads() const override { return true; }
    void setReadUntilPosition(size_t position) override;
    void setReadUntilEnd() override;

private:
    PageCacheKey cache_key; // .offset is offset of `chunk` start
    PageCachePtr cache;
    size_t block_size;
    size_t lookahead_blocks;
    ReadSettings settings;
    std::unique_ptr<ReadBufferFromFileBase> in;

    size_t file_offset_of_buffer_end = 0;
    size_t read_until_position;
    /// From the latest call to in->setReadUntilPosition.
    size_t inner_read_until_position;

    PageCache::MappedPtr chunk;
    bool last_read_hit_cache = false;

    bool nextImpl() override;
};

}
