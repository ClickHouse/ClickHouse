#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <IO/MMappedFileCache.h>
#include <IO/MMapReadBufferFromFileDescriptor.h>


namespace DB
{

class MMapReadBufferFromFileWithCache : public ReadBufferFromFileBase
{
public:
    MMapReadBufferFromFileWithCache(MMappedFileCache & cache, const std::string & file_name, size_t offset, size_t length);

    /// Map till end of file.
    MMapReadBufferFromFileWithCache(MMappedFileCache & cache, const std::string & file_name, size_t offset);

    ~MMapReadBufferFromFileWithCache() override;

    off_t getPosition() override;
    std::string getFileName() const override;
    off_t seek(off_t offset, int whence) override;

    /// Track memory with MemoryTracker.
    ///
    /// NOTE: that this is not the size of mmap() region, but estimated size of
    /// data that will be read (see MergeTreeReaderStream).
    /// And from one point of view it should not be accounted here, since the
    /// kernel may unmap physical pages, but in practice firstly it will mmap it,
    /// RSS will grow, total_memory_tracker will be synced with RSS and the
    /// allocations will start to fail.
    void track(size_t bytes_);

private:
    MMappedFileCache::MappedPtr mapped;

    size_t tracked_bytes = 0;

    void init();
};

}
