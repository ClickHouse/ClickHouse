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

    off_t getPosition() override;
    std::string getFileName() const override;
    off_t seek(off_t offset, int whence) override;

    bool isRegularLocalFile(size_t * /* out_view_offset */) override { return true; }

    /// mmap has no producer behind `nextImpl` — `working_buffer` already points
    /// at the entire mapped region. After `set(dest, size)` the buffer cannot
    /// refill into `dest`, so callers must use `read(dest, n)` instead, which
    /// memcpys from the mapped region into the caller's buffer.
    bool supportsExternalBufferMode() const override { return false; }

private:
    MMappedFileCache::MappedPtr mapped;

    void init();
};

}
