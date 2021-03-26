#pragma once

#include <IO/ReadBufferFromFileBase.h>
#include <IO/MappedFileCache.h>
#include <IO/MMapReadBufferFromFileDescriptor.h>


namespace DB
{

class MMapReadBufferFromFileWithCache : public ReadBufferFromFileBase
{
public:
    MMapReadBufferFromFileWithCache(const std::string & file_name, size_t offset, size_t length);

    /// Map till end of file.
    MMapReadBufferFromFileWithCache(const std::string & file_name, size_t offset);

    off_t getPosition() override;
    std::string getFileName() const override;
    off_t seek(off_t offset, int whence) override;

private:
    MappedFileCache::MappedPtr mapped;

    void init();
};

}


