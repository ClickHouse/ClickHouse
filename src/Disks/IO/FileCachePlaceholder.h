#pragma once

#include <Interpreters/Cache/FileCache.h>
#include <Disks/IO/CachedOnDiskWriteBufferFromFile.h>

#include <Poco/Logger.h>
#include <Poco/ConsoleChannel.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_ENOUGH_SPACE;
}

class ISpacePlaceholder
{
public:
    void reserveCapacity(size_t requested_capacity);
    void setUsed(size_t size);

    virtual ~ISpacePlaceholder() = default;

private:
    virtual void reserveImpl(size_t size) = 0;

    size_t capacity = 0;
    size_t used_space = 0;
};


class FileCachePlaceholder : public ISpacePlaceholder
{
public:
    FileCachePlaceholder(FileCache * cache, const String & name);

    void reserveImpl(size_t requested_size) override;

    ~FileCachePlaceholder() override;

private:
    std::string key_name;
    FileCache * file_cache;

    std::vector<std::unique_ptr<FileSegmentRangeWriter>> cache_writers;
};

}
