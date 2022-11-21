#pragma once

#include <Interpreters/Cache/FileCache.h>
#include <Disks/IO/CachedOnDiskWriteBufferFromFile.h>

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
    void reserveCapacity(size_t requested_capacity)
    {
        chassert(used_space <= capacity);

        size_t remaining_space = capacity - used_space;
        if (requested_capacity <= remaining_space)
            return;

        size_t capacity_to_reserve = requested_capacity - remaining_space;
        reserveImpl(capacity_to_reserve);
        capacity += capacity_to_reserve;
    }

    void setUsed(size_t size)
    {
        if (used_space + size > capacity)
        {
            LOG_WARNING(&Poco::Logger::get("ISpacePlaceholder"), "Used space is greater than capacity. It may lead to not enough space error");
            reserveCapacity(used_space + size - capacity);
        }

        used_space = used_space + size;
    }

    virtual ~ISpacePlaceholder() = default;

private:
    virtual void reserveImpl(size_t size) = 0;

    size_t capacity = 0;
    size_t used_space = 0;
};

class FileCachePlaceholder : public ISpacePlaceholder
{
public:
    FileCachePlaceholder(FileCache * cache, const String & name)
        : key(cache->hash(name))
        , directory(cache->getPathInLocalCache(key))
        , cache_writer(cache, key, nullptr, "", name)
    {

    }

    fs::path getDirectoryPath() const
    {
        return directory;
    }

    void reserveImpl(size_t size) override
    {
        while (size > 0)
        {
            size_t current_offset = cache_writer.currentOffset();
            size_t written = cache_writer.tryWrite(nullptr, size, current_offset, /* is_persistent */ false);
            if (written == 0)
                throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Cannot reserve space in file cache ({} bytes required)", size);
            size -= written;
        }
    }

    ~FileCachePlaceholder() override
    {
        try
        {
            cache_writer.finalize(/* clear */ true);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

private:
    FileSegment::Key key;
    fs::path directory;

    FileSegmentRangeWriter cache_writer;
};

}
