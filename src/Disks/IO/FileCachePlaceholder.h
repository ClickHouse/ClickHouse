#pragma once

#include <Interpreters/Cache/FileCache.h>
#include <Disks/IO/CachedOnDiskWriteBufferFromFile.h>

#include <Poco/Logger.h>
#include <Poco/ConsoleChannel.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{


/* ISpacePlaceholder is a base class for all classes that need to reserve space in some storage.
 * You should resrve space with call reserveCapacity() before writing to it.
 * After writing you should call setUsed() to let ISpacePlaceholder know how much space was used.
 * It can be different because in some cases you don't know exact size of data you will write (because of compression, for example).
 * It's better to reserve more space in advance not to overuse space.
 */
class ISpacePlaceholder
{
public:
    /// Reserve space in storage
    void reserveCapacity(size_t requested_capacity);

    /// Indicate that some space is used
    /// It uses reserved space if it is possible, otherwise it reserves more space
    void setUsed(size_t size);

    virtual ~ISpacePlaceholder() = default;

private:
    virtual void reserveImpl(size_t size) = 0;

    size_t capacity = 0;
    size_t used_space = 0;
};

/* FileCachePlaceholder is a class that reserves space in FileCache.
 * Data is written externally, and FileCachePlaceholder is only used to hold space in FileCache.
 */
class FileCachePlaceholder : public ISpacePlaceholder
{
public:
    FileCachePlaceholder(FileCache * cache, const String & name);

    void reserveImpl(size_t requested_size) override;

private:
    std::string key_name;
    FileCache * file_cache;

    /// On each reserveImpl() call we create new FileSegmentRangeWriter that would be hold space
    /// It's required to easily release already reserved space on unsuccessful attempt
    std::vector<std::unique_ptr<FileSegmentRangeWriter>> cache_writers;
};

}
