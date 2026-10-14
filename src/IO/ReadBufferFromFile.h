#pragma once

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/OpenedFileCache.h>
#include <Common/CurrentMetrics.h>


namespace CurrentMetrics
{
    extern const Metric OpenFileForRead;
}

namespace DB
{

/** Accepts path to file and opens it, or pre-opened file descriptor.
  * Closes file by himself (thus "owns" a file descriptor).
  */
class ReadBufferFromFile : public ReadBufferFromFileDescriptor
{
protected:
    std::string file_name;
    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForRead};

public:
    explicit ReadBufferFromFile(
        const std::string & file_name_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = -1,
        char * existing_memory = nullptr,
        size_t alignment = 0,
        std::optional<size_t> file_size_ = std::nullopt,
        ThrottlerPtr throttler = {});

    /// Use pre-opened file descriptor.
    explicit ReadBufferFromFile(
        int & fd, /// Will be set to -1 if constructor didn't throw and ownership of file descriptor is passed to the object.
        const std::string & original_file_name = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0,
        std::optional<size_t> file_size_ = std::nullopt,
        ThrottlerPtr throttler = {});

    ~ReadBufferFromFile() override;

    /// Close file before destruction of object.
    void close();

    std::string getFileName() const override
    {
        return file_name;
    }

    size_t getFileOffsetOfBufferEnd() const override { return file_offset_of_buffer_end; }

    bool isRegularLocalFile(size_t * /* out_view_offset */) override { return true; }
};


/** Similar to ReadBufferFromFile but it is using 'pread' instead of 'read'.
  */
class ReadBufferFromFilePRead : public ReadBufferFromFile
{
public:
    explicit ReadBufferFromFilePRead(
        const std::string & file_name_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = -1,
        char * existing_memory = nullptr,
        size_t alignment = 0,
        std::optional<size_t> file_size_ = std::nullopt)
        : ReadBufferFromFile(file_name_, buf_size, flags, existing_memory, alignment, file_size_)
    {
        use_pread = true;
    }
};


/** Similar to ReadBufferFromFilePRead but also transparently shares open file descriptors.
  */
class ReadBufferFromFilePReadWithDescriptorsCache : public ReadBufferFromFileDescriptorPRead
{
private:
    std::string file_name;
    OpenedFileCache::OpenedFilePtr file;

public:
    explicit ReadBufferFromFilePReadWithDescriptorsCache(
        const std::string & file_name_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = -1,
        char * existing_memory = nullptr,
        size_t alignment = 0,
        std::optional<size_t> file_size_ = std::nullopt,
        ThrottlerPtr throttler_ = {})
        : ReadBufferFromFileDescriptorPRead(-1, buf_size, existing_memory, alignment, file_size_, throttler_)
        , file_name(file_name_)
    {
        file = OpenedFileCache::instance().get(file_name, flags);
        fd = file->getFD();
    }

    std::string getFileName() const override
    {
        return file_name;
    }

    bool isRegularLocalFile(size_t * /* out_view_offset */) override { return true; }
};

}
