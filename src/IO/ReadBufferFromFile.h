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
    explicit ReadBufferFromFile(const std::string & file_name_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE, int flags = -1,
        char * existing_memory = nullptr, size_t alignment = 0);

    /// Use pre-opened file descriptor.
    explicit ReadBufferFromFile(
        int & fd, /// Will be set to -1 if constructor didn't throw and ownership of file descriptor is passed to the object.
        const std::string & original_file_name = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr, size_t alignment = 0);

    ~ReadBufferFromFile() override;

    /// Close file before destruction of object.
    void close();

    std::string getFileName() const override
    {
        return file_name;
    }
};


/** Similar to ReadBufferFromFile but it is using 'pread' instead of 'read'.
  */
class ReadBufferFromFilePRead : public ReadBufferFromFile
{
public:
    ReadBufferFromFilePRead(const std::string & file_name_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE, int flags = -1,
        char * existing_memory = nullptr, size_t alignment = 0)
        : ReadBufferFromFile(file_name_, buf_size, flags, existing_memory, alignment)
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
    ReadBufferFromFilePReadWithDescriptorsCache(const std::string & file_name_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE, int flags = -1,
        char * existing_memory = nullptr, size_t alignment = 0)
        : ReadBufferFromFileDescriptorPRead(-1, buf_size, existing_memory, alignment),
        file_name(file_name_)
    {
        file = OpenedFileCache::instance().get(file_name, flags);
        fd = file->getFD();
    }

    std::string getFileName() const override
    {
        return file_name;
    }
};

}
