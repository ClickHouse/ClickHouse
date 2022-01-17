#pragma once

#include <IO/AsynchronousReadBufferFromFileDescriptor.h>
#include <IO/OpenedFileCache.h>


namespace DB
{

class AsynchronousReadBufferFromFile : public AsynchronousReadBufferFromFileDescriptor
{
protected:
    std::string file_name;

public:
    explicit AsynchronousReadBufferFromFile(
        AsynchronousReaderPtr reader_,
        Int32 priority_,
        const std::string & file_name_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = -1,
        char * existing_memory = nullptr,
        size_t alignment = 0,
        std::optional<size_t> file_size_ = std::nullopt);

    /// Use pre-opened file descriptor.
    explicit AsynchronousReadBufferFromFile(
        AsynchronousReaderPtr reader_,
        Int32 priority_,
        int & fd, /// Will be set to -1 if constructor didn't throw and ownership of file descriptor is passed to the object.
        const std::string & original_file_name = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0,
        std::optional<size_t> file_size_ = std::nullopt);

    ~AsynchronousReadBufferFromFile() override;

    /// Close file before destruction of object.
    void close();

    std::string getFileName() const override
    {
        return file_name;
    }
};


/** Similar to AsynchronousReadBufferFromFile but also transparently shares open file descriptors.
  */
class AsynchronousReadBufferFromFileWithDescriptorsCache : public AsynchronousReadBufferFromFileDescriptor
{
private:
    std::string file_name;
    OpenedFileCache::OpenedFilePtr file;

public:
    AsynchronousReadBufferFromFileWithDescriptorsCache(
        AsynchronousReaderPtr reader_,
        Int32 priority_,
        const std::string & file_name_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        int flags = -1,
        char * existing_memory = nullptr,
        size_t alignment = 0,
        std::optional<size_t> file_size_ = std::nullopt)
        : AsynchronousReadBufferFromFileDescriptor(std::move(reader_), priority_, -1, buf_size, existing_memory, alignment, file_size_)
        , file_name(file_name_)
    {
        file = OpenedFileCache::instance().get(file_name, flags);
        fd = file->getFD();
    }

    ~AsynchronousReadBufferFromFileWithDescriptorsCache() override;

    std::string getFileName() const override
    {
        return file_name;
    }
};

}

