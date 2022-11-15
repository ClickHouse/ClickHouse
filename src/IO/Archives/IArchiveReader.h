#pragma once

#include <boost/noncopyable.hpp>
#include <base/types.h>
#include <memory>


namespace DB
{
class ReadBuffer;
class ReadBufferFromFileBase;
class SeekableReadBuffer;

/// Interface for reading an archive.
class IArchiveReader : public std::enable_shared_from_this<IArchiveReader>, boost::noncopyable
{
public:
    virtual ~IArchiveReader() = default;

    /// Returns true if there is a specified file in the archive.
    virtual bool fileExists(const String & filename) = 0;

    struct FileInfo
    {
        UInt64 uncompressed_size;
        UInt64 compressed_size;
        bool is_encrypted;
    };

    /// Returns the information about a file stored in the archive.
    virtual FileInfo getFileInfo(const String & filename) = 0;

    class FileEnumerator
    {
    public:
        virtual ~FileEnumerator() = default;
        virtual const String & getFileName() const = 0;
        virtual const FileInfo & getFileInfo() const = 0;
        virtual bool nextFile() = 0;
    };

    /// Starts enumerating files in the archive.
    virtual std::unique_ptr<FileEnumerator> firstFile() = 0;

    /// Starts reading a file from the archive. The function returns a read buffer,
    /// you can read that buffer to extract uncompressed data from the archive.
    /// Several read buffers can be used at the same time in parallel.
    virtual std::unique_ptr<ReadBufferFromFileBase> readFile(const String & filename) = 0;

    /// It's possible to convert a file enumerator to a read buffer and vice versa.
    virtual std::unique_ptr<ReadBufferFromFileBase> readFile(std::unique_ptr<FileEnumerator> enumerator) = 0;
    virtual std::unique_ptr<FileEnumerator> nextFile(std::unique_ptr<ReadBuffer> read_buffer) = 0;

    /// Sets password used to decrypt files in the archive.
    virtual void setPassword(const String & /* password */) {}

    using ReadArchiveFunction = std::function<std::unique_ptr<SeekableReadBuffer>()>;
};

}
