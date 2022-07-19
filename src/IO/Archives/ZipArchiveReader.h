#pragma once

#include <Common/config.h>

#if USE_MINIZIP
#include <IO/Archives/IArchiveReader.h>
#include <mutex>
#include <vector>


namespace DB
{
class ReadBuffer;
class ReadBufferFromFileBase;
class SeekableReadBuffer;

/// Implementation of IArchiveReader for reading zip archives.
class ZipArchiveReader : public IArchiveReader
{
public:
    /// Constructs an archive's reader that will read from a file in the local filesystem.
    explicit ZipArchiveReader(const String & path_to_archive_);

    /// Constructs an archive's reader that will read by making a read buffer by using
    /// a specified function.
    ZipArchiveReader(const String & path_to_archive_, const ReadArchiveFunction & archive_read_function_, UInt64 archive_size_);

    ~ZipArchiveReader() override;

    /// Returns true if there is a specified file in the archive.
    bool fileExists(const String & filename) override;

    /// Returns the information about a file stored in the archive.
    FileInfo getFileInfo(const String & filename) override;

    /// Starts enumerating files in the archive.
    std::unique_ptr<FileEnumerator> firstFile() override;

    /// Starts reading a file from the archive. The function returns a read buffer,
    /// you can read that buffer to extract uncompressed data from the archive.
    /// Several read buffers can be used at the same time in parallel.
    std::unique_ptr<ReadBufferFromFileBase> readFile(const String & filename) override;

    /// It's possible to convert a file enumerator to a read buffer and vice versa.
    std::unique_ptr<ReadBufferFromFileBase> readFile(std::unique_ptr<FileEnumerator> enumerator) override;
    std::unique_ptr<FileEnumerator> nextFile(std::unique_ptr<ReadBuffer> read_buffer) override;

    /// Sets password used to decrypt the contents of the files in the archive.
    void setPassword(const String & password_) override;

private:
    class ReadBufferFromZipArchive;
    class FileEnumeratorImpl;
    class HandleHolder;
    using RawHandle = void *;

    void init();

    struct FileInfoImpl : public FileInfo
    {
        int compression_method;
    };

    HandleHolder acquireHandle();
    RawHandle acquireRawHandle();
    void releaseRawHandle(RawHandle handle_);

    void checkResult(int code) const;
    [[noreturn]] void showError(const String & message) const;

    const String path_to_archive;
    const ReadArchiveFunction archive_read_function;
    const UInt64 archive_size = 0;
    String password;
    std::vector<RawHandle> free_handles;
    mutable std::mutex mutex;
};

}

#endif
