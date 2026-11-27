#pragma once

#include <mutex>
#include <IO/Archives/IArchiveReader.h>
#include "config.h"


namespace DB
{

#if USE_LIBARCHIVE

class ReadBuffer;
class ReadBufferFromFileBase;
class SeekableReadBuffer;

/// Implementation of IArchiveReader for reading archives using libarchive.
class LibArchiveReader : public IArchiveReader
{
public:
    ~LibArchiveReader() override;

    const std::string & getPath() const override;

    /// Returns true if there is a specified file in the archive.
    bool fileExists(const String & filename) override;

    /// Returns the information about a file stored in the archive.
    FileInfo getFileInfo(const String & filename) override;

    /// Starts enumerating files in the archive.
    std::unique_ptr<FileEnumerator> firstFile() override;

    /// Starts reading a file from the archive. The function returns a read buffer,
    /// you can read that buffer to extract uncompressed data from the archive.
    /// Several read buffers can be used at the same time in parallel.
    std::unique_ptr<ReadBufferFromFileBase> readFile(const String & filename, bool throw_on_not_found) override;
    std::unique_ptr<ReadBufferFromFileBase> readFile(NameFilter filter, bool throw_on_not_found) override;

    /// It's possible to convert a file enumerator to a read buffer and vice versa.
    std::unique_ptr<ReadBufferFromFileBase> readFile(std::unique_ptr<FileEnumerator> enumerator) override;
    std::unique_ptr<FileEnumerator> nextFile(std::unique_ptr<ReadBuffer> read_buffer) override;
    std::unique_ptr<FileEnumerator> currentFile(std::unique_ptr<ReadBuffer> read_buffer) override;

    std::vector<std::string> getAllFiles() override;
    std::vector<std::string> getAllFiles(NameFilter filter) override;

    /// Sets password used to decrypt the contents of the files in the archive.
    void setPassword(const String & password_) override;

protected:
    /// Constructs an archive's reader that will read from a file in the local filesystem.
    LibArchiveReader(std::string archive_name_, bool lock_on_reading_, std::string path_to_archive_);

    LibArchiveReader(
        std::string archive_name_, bool lock_on_reading_, std::string path_to_archive_, const ReadArchiveFunction & archive_read_function_);

private:
    class ReadBufferFromLibArchive;
    class Handle;
    class FileEnumeratorImpl;
    class StreamInfo;

    Handle acquireHandle();

    const std::string archive_name;
    const bool lock_on_reading;
    const String path_to_archive;
    const ReadArchiveFunction archive_read_function;
    mutable std::mutex mutex;
};

class TarArchiveReader : public LibArchiveReader
{
public:
    explicit TarArchiveReader(std::string path_to_archive) : LibArchiveReader("tar", /*lock_on_reading_=*/true, std::move(path_to_archive))
    {
    }

    explicit TarArchiveReader(std::string path_to_archive, const ReadArchiveFunction & archive_read_function)
        : LibArchiveReader("tar", /*lock_on_reading_=*/true, std::move(path_to_archive), archive_read_function)
    {
    }
};

class SevenZipArchiveReader : public LibArchiveReader
{
public:
    explicit SevenZipArchiveReader(std::string path_to_archive)
        : LibArchiveReader("7z", /*lock_on_reading_=*/false, std::move(path_to_archive))
    {
    }
};

#endif

}
