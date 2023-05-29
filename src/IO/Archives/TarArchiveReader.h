#pragma once

#include <IO/Archives/IArchiveReader.h>

#include <archive.h>
#include <archive_entry.h>


namespace DB
{
class ReadBuffer;
class ReadBufferFromFileBase;
class SeekableReadBuffer;

/// Implementation of IArchiveReader for reading tar archives.
class TarArchiveReader : public IArchiveReader
{
public:
    /// Constructs an archive's reader that will read from a file in the local filesystem.
    explicit TarArchiveReader(const String & path_to_archive_);

    /// Constructs an archive's reader that will read by making a read buffer by using
    /// a specified function.
    TarArchiveReader(const String & path_to_archive_, const ReadArchiveFunction & archive_read_function_, UInt64 archive_size_);

    ~TarArchiveReader() override;

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
    [[maybe_unused]] std::unique_ptr<ReadBufferFromFileBase> readFile(std::unique_ptr<FileEnumerator> enumerator) override;
    [[maybe_unused]] std::unique_ptr<FileEnumerator> nextFile(std::unique_ptr<ReadBuffer> read_buffer) override;

    /// Sets password used to decrypt the contents of the files in the archive.
    void setPassword([[maybe_unused]] const String & password_) override;

private:

    class ReadBufferFromTarArchive;
    class Handle;

    const String path_to_archive;
    const ReadArchiveFunction archive_read_function;
    [[maybe_unused]] const UInt64 archive_size = 0;
};

}
