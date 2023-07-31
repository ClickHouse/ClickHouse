#pragma once

#include "config.h"

#include <IO/Archives/IArchiveReader.h>

#include <iostream>

namespace DB
{

#if USE_LIBARCHIVE

class ReadBuffer;
class ReadBufferFromFileBase;
class SeekableReadBuffer;

/// Implementation of IArchiveReader for reading archives using libarchive.
template <typename ArchiveInfo>
class LibArchiveReader : public IArchiveReader
{
public:
    /// Constructs an archive's reader that will read from a file in the local filesystem.
    explicit LibArchiveReader(const String & path_to_archive_);

    /// Constructs an archive's reader that will read by making a read buffer by using
    /// a specified function.
    LibArchiveReader(const String & path_to_archive_, const ReadArchiveFunction & archive_read_function_);

    ~LibArchiveReader() override;

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
    class ReadBufferFromLibArchive;
    class Handle;

    const String path_to_archive;
    const ReadArchiveFunction archive_read_function;
};

struct TarArchiveInfo { static constexpr std::string_view name = "tar"; };
using TarArchiveReader = LibArchiveReader<TarArchiveInfo>;
struct SevenZipArchiveInfo { static constexpr std::string_view name = "7z"; };
using SevenZipArchiveReader = LibArchiveReader<SevenZipArchiveInfo>;

#endif

}
