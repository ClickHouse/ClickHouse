#pragma once

#include <IO/PackedFilesIO.h>
#include <IO/ReadSettings.h>
#include <Core/Names.h>
#include <base/types.h>
#include <IO/ReadBuffer.h>

namespace DB
{

class IDisk;
class ReadBufferFromFileBase;
using DiskPtr = std::shared_ptr<IDisk>;

/// Class that allows to read files from archive written by PackedFilesWriter.
class PackedFilesReader
{
public:
    /// Constructor that loads index from disk.
    PackedFilesReader(DiskPtr disk_, const String & data_file_name_, const ReadSettings & read_settings_);

    /// Constructor that initializes index by provided one.
    PackedFilesReader(DiskPtr disk_, const String & data_file_name_, const PackedFilesIO::Index & index_);

    /// Common read operations which return data from index.
    bool exists(const std::string & file_name) const;
    size_t getFileSize(const String & file_name) const;
    Names getFileNames() const;

    /// Returns read buffer to read requested file as a part of archive file.
    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & file_name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint) const;

    const PackedFilesIO::Index & getIndex() const { return index; }

    static PackedFilesIO::Index readIndex(ReadBuffer & in);

private:
    /// Disk of file with archive.
    const DiskPtr disk;
    /// Path of file with archive.
    const String data_file_name;
    /// Index of archive.
    PackedFilesIO::Index index;
};

}
