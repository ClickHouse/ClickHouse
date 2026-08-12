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
///
/// Holds only the archive index (file name -> offset+size within the archive). The index is
/// path-independent: it stays valid when the part is renamed or moved, because the offsets inside
/// the archive do not change. The disk and the archive's current path are supplied by the caller
/// to readFile, never cached here, so a concurrent rename cannot leave the reader pointing at a
/// stale location, and the reader can be shared without locking.
class PackedFilesReader
{
public:
    /// Constructor that loads the index from an archive file on disk. The disk and path are used
    /// only to read the index here; they are not retained.
    PackedFilesReader(const DiskPtr & disk, const String & data_file_name, const ReadSettings & read_settings);

    /// Constructor that initializes the index by the provided one.
    explicit PackedFilesReader(PackedFilesIO::Index index_);

    /// Common read operations which return data from index.
    bool exists(const std::string & file_name) const;
    size_t getFileSize(const String & file_name) const;
    /// Recorded uncompressed size, or nullopt if not recorded (v0). Throws if file is absent.
    std::optional<UInt64> getFileUncompressedSize(const String & file_name) const;
    PackedFilesIO::FileOffset getFileOffsetAndSize(const String & file_name) const;
    Names getFileNames() const;

    /// Returns read buffer to read requested file as a part of the archive file. The archive's
    /// current location (disk + path) is supplied by the caller, so a relocated part reads from
    /// the right place without the reader caching a path.
    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const DiskPtr & disk,
        const String & data_file_name,
        const String & file_name,
        const ReadSettings & settings,
        std::optional<size_t> read_hint) const;

    const PackedFilesIO::Index & getIndex() const { return index; }

    static PackedFilesIO::Index readIndex(ReadBuffer & in);

private:
    /// Index of archive: immutable and path-independent.
    const PackedFilesIO::Index index;
};

}
