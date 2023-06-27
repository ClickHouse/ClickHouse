#pragma once

#include <base/types.h>
#include <memory>


namespace DB
{
class IArchiveReader;
class SeekableReadBuffer;

/// Starts reading a specified archive in the local filesystem.
std::shared_ptr<IArchiveReader> createArchiveReader(const String & path_to_archive);

/// Starts reading a specified archive, the archive is read by using a specified read buffer,
/// `path_to_archive` is used only to determine the archive's type.
std::shared_ptr<IArchiveReader> createArchiveReader(
    const String & path_to_archive,
    const std::function<std::unique_ptr<SeekableReadBuffer>()> & archive_read_function,
    size_t archive_size);

}
