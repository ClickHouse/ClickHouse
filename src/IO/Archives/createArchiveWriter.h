#pragma once

#include <base/types.h>
#include <memory>


namespace DB
{
class IArchiveWriter;
class WriteBuffer;

/// Starts writing a specified archive in the local filesystem.
std::shared_ptr<IArchiveWriter> createArchiveWriter(const String & path_to_archive);

/// Starts writing a specified archive, the archive is written by using a specified write buffer,
/// `path_to_archive` is used only to determine the archive's type.
std::shared_ptr<IArchiveWriter> createArchiveWriter(const String & path_to_archive, std::unique_ptr<WriteBuffer> archive_write_buffer);

}
