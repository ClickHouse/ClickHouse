#pragma once

#include <Core/Defines.h>
#include <IO/WriteBuffer.h>

#include <base/types.h>
#include <memory>


namespace DB
{
class IArchiveWriter;

/// Starts writing a specified archive in the local filesystem.
/// If `archive_write_buffer` is null, the archive is written to the local filesystem at `path_to_archive`.
/// Otherwise, `path_to_archive` is used only to determine the archive type.
std::shared_ptr<IArchiveWriter> createArchiveWriter(
    const String & path_to_archive,
    std::unique_ptr<WriteBuffer> archive_write_buffer = nullptr,
    size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
    size_t adaptive_buffer_max_size = 8 * DBMS_DEFAULT_BUFFER_SIZE);
}
