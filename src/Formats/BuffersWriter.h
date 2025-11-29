#pragma once

#include <Core/Block_fwd.h>
#include <Formats/FormatSettings.h>

namespace DB
{

class Block;
class WriteBuffer;

/** Serializes the stream of blocks in their native binary format (without names, columns and serialization types).
  *
  * For each block:
  *   Number of buffers (UInt64, little-endian)
  *   Size of each buffer in bytes (UInt64 * number of buffers, little-endian)
  *   Contents of each buffer (raw bytes, concatenated)
  */
class BuffersWriter
{
public:
    BuffersWriter(WriteBuffer & ostr_, SharedHeader header_, std::optional<FormatSettings> format_settings_ = std::nullopt);

    SharedHeader getHeader() const { return header; }

    /// Returns the number of bytes written
    size_t write(const Block & block);

private:
    WriteBuffer & ostr;
    SharedHeader header;
    std::optional<FormatSettings> format_settings;
};

}
