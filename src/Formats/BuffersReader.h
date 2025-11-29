#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>

#include <optional>

namespace DB
{

class ReadBuffer;

/** Deserializes a stream of blocks from the Buffers format.
  *
  * For each block:
  *   Number of buffers (UInt64, little-endian)
  *   Size of each buffer in bytes (UInt64 * number of buffers, little-endian)
  *   Contents of each buffer (raw bytes, concatenated)
  *
  * The schema (names, types, order) is taken from the header passed
  * in the constructor; the stream itself does not contain names or types.
  */
class BuffersReader
{
public:
    BuffersReader(ReadBuffer & istr_, const Block & header_, std::optional<FormatSettings> format_settings_ = std::nullopt);

    Block getHeader() const;

    /// Reads one block
    Block read();

private:
    ReadBuffer & istr;
    Block header;
    std::optional<FormatSettings> format_settings;
};

}
