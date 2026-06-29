#pragma once

#include <Core/Block_fwd.h>
#include <Formats/FormatSettings.h>

namespace DB
{

class Block;
class WriteBuffer;

/** Serializes the stream of blocks in their native binary format (without names, columns and serialization types).
  *
  * For each block, the following sequence is written:
  * 1. Number of columns (UInt64, little endian).
  * 2. Number of rows (UInt64, little endian).
  * 3. For each column:
  *   - Total byte size of the serialized column data (UInt64, little endian).
  *   - Serialized column data bytes, exactly as in the Native format.
*/

class BuffersWriter
{
public:
    BuffersWriter(WriteBuffer & ostr_, SharedHeader header_, const FormatSettings & format_settings_);

    SharedHeader getHeader() const { return header; }

    /// Returns the number of bytes written
    void write(const Block & block);

private:
    WriteBuffer & ostr;
    SharedHeader header;
    FormatSettings format_settings;
};

}
