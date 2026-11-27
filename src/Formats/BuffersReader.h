#pragma once

#include <Core/Block.h>
#include <Formats/FormatSettings.h>

#include <optional>

namespace DB
{

class ReadBuffer;

/** Deserializes a stream of blocks from the Buffers format.
  *
  * For each block, the following sequence is read:
  * 1. Number of columns (UInt64, little endian).
  * 2. Number of rows (UInt64, little endian).
  * 3. For each column:
  *   - Total byte size of the serialized column data (UInt64, little endian).
  *   - Serialized column data bytes, exactly as in the Native format.
  *
  * The schema (names, types, order) is taken from the header passed
  * in the constructor; the stream itself does not contain names or types.
*/

class BuffersReader
{
public:
    BuffersReader(ReadBuffer & istr_, const Block & header_, const FormatSettings & format_settings_);

    Block getHeader() const;

    /// Reads one block
    Block read();

private:
    ReadBuffer & istr;
    Block header;
    FormatSettings format_settings;
};

}
