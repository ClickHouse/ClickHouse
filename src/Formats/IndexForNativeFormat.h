#pragma once

#include <Core/Names.h>
#include <Formats/MarkInCompressedFile.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

/** The Native format can contain a separately located index,
  *  which allows you to understand where what column is located,
  *  and skip unnecessary columns.
  */

/** The position of one piece of a single column. */
struct IndexOfOneColumnForNativeFormat
{
    String name;
    String type;
    MarkInCompressedFile location;
};

/** The index for the data block. */
struct IndexOfBlockForNativeFormat
{
    using Columns = std::vector<IndexOfOneColumnForNativeFormat>;

    size_t num_columns;
    size_t num_rows;
    Columns columns;

    /// Reads the index for the data block.
    void read(ReadBuffer & istr);

    /// Writes the index for the data block.
    void write(WriteBuffer & ostr) const;

    /// Returns the index only for the required columns.
    IndexOfBlockForNativeFormat extractIndexForColumns(const NameSet & required_columns) const;
};

/** The whole index. */
struct IndexForNativeFormat
{
    using Blocks = std::vector<IndexOfBlockForNativeFormat>;
    Blocks blocks;

    bool empty() const { return blocks.empty(); }
    void clear() { blocks.clear(); }

    /// Reads the index.
    void read(ReadBuffer & istr);

    /// Writes the index.
    void write(WriteBuffer & ostr) const;

    /// Returns the index only for the required columns.
    IndexForNativeFormat extractIndexForColumns(const NameSet & required_columns) const;
};

}
