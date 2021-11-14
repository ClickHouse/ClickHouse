#pragma once

#include <Core/Names.h>
#include <Formats/MarkInCompressedFile.h>

namespace DB
{

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

    friend bool operator==(const IndexOfOneColumnForNativeFormat & lhs, const IndexOfOneColumnForNativeFormat & rhs)
    {
        return (lhs.name == rhs.name) && (lhs.type == rhs.type) && (lhs.location == rhs.location);
    }

    friend bool operator!=(const IndexOfOneColumnForNativeFormat & lhs, const IndexOfOneColumnForNativeFormat & rhs)
    {
        return !(lhs == rhs);
    }
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

    size_t getMinOffsetInCompressedFile() const;

    friend bool operator==(const IndexOfBlockForNativeFormat & lhs, const IndexOfBlockForNativeFormat & rhs)
    {
        return (lhs.num_columns == rhs.num_columns) && (lhs.num_rows == rhs.num_rows) && (lhs.columns == rhs.columns);
    }

    friend bool operator!=(const IndexOfBlockForNativeFormat & lhs, const IndexOfBlockForNativeFormat & rhs) { return !(lhs == rhs); }
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

    friend bool operator==(const IndexForNativeFormat & lhs, const IndexForNativeFormat & rhs) { return (lhs.blocks == rhs.blocks); }
    friend bool operator!=(const IndexForNativeFormat & lhs, const IndexForNativeFormat & rhs) { return !(lhs == rhs); }
};
}
