#pragma once

#include <Columns/IColumn.h>
#include <unordered_map>

namespace DB
{

class ChunkInfo
{
public:
    virtual ~ChunkInfo() = default;
    ChunkInfo() = default;
};

using ChunkInfoPtr = std::shared_ptr<const ChunkInfo>;

class Chunk
{
public:
    Chunk() = default;
    Chunk(const Chunk & other) = delete;
    Chunk(Chunk && other) noexcept;
    Chunk(Columns columns_, UInt64 num_rows_);
    Chunk(Columns columns_, UInt64 num_rows_, ChunkInfoPtr chunk_info_);
    Chunk(MutableColumns columns_, UInt64 num_rows_);
    Chunk(MutableColumns columns_, UInt64 num_rows_, ChunkInfoPtr chunk_info_);

    Chunk & operator=(const Chunk & other) = delete;
    Chunk & operator=(Chunk && other) noexcept;

    Chunk clone() const;

    const Columns & getColumns() const { return columns; }
    void setColumns(Columns columns_, UInt64 num_rows_);
    void setColumns(MutableColumns columns_, UInt64 num_rows_);
    Columns detachColumns();
    MutableColumns mutateColumns();
    /** Get empty columns with the same types as in block. */
    MutableColumns cloneEmptyColumns() const;

    const ChunkInfoPtr & getChunkInfo() const { return chunk_info; }
    void setChunkInfo(ChunkInfoPtr chunk_info_) { chunk_info = std::move(chunk_info_); }

    UInt64 getNumRows() const { return num_rows; }
    UInt64 getNumColumns() const { return columns.size(); }
    bool hasNoRows() const { return num_rows == 0; }
    bool hasNoColumns() const { return columns.empty(); }
    bool empty() const { return hasNoRows() && hasNoColumns(); }
    operator bool() const { return !empty(); }

    void clear();
    void erase(size_t position);

    UInt64 bytes() const;
    UInt64 allocatedBytes() const;

    std::string dumpStructure() const;

private:
    Columns columns;
    UInt64 num_rows = 0;
    ChunkInfoPtr chunk_info;

    void checkNumRowsIsConsistent();
};

using Chunks = std::vector<Chunk>;

/// Block extension to support delayed defaults. AddingDefaultsProcessor uses it to replace missing values with column defaults.
class ChunkMissingValues : public ChunkInfo
{
public:
    using RowsBitMask = std::vector<bool>; /// a bit per row for a column

    const RowsBitMask & getDefaultsBitmask(size_t column_idx) const;
    void setBit(size_t column_idx, size_t row_idx);
    bool empty() const { return rows_mask_by_column_id.empty(); }
    size_t size() const { return rows_mask_by_column_id.size(); }
    void clear() { rows_mask_by_column_id.clear(); }

private:
    using RowsMaskByColumnId = std::unordered_map<size_t, RowsBitMask>;

    /// If rows_mask_by_column_id[column_id][row_id] is true related value in Block should be replaced with column default.
    /// It could contain less columns and rows then related block.
    RowsMaskByColumnId rows_mask_by_column_id;
};

}
