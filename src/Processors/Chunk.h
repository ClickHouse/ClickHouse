#pragma once

#include <Common/CollectionOfDerived.h>
#include <Columns/IColumn.h>

#include <memory>

namespace DB
{

class ChunkInfo
{
public:
    using Ptr = std::shared_ptr<ChunkInfo>;

    ChunkInfo() = default;
    ChunkInfo(const ChunkInfo&) = default;
    ChunkInfo(ChunkInfo&&) = default;

    virtual Ptr clone() const = 0;
    virtual ~ChunkInfo() = default;
};


template <typename Derived>
class ChunkInfoCloneable : public ChunkInfo
{
public:
    ChunkInfoCloneable() = default;
    ChunkInfoCloneable(const ChunkInfoCloneable & other) = default;

    Ptr clone() const override
    {
        return std::static_pointer_cast<ChunkInfo>(std::make_shared<Derived>(*static_cast<const Derived*>(this)));
    }
};

/**
 * Chunk is a list of columns with the same length.
 * Chunk stores the number of rows in a separate field and supports invariant of equal column length.
 *
 * Chunk has move-only semantic. It's more lightweight than block cause doesn't store names, types and index_by_name.
 *
 * Chunk can have empty set of columns but non-zero number of rows. It helps when only the number of rows is needed.
 * Chunk can have columns with zero number of rows. It may happen, for example, if all rows were filtered.
 * Chunk is empty only if it has zero rows and empty list of columns.
 *
 * Any ChunkInfo may be attached to chunk.
 * It may be useful if additional info per chunk is needed. For example, bucket number for aggregated data.
**/

class Chunk
{
public:
    using ChunkInfoCollection = CollectionOfDerivedItems<ChunkInfo>;

    Chunk() = default;
    Chunk(const Chunk & other) = delete;
    Chunk(Chunk && other) noexcept
        : columns(std::move(other.columns))
        , num_rows(other.num_rows)
        , chunk_infos(std::move(other.chunk_infos))
    {
        other.num_rows = 0;
    }

    Chunk(Columns columns_, UInt64 num_rows_);
    Chunk(MutableColumns columns_, UInt64 num_rows_);

    Chunk & operator=(const Chunk & other) = delete;
    Chunk & operator=(Chunk && other) noexcept
    {
        columns = std::move(other.columns);
        chunk_infos = std::move(other.chunk_infos);
        num_rows = other.num_rows;
        other.num_rows = 0;
        return *this;
    }

    Chunk clone() const;

    void swap(Chunk & other) noexcept
    {
        columns.swap(other.columns);
        std::swap(num_rows, other.num_rows);
        chunk_infos.swap(other.chunk_infos);
    }

    void clear()
    {
        num_rows = 0;
        columns.clear();
        chunk_infos.clear();
    }

    const Columns & getColumns() const { return columns; }
    void setColumns(Columns columns_, UInt64 num_rows_);
    void setColumns(MutableColumns columns_, UInt64 num_rows_);
    Columns detachColumns();
    MutableColumns mutateColumns();
    /** Get empty columns with the same types as in block. */
    MutableColumns cloneEmptyColumns() const;

    ChunkInfoCollection & getChunkInfos() { return chunk_infos; }
    const ChunkInfoCollection & getChunkInfos() const { return chunk_infos; }
    void setChunkInfos(ChunkInfoCollection chunk_infos_) { chunk_infos = std::move(chunk_infos_); }

    UInt64 getNumRows() const { return num_rows; }
    UInt64 getNumColumns() const { return columns.size(); }
    bool hasRows() const { return num_rows > 0; }
    bool hasColumns() const { return !columns.empty(); }
    bool empty() const { return !hasRows() && !hasColumns(); }
    explicit operator bool() const { return !empty(); }

    void addColumn(ColumnPtr column);
    void addColumn(size_t position, ColumnPtr column);
    void erase(size_t position);

    UInt64 bytes() const;
    UInt64 allocatedBytes() const;

    std::string dumpStructure() const;

    void append(const Chunk & chunk);
    void append(const Chunk & chunk, size_t from, size_t length); // append rows [from, from+length) of chunk

private:
    Columns columns;
    UInt64 num_rows = 0;
    ChunkInfoCollection chunk_infos;

    void checkNumRowsIsConsistent();
};

using Chunks = std::vector<Chunk>;

/// AsyncInsert needs two kinds of information:
/// - offsets of different sub-chunks
/// - tokens of different sub-chunks, which are assigned by setting `insert_deduplication_token`.
class AsyncInsertInfo : public ChunkInfoCloneable<AsyncInsertInfo>
{
public:
    AsyncInsertInfo() = default;
    AsyncInsertInfo(const AsyncInsertInfo & other) = default;
    AsyncInsertInfo(const std::vector<size_t> & offsets_, const std::vector<String> & tokens_)
        : offsets(offsets_)
        , tokens(tokens_)
    {}

    std::vector<size_t> offsets;
    std::vector<String> tokens;
};

using AsyncInsertInfoPtr = std::shared_ptr<AsyncInsertInfo>;

/// Extension to support delayed defaults. AddingDefaultsProcessor uses it to replace missing values with column defaults.
class ChunkMissingValues : public ChunkInfoCloneable<ChunkMissingValues>
{
public:
    ChunkMissingValues(const ChunkMissingValues & other) = default;

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

/// Converts all columns to full serialization in chunk.
/// It's needed, when you have to access to the internals of the column,
/// or when you need to perform operation with two columns
/// and their structure must be equal (e.g. compareAt).
void convertToFullIfConst(Chunk & chunk);
void convertToFullIfSparse(Chunk & chunk);

/// Creates a chunk with the same columns but makes them constants with a default value and a specified number of rows.
Chunk cloneConstWithDefault(const Chunk & chunk, size_t num_rows);

}
