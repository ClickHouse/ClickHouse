#pragma once

#include <Core/Streaming/Watermark.h>
#include <Columns/IColumn.h>
#include <unordered_map>

namespace DB
{

struct ChunkContext
{
    static constexpr UInt64 WATERMARK_FLAG = 0x1;
    static constexpr UInt64 APPEND_TIME_FLAG = 0x2;
    static constexpr UInt64 RETRACTED_DATA_FLAG = 0x10;
    static constexpr UInt64 AVOID_WATERMARK_FLAG = 0x8000'0000'0000'0000;

    /// A pair of Int64, flags represent what they mean
    Int64 ts_1 = 0;
    UInt64 flags = 0;

    ALWAYS_INLINE void setMark(UInt64 mark) { flags |= mark; }

    ALWAYS_INLINE explicit operator bool () const { return flags != 0; }

    ALWAYS_INLINE bool hasWatermark() const { return flags & WATERMARK_FLAG; }

    ALWAYS_INLINE void setWatermark(Int64 watermark)
    {
        assert(watermark != Streaming::INVALID_WATERMARK);
        flags |= WATERMARK_FLAG;
        ts_1 = watermark;
    }

    ALWAYS_INLINE Int64 getWatermark() const
    {
        assert(hasWatermark());

        return ts_1;
    }

    ALWAYS_INLINE void clearWatermark()
    {
        if (hasWatermark())
        {
            flags &= ~WATERMARK_FLAG;
            ts_1 = 0;
        }
    }

    ALWAYS_INLINE void setRetractedDataFlag()
    {
        flags |= RETRACTED_DATA_FLAG;
        setAvoidWatermark();
    }

    ALWAYS_INLINE bool isRetractedData() const { return flags & RETRACTED_DATA_FLAG; }

    ALWAYS_INLINE void setAvoidWatermark() { flags |= AVOID_WATERMARK_FLAG; }

    ALWAYS_INLINE bool avoidWatermark() const { return flags & AVOID_WATERMARK_FLAG; }

    ALWAYS_INLINE bool hasAppendTime() const { return flags & APPEND_TIME_FLAG; }
    ALWAYS_INLINE void setAppendTime(Int64 append_time)
    {
        if (append_time > 0)
        {
            flags |= APPEND_TIME_FLAG;
            ts_1 = append_time;
        }
    }

    ALWAYS_INLINE Int64 getAppendTime() const { assert(hasAppendTime()); return ts_1; }
};
using ChunkContextPtr = std::shared_ptr<ChunkContext>;

class ChunkInfo
{
public:
    virtual ~ChunkInfo() = default;
    ChunkInfo() = default;
};

using ChunkInfoPtr = std::shared_ptr<const ChunkInfo>;

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
    Chunk() = default;
    Chunk(const Chunk & other) = delete;
    Chunk(Chunk && other) noexcept
        : columns(std::move(other.columns))
        , num_rows(other.num_rows)
        , chunk_info(std::move(other.chunk_info))
        , chunk_ctx(std::move(other.chunk_ctx))
    {
        other.num_rows = 0;
    }

    Chunk(Columns columns_, UInt64 num_rows_);
    Chunk(Columns columns_, UInt64 num_rows_, ChunkInfoPtr chunk_info_);
    Chunk(MutableColumns columns_, UInt64 num_rows_);
    Chunk(MutableColumns columns_, UInt64 num_rows_, ChunkInfoPtr chunk_info_);

    Chunk & operator=(const Chunk & other) = delete;
    Chunk & operator=(Chunk && other) noexcept
    {
        columns = std::move(other.columns);
        chunk_info = std::move(other.chunk_info);
        chunk_ctx = std::move(other.chunk_ctx);
        num_rows = other.num_rows;
        other.num_rows = 0;
        return *this;
    }

    Chunk clone() const;

    void swap(Chunk & other)
    {
        columns.swap(other.columns);
        chunk_info.swap(other.chunk_info);
        chunk_ctx.swap(other.chunk_ctx);
        std::swap(num_rows, other.num_rows);
    }

    void clear()
    {
        num_rows = 0;
        columns.clear();
        chunk_info.reset();
        chunk_ctx.reset();
    }

    const Columns & getColumns() const { return columns; }
    void setColumns(Columns columns_, UInt64 num_rows_);
    void setColumns(MutableColumns columns_, UInt64 num_rows_);
    Columns detachColumns();
    MutableColumns mutateColumns();
    /** Get empty columns with the same types as in block. */
    MutableColumns cloneEmptyColumns() const;

    const ChunkInfoPtr & getChunkInfo() const { return chunk_info; }
    bool hasChunkInfo() const { return chunk_info != nullptr; }
    void setChunkInfo(ChunkInfoPtr chunk_info_) { chunk_info = std::move(chunk_info_); }

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

    bool hasWatermark() const { return chunk_ctx && chunk_ctx->hasWatermark(); }
    bool hasChunkContext() const { return chunk_ctx && chunk_ctx->operator bool(); }
    void setChunkContext(ChunkContextPtr chunk_ctx_) { chunk_ctx = std::move(chunk_ctx_); }
    ChunkContextPtr getChunkContext() const { return chunk_ctx; }

    ChunkContextPtr getOrCreateChunkContext()
    {
        if (chunk_ctx)
            return chunk_ctx;

        setChunkContext(std::make_shared<ChunkContext>());

        return chunk_ctx;
    }


    Int64 getWatermark() const
    {
        assert(chunk_ctx);
        return chunk_ctx->getWatermark();
    }

    void reserve(size_t num_columns)
    {
        columns.reserve(num_columns);
    }

    bool isRetractedData() const
    {
        return chunk_ctx && chunk_ctx->isRetractedData();
    }

    bool avoidWatermark() const
    {
        return chunk_ctx && chunk_ctx->avoidWatermark();
    }

    void clearWatermark() const
    {
        if (chunk_ctx)
            chunk_ctx->clearWatermark();
    }

    /// Dummy interface to make RefCountBlockList happy
    Int64 minTimestamp() const { return 0; }
    Int64 maxTimestamp() const { return 0;}

private:
    Columns columns;
    UInt64 num_rows = 0;
    ChunkInfoPtr chunk_info;
    ChunkContextPtr chunk_ctx;

    void checkNumRowsIsConsistent();
};

using Chunks = std::vector<Chunk>;

/// AsyncInsert needs two kinds of information:
/// - offsets of different sub-chunks
/// - tokens of different sub-chunks, which are assigned by setting `insert_deduplication_token`.
class AsyncInsertInfo : public ChunkInfo
{
public:
    AsyncInsertInfo() = default;
    explicit AsyncInsertInfo(const std::vector<size_t> & offsets_, const std::vector<String> & tokens_) : offsets(offsets_), tokens(tokens_) {}

    std::vector<size_t> offsets;
    std::vector<String> tokens;
};

using AsyncInsertInfoPtr = std::shared_ptr<AsyncInsertInfo>;

/// Extension to support delayed defaults. AddingDefaultsProcessor uses it to replace missing values with column defaults.
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

/// Converts all columns to full serialization in chunk.
/// It's needed, when you have to access to the internals of the column,
/// or when you need to perform operation with two columns
/// and their structure must be equal (e.g. compareAt).
void convertToFullIfConst(Chunk & chunk);
void convertToFullIfSparse(Chunk & chunk);

/// Creates a chunk with the same columns but makes them constants with a default value and a specified number of rows.
Chunk cloneConstWithDefault(const Chunk & chunk, size_t num_rows);

}
