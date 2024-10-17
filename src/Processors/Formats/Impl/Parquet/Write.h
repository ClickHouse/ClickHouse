#pragma once

#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Common/PODArray.h>
#include <IO/CompressionMethod.h>
#include <generated/parquet_types.h>

namespace DB::Parquet
{

/// A good resource for learning how Parquet format works is
/// contrib/arrow/cpp/src/parquet/parquet.thrift

struct WriteOptions
{
    bool output_string_as_string = false;
    bool output_fixed_string_as_fixed_byte_array = true;

    CompressionMethod compression = CompressionMethod::Lz4;

    size_t data_page_size = 1024 * 1024;
    size_t write_batch_size = 1024;

    bool use_dictionary_encoding = true;
    size_t dictionary_size_limit = 1024 * 1024;
    /// If using dictionary, this encoding is used as a fallback when dictionary gets too big.
    /// Otherwise, this is used for everything.
    parquet::format::Encoding::type encoding = parquet::format::Encoding::PLAIN;

    bool write_page_statistics = true;
    bool write_column_chunk_statistics = true;
    size_t max_statistics_size = 4096;
};

/// Information about a primitive column (leaf of the schema tree) to write to Parquet file.
struct ColumnChunkWriteState
{
    /// After writeColumnChunkBody(), offsets in this struct are relative to the start of column chunk.
    /// Then finalizeColumnChunkAndWriteFooter() fixes them up before writing to file.
    parquet::format::ColumnChunk column_chunk;

    ColumnPtr primitive_column;
    CompressionMethod compression; // must match what's inside column_chunk
    Int64 datetime64_multiplier = 1; // for converting e.g. seconds to milliseconds
    bool is_bool = false; // bool vs UInt8 have the same column type but are encoded differently

    /// Repetition and definition levels. Produced by prepareColumnForWrite().
    /// def is empty iff max_def == 0, which means no arrays or nullables.
    /// rep is empty iff max_rep == 0, which means no arrays.
    PaddedPODArray<UInt8> def; // definition levels
    PaddedPODArray<UInt8> rep; // repetition levels
    /// Max possible levels, according to schema. Actual max in def/rep may be smaller.
    UInt8 max_def = 0;
    UInt8 max_rep = 0;

    parquet::format::ColumnIndex column_index;
    parquet::format::OffsetIndex offset_index;

    ColumnChunkWriteState() = default;
    /// Prevent accidental copying.
    ColumnChunkWriteState(ColumnChunkWriteState &&) = default;
    ColumnChunkWriteState & operator=(ColumnChunkWriteState &&) = default;

    /// Estimated memory usage.
    size_t allocatedBytes() const
    {
        size_t r = def.allocated_bytes() + rep.allocated_bytes();
        if (primitive_column)
            r += primitive_column->allocatedBytes();
        return r;
    }
};

using SchemaElements = std::vector<parquet::format::SchemaElement>;
using ColumnChunkWriteStates = std::vector<ColumnChunkWriteState>;

/// Parquet file consists of row groups, which consist of column chunks.
///
/// Column chunks can be encoded mostly independently of each other, in parallel.
/// But there are two small complications:
///  1. One ClickHouse column can translate to multiple leaf columns in parquet.
///     E.g. tuples and maps.
///     If all primitive columns are in one big tuple, we'd like to encode them in parallel too,
///     even though they're one top-level ClickHouse column.
///  2. At the end of each encoded column chunk there's a footer (struct ColumnMetaData) that
///     contains some absolute offsets in the file. We can't encode it until we know the exact
///     position in the file where the column chunk will go. So these footers have to be serialized
///     sequentially, after we know sizes of all previous column chunks.
///
/// With that in mind, here's how to write a parquet file:
///
/// (1) writeFileHeader()
/// (2) For each row group:
///  | (3) For each ClickHouse column:
///  |    (4) Call prepareColumnForWrite().
///  |        It'll produce one or more ColumnChunkWriteStates, corresponding to primitive columns that
///  |        we need to write.
///  |        It'll also produce SchemaElements as a byproduct, describing the logical types and
///  |        groupings of the physical columns (e.g. tuples, arrays, maps).
///  | (5) For each ColumnChunkWriteState:
///  |    (6) Call writeColumnChunkBody() to write the actual data to the given WriteBuffer.
///  |    (7) Call finalizeColumnChunkAndWriteFooter() to write the footer of the column chunk.
///  | (8) Call makeRowGroup() using the ColumnChunk metadata structs from previous step.
/// (9) Call writeFileFooter() using the row groups from previous step and SchemaElements from
///     convertSchema().
///
/// Steps (4) and (6) can be parallelized, both within and across row groups.

/// Parquet schema is a tree of SchemaElements, flattened into a list in depth-first order.
/// Leaf nodes correspond to physical columns of primitive types. Inner nodes describe logical
/// groupings of those columns, e.g. tuples or structs.
SchemaElements convertSchema(const Block & sample, const WriteOptions & options);

void prepareColumnForWrite(
    ColumnPtr column, DataTypePtr type, const std::string & name, const WriteOptions & options,
    ColumnChunkWriteStates * out_columns_to_write, SchemaElements * out_schema = nullptr);

void writeFileHeader(WriteBuffer & out);

/// Encodes a column chunk, without the footer.
/// The ColumnChunkWriteState-s should then passed to finalizeColumnChunkAndWriteFooter().
void writeColumnChunkBody(ColumnChunkWriteState & s, const WriteOptions & options, WriteBuffer & out);

/// Unlike most of the column chunk data, the footer (`ColumnMetaData`) needs to know its absolute
/// offset in the file. So we encode it separately, after all previous row groups and column chunks
/// have been encoded.
/// (If you're wondering if the 8-byte offset values can be patched inside the encoded blob - no,
/// they're varint-encoded and can't be padded to a fixed length.)
/// `offset_in_file` is the absolute position in the file where the writeColumnChunkBody()'s output
/// starts.
/// Returns a ColumnChunk to add to the RowGroup.
parquet::format::ColumnChunk finalizeColumnChunkAndWriteFooter(
    size_t offset_in_file, ColumnChunkWriteState s, const WriteOptions & options, WriteBuffer & out);

parquet::format::RowGroup makeRowGroup(std::vector<parquet::format::ColumnChunk> column_chunks, size_t num_rows);

void writePageIndex(const std::vector<std::vector<parquet::format::ColumnIndex>>& column_indexes, const std::vector<std::vector<parquet::format::OffsetIndex>>& offset_indexes, std::vector<parquet::format::RowGroup>& row_groups, WriteBuffer & out, size_t base_offset);
void writeFileFooter(std::vector<parquet::format::RowGroup> row_groups, SchemaElements schema, const WriteOptions & options, WriteBuffer & out);

}
