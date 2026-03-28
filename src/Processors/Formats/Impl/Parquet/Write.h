#pragma once

#include <Columns/IColumn_fwd.h>
#include <DataTypes/IDataType.h>
#include <IO/CompressionMethod.h>
#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>
#include <generated/parquet_types.h>
#include <Common/PODArray.h>

namespace DB
{
class Block;
}

namespace DB::Parquet
{

/// A good resource for learning how Parquet format works is
/// contrib/arrow/cpp/src/parquet/parquet.thrift

struct WriteOptions
{
    bool output_string_as_string = false;
    bool output_fixed_string_as_fixed_byte_array = true;
    bool output_datetime_as_uint32 = false;
    bool output_date_as_uint16 = false;
    bool output_enum_as_byte_array = false;

    /// Note: the meaning of some compression methods here is different from
    /// wrapReadBufferWithCompressionMethod:
    ///  * Lz4 here lz4 block format, while in Lz4InflatingReadBuffer uses lz4 framed format,
    ///  * Snappy here doesn't have extra headers, while HadoopSnappyReadBuffer does.
    CompressionMethod compression = CompressionMethod::Lz4;
    int compression_level = 3;

    size_t data_page_size = 1024 * 1024;
    size_t write_batch_size = 1024;

    bool use_dictionary_encoding = true;
    size_t max_dictionary_size = 1024 * 1024;
    /// If using dictionary, this encoding is used as a fallback when dictionary gets too big.
    /// Otherwise, this is used for everything.
    parq::Encoding::type encoding = parq::Encoding::PLAIN;

    bool write_column_chunk_statistics = true;
    bool write_page_statistics = true;
    bool write_page_index = true;
    bool write_bloom_filter = true;
    bool write_checksums = true;

    size_t max_statistics_size = 4096;

    /// Bits -> false positive rate (from https://parquet.apache.org/docs/file-format/bloomfilter/):
    ///  6.0  10%
    /// 10.5   1%
    /// 16.9   0.1%
    /// 26.4   0.01%
    /// 41     0.001%
    double bloom_filter_bits_per_value = 10.5;

    /// Parquet format allows bloom filters to be placed either after each row group, or all at
    /// the end of the file. Or, presumably, after ranges of consecutive row groups.
    /// This setting controls this.
    ///  * If set to 0, bloom filters for each row group are written immediately after that row group.
    ///  * If set to infinity, bloom filters for all row groups are written at the end of the file,
    ///    after all row groups. This may be better for read locality, but may use a lot of memory in
    ///    the writer as it needs to keep all bloom filters in memory at once.
    ///  * In general, if set to N, bloom filters for written row groups are accumulated in memory
    ///    and flushed to the file when they become bigger than N bytes (to limit memory usage).
    size_t bloom_filter_flush_threshold_bytes = 1024 * 1024 * 128;

    bool write_geometadata = true;
};

struct ColumnChunkIndexes
{
    parq::ColumnIndex column_index; // if write_page_index
    parq::OffsetIndex offset_index; // if write_page_index
    parq::BloomFilterHeader bloom_filter_header;
    PODArray<UInt32> bloom_filter_data; // if write_bloom_filter, and not flushed yet
};

/// Information about a primitive column (leaf of the schema tree) to write to Parquet file.
struct ColumnChunkWriteState
{
    /// After writeColumnChunkBody(), offsets in this struct are relative to the start of column chunk.
    /// Then finalizeColumnChunkAndWriteFooter fixes them up before writing to file.
    parq::ColumnChunk column_chunk;

    ColumnPtr primitive_column;
    DataTypePtr type;
    CompressionMethod compression; // must match what's inside column_chunk
    int compression_level = 3;
    Int64 datetime_multiplier = 1; // for converting e.g. seconds to milliseconds
    bool is_bool = false; // bool vs UInt8 have the same column type but are encoded differently

    /// Repetition and definition levels. Produced by prepareColumnForWrite().
    /// def is empty iff max_def == 0, which means no arrays or nullables.
    /// rep is empty iff max_rep == 0, which means no arrays.
    PaddedPODArray<UInt8> def; // definition levels
    PaddedPODArray<UInt8> rep; // repetition levels
    /// Max possible levels, according to schema. Actual max in def/rep may be smaller.
    UInt8 max_def = 0;
    UInt8 max_rep = 0;

    /// Indexes that will need to be written after the row group or at the end of the file.
    ColumnChunkIndexes indexes;

    ColumnChunkWriteState() = default;
    /// Prevent accidental copying.
    ColumnChunkWriteState(ColumnChunkWriteState &&) = default;
    ColumnChunkWriteState & operator=(ColumnChunkWriteState &&) = default;

    /// Estimated memory usage.
    size_t allocatedBytes() const;
};

struct RowGroupWithIndexes
{
    parq::RowGroup row_group;
    std::vector<ColumnChunkIndexes> column_indexes;
};

struct FileWriteState
{
    std::vector<RowGroupWithIndexes> completed_row_groups;
    RowGroupWithIndexes current_row_group;
    size_t row_groups_with_flushed_bloom_filter = 0;
    size_t unflushed_bloom_filter_bytes = 0;
    size_t offset = 0;
};

using SchemaElements = std::vector<parq::SchemaElement>;
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
/// (1) Call writeFileHeader
/// (2) For each row group:
///  | (3) For each ClickHouse column:
///  |  | (4) Call prepareColumnForWrite.
///  |  |     It'll produce one or more ColumnChunkWriteStates, corresponding to primitive columns
///  |  |     that we need to write.
///  | (5) For each ColumnChunkWriteState:
///  |  | (6) Call writeColumnChunkBody to write the actual data to the given WriteBuffer.
///  |  | (7) Call finalizeColumnChunkAndWriteFooter to write the footer of the column chunk.
///  | (8) Call finalizeRowGroup.
/// (9) Call writeFileFooter.
///
/// Steps (4) and (6) can be parallelized, both within and across row groups.

/// Parquet schema is a tree of SchemaElements, flattened into a list in depth-first order.
/// Leaf nodes correspond to physical columns of primitive types. Inner nodes describe logical
/// groupings of those columns, e.g. tuples or structs.
SchemaElements convertSchema(const Block & sample, const WriteOptions & options, const std::optional<std::unordered_map<String, Int64>> & column_field_ids);

void prepareColumnForWrite(
    ColumnPtr column, DataTypePtr type, const std::string & name, const WriteOptions & options,
    ColumnChunkWriteStates * out_columns_to_write, SchemaElements * out_schema = nullptr, const std::optional<std::unordered_map<String, Int64>> & column_field_ids = std::nullopt);

void writeFileHeader(FileWriteState & file, WriteBuffer & out);

/// Encodes a column chunk, without the footer.
/// Can be called in parallel for multiple column chunks (with different WriteBuffer-s).
void writeColumnChunkBody(
    ColumnChunkWriteState & s, const WriteOptions & options, const FormatSettings & format_settings, WriteBuffer & out);

/// Unlike most of the column chunk data, the footer (`ColumnMetaData`) needs to know its absolute
/// offset in the file. So we encode it separately, in one thread, after all previous row groups
/// and column chunks have been encoded.
/// (If you're wondering if the 8-byte offset values can be patched inside the encoded blob - no,
/// they're varint-encoded and can't be padded to a fixed length.)
void finalizeColumnChunkAndWriteFooter(
    ColumnChunkWriteState s, FileWriteState & file, WriteBuffer & out);

void finalizeRowGroup(FileWriteState & file, size_t num_rows, const WriteOptions & options, WriteBuffer & out);

void writeFileFooter(FileWriteState & file,
    SchemaElements schema,
    const WriteOptions & options,
    WriteBuffer & out,
    const Block & header);

}
