#pragma once

#include "config.h"

#if USE_PARQUET

#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>
#include <Common/VectorWithMemoryTracking.h>

#include <optional>
#include <string_view>

namespace DB
{
class ReadBuffer;
struct FormatSettings;
}

namespace DB::Parquet
{

/// The metadata of a Parquet file as the Apache Arrow C++ library (parquet-cpp) presents it: leaf columns with
/// their levels and logical types, and column chunk statistics that are trusted only as far as parquet-cpp trusts
/// them, given the sort order of the column and the version of the writer. The `ParquetMetadata` format and the
/// `Hive` engine used to read the metadata with parquet-cpp, and this keeps what they show and use unchanged.
/// The native Parquet reader has rules of its own and does not use this.
class FooterMetadata
{
public:
    /// The sort order of the values of a column (parquet-cpp's `SortOrder`), which decides whether its statistics
    /// can be used.
    enum class SortOrder : UInt8
    {
        Signed,
        Unsigned,
        Unknown,
    };

    /// A leaf column of the schema.
    struct Column
    {
        String name;
        /// The names from the top-level field down to the leaf, joined by dots.
        String path;
        Int16 max_definition_level = 0;
        Int16 max_repetition_level = 0;
        /// The physical type, or `std::nullopt` for a value out of the range of the enum.
        std::optional<parq::Type::type> physical_type;
        /// The logical type, as parquet-cpp's `LogicalType::ToString` spells it.
        String logical_type;
        SortOrder sort_order = SortOrder::Unknown;
        Int32 type_length = 0;
    };

    struct Statistics
    {
        Int64 num_values = 0;
        std::optional<Int64> null_count;
        std::optional<Int64> distinct_count;
        /// The PLAIN-encoded bounds, set when both are present and trusted.
        std::optional<std::string_view> min;
        std::optional<std::string_view> max;
    };

    /// Reads the footer of the Parquet file `in`.
    static FooterMetadata read(ReadBuffer & in, const FormatSettings & format_settings);
    /// Parses a serialized `FileMetaData`. The schema can be nested up to `max_schema_depth` deep, 0 means unlimited.
    static FooterMetadata parse(const char * data, size_t size, size_t max_schema_depth);

    [[nodiscard]] const parq::FileMetaData & metadata() const { return file_metadata; }
    /// The number of bytes of the serialized `FileMetaData`.
    [[nodiscard]] size_t metadataSize() const { return metadata_size; }
    [[nodiscard]] const VectorWithMemoryTracking<Column> & columns() const { return leaf_columns; }

    /// The metadata of the chunk of column `column` in row group `row_group`. Throws if the row group has no such
    /// chunk, if the chunk is encrypted with a column key, or if its size statistics are inconsistent with the schema.
    [[nodiscard]] const parq::ColumnMetaData & columnChunk(size_t row_group, size_t column) const;

    /// The statistics of a column chunk, or `std::nullopt` when it has none or when they cannot be trusted.
    [[nodiscard]] std::optional<Statistics> statistics(size_t row_group, size_t column) const;

    /// Decodes a PLAIN-encoded statistics bound of a column into its text form: a number, the bytes of a
    /// `BYTE_ARRAY`, or the decimal values of the bytes of an `INT96` (as 32-bit words) or a `FIXED_LEN_BYTE_ARRAY`,
    /// each followed by a space.
    [[nodiscard]] String formatStatisticsValue(size_t column, std::string_view encoded) const;

    /// Decodes a PLAIN-encoded statistics bound of a column of a fixed-width physical type. Throws if it is too short.
    template <typename T>
    static T decodeStatisticsValue(std::string_view encoded);

private:
    struct WriterVersion
    {
        String application = "unknown";
        int major = 0;
        int minor = 0;
        int patch = 0;
    };

    enum class ColumnOrder : UInt8
    {
        Undefined,
        TypeDefined,
        Unknown,
    };

    void initialize(size_t max_schema_depth);
    [[nodiscard]] bool hasCorrectStatistics(const parq::ColumnMetaData & chunk, const Column & column, bool has_min, bool has_max,
        std::string_view min, std::string_view max) const;

    parq::FileMetaData file_metadata;
    size_t metadata_size = 0;
    VectorWithMemoryTracking<Column> leaf_columns;
    VectorWithMemoryTracking<ColumnOrder> column_orders;
    WriterVersion writer_version;
};

}

#endif
