#include <span>
#include <Processors/Formats/Impl/ParquetMetadataInputFormat.h>

#if USE_PARQUET

#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Formats/FormatFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnNullable.h>
#include <Core/NamesAndTypes.h>
#include <Processors/Formats/Impl/Parquet/FooterMetadata.h>

#include <algorithm>
#include <cmath>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

static NamesAndTypesList getHeaderForParquetMetadata()
{
    NamesAndTypesList names_and_types{
        {"num_columns", std::make_shared<DataTypeUInt64>()},
        {"num_rows", std::make_shared<DataTypeUInt64>()},
        {"num_row_groups", std::make_shared<DataTypeUInt64>()},
        {"format_version", std::make_shared<DataTypeString>()},
        {"metadata_size", std::make_shared<DataTypeUInt64>()},
        {"total_uncompressed_size", std::make_shared<DataTypeUInt64>()},
        {"total_compressed_size", std::make_shared<DataTypeUInt64>()},
        {"columns",
         std::make_shared<DataTypeArray>(
             std::make_shared<DataTypeTuple>(
                 DataTypes{
                     std::make_shared<DataTypeString>(),
                     std::make_shared<DataTypeString>(),
                     std::make_shared<DataTypeUInt64>(),
                     std::make_shared<DataTypeUInt64>(),
                     std::make_shared<DataTypeString>(),
                     std::make_shared<DataTypeString>(),
                     std::make_shared<DataTypeString>(),
                     std::make_shared<DataTypeUInt64>(),
                     std::make_shared<DataTypeUInt64>(),
                     std::make_shared<DataTypeString>(),
                     std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
                 Names{
                     "name",
                     "path",
                     "max_definition_level",
                     "max_repetition_level",
                     "physical_type",
                     "logical_type",
                     "compression",
                     "total_uncompressed_size",
                     "total_compressed_size",
                     "space_saved",
                     "encodings"}))},
        {"row_groups",
         std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
             DataTypes{
                 std::make_shared<DataTypeUInt64>(),
                 std::make_shared<DataTypeUInt64>(),
                 std::make_shared<DataTypeUInt64>(),
                 std::make_shared<DataTypeUInt64>(),
                 std::make_shared<DataTypeUInt64>(),
                 std::make_shared<DataTypeArray>(
                     std::make_shared<DataTypeTuple>(
                         DataTypes{
                             std::make_shared<DataTypeString>(),
                             std::make_shared<DataTypeString>(),
                             std::make_shared<DataTypeUInt64>(),
                             std::make_shared<DataTypeUInt64>(),
                             DataTypeFactory::instance().get("Bool"),
                             std::make_shared<DataTypeTuple>(
                                 DataTypes{
                                     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
                                     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
                                     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
                                     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
                                     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
                                 Names{"num_values", "null_count", "distinct_count", "min", "max"}),
                             std::make_shared<DataTypeInt64>(),
                             DataTypeFactory::instance().get("Bool"),
                             std::make_shared<DataTypeTuple>(
                                 DataTypes{
                                     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
                                     std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()),
                                     std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())},
                                 Names{"unencoded_byte_array_data_bytes", "repetition_level_histogram", "definition_level_histogram"}),
                             DataTypeFactory::instance().get("Bool"),
                             std::make_shared<DataTypeTuple>(
                                 DataTypes{
                                     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()),
                                     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()),
                                     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()),
                                     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()),
                                     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()),
                                     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()),
                                     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()),
                                     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()),
                                     std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>())},
                                 Names{"xmin", "xmax", "ymin", "ymax", "zmin", "zmax", "mmin", "mmax", "geospatial_types"}),
                         },
                         Names{"name", "path", "total_compressed_size", "total_uncompressed_size", "have_statistics", "statistics", "bloom_filter_bytes", "have_size_statistics", "size_statistics", "have_geospatial_statistics", "geospatial_statistics"}))},
             Names{"file_offset", "num_columns", "num_rows", "total_uncompressed_size", "total_compressed_size", "columns"}))},
    };
    return names_and_types;
}

static void checkHeader(const Block & header)
{
    auto expected_names_and_types = getHeaderForParquetMetadata();
    UnorderedMapWithMemoryTracking<String, DataTypePtr> name_to_type;
    for (const auto & [name, type] : expected_names_and_types)
        name_to_type[name] = type;

    for (const auto & [name, type] : header.getNamesAndTypes())
    {
        auto it = name_to_type.find(name);
        if (it == name_to_type.end())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unexpected column: {}. ParquetMetadata format allows only the next columns: num_columns, num_rows, num_row_groups, "
                "format_version, metadata_size, total_uncompressed_size, total_compressed_size, columns, row_groups", name);

        if (!it->second->equals(*type))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unexpected type {} for column {}. Expected type: {}",
                type->getName(),
                name,
                it->second->getName());
    }
}

namespace
{

using Parquet::parq::CompressionCodec;

/// The compression codec as the Apache Arrow library named it, for the output to stay the same: its `LZ4` is
/// Parquet's `LZ4_RAW`, and Parquet's deprecated `LZ4` is its `LZ4_HADOOP`. An unknown codec reads as `UNCOMPRESSED`.
/// The enums are read with `thriftEnumToInt`: loading a value out of the range of the enum, which a malformed file
/// can have, is undefined behavior.
std::string_view compressionName(const CompressionCodec::type & codec)
{
    switch (Parquet::thriftEnumToInt(codec))
    {
        case CompressionCodec::UNCOMPRESSED: return "UNCOMPRESSED";
        case CompressionCodec::SNAPPY: return "SNAPPY";
        case CompressionCodec::GZIP: return "GZIP";
        case CompressionCodec::LZO: return "LZO";
        case CompressionCodec::BROTLI: return "BROTLI";
        case CompressionCodec::LZ4: return "LZ4_HADOOP";
        case CompressionCodec::ZSTD: return "ZSTD";
        case CompressionCodec::LZ4_RAW: return "LZ4";
        default: return "UNCOMPRESSED";
    }
}

std::string_view encodingName(const Parquet::parq::Encoding::type & encoding)
{
    auto it = Parquet::parq::_Encoding_VALUES_TO_NAMES.find(Parquet::thriftEnumToInt(encoding));
    if (it == Parquet::parq::_Encoding_VALUES_TO_NAMES.end())
        return "UNDEFINED";
    return it->second;
}

std::string_view physicalTypeName(std::optional<Parquet::parq::Type::type> type)
{
    if (!type)
        return "UNDEFINED";
    return Parquet::parq::_Type_VALUES_TO_NAMES.at(*type);
}

template <typename T>
void insertNullable(IColumn & column, std::optional<T> value)
{
    auto & nullable = assert_cast<ColumnNullable &>(column);
    if (!value)
    {
        nullable.insertDefault();
        return;
    }
    if constexpr (std::is_same_v<T, String>)
        assert_cast<ColumnString &>(nullable.getNestedColumn()).insertData(value->data(), value->size());
    else
        assert_cast<ColumnVector<T> &>(nullable.getNestedColumn()).insertValue(*value);
    nullable.getNullMapData().push_back(false);
}

void insertString(IColumn & column, std::string_view value)
{
    assert_cast<ColumnString &>(column).insertData(value.data(), value.size());
}

}

ParquetMetadataInputFormat::ParquetMetadataInputFormat(ReadBuffer & in_, SharedHeader header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), &in_), format_settings(format_settings_)
{
    checkHeader(getPort().getHeader());
}

Chunk ParquetMetadataInputFormat::read()
{
    Chunk res;
    if (done)
        return res;

    const auto metadata = Parquet::FooterMetadata::read(*in, format_settings);
    const auto & file_metadata = metadata.metadata();

    const auto & header = getPort().getHeader();
    auto names_and_types = getHeaderForParquetMetadata();
    auto names = names_and_types.getNames();
    auto types = names_and_types.getTypes();

    for (const auto & name : header.getNames())
    {
        /// num_columns
        if (name == names[0])
        {
            auto column = types[0]->createColumn();
            assert_cast<ColumnUInt64 &>(*column).insertValue(metadata.columns().size());
            res.addColumn(std::move(column));
        }
        /// num_rows
        else if (name == names[1])
        {
            auto column = types[1]->createColumn();
            assert_cast<ColumnUInt64 &>(*column).insertValue(file_metadata.num_rows);
            res.addColumn(std::move(column));
        }
        /// num_row_groups
        else if (name == names[2])
        {
            auto column = types[2]->createColumn();
            assert_cast<ColumnUInt64 &>(*column).insertValue(file_metadata.row_groups.size());
            res.addColumn(std::move(column));
        }
        /// format_version
        else if (name == names[3])
        {
            auto column = types[3]->createColumn();
            /// Parquet file doesn't know its exact version, only whether it's 1.x or 2.x
            /// (FileMetaData.version = 1 or 2).
            insertString(*column, file_metadata.version == 2 ? "2" : "1");
            res.addColumn(std::move(column));
        }
        /// metadata_size
        else if (name == names[4])
        {
            auto column = types[4]->createColumn();
            assert_cast<ColumnUInt64 &>(*column).insertValue(metadata.metadataSize());
            res.addColumn(std::move(column));
        }
        /// total_uncompressed_size
        else if (name == names[5])
        {
            auto column = types[5]->createColumn();
            size_t total_uncompressed_size = 0;
            for (const auto & row_group : file_metadata.row_groups)
                total_uncompressed_size += row_group.total_byte_size;

            assert_cast<ColumnUInt64 &>(*column).insertValue(total_uncompressed_size);
            res.addColumn(std::move(column));
        }
        /// total_compressed_size
        else if (name == names[6])
        {
            auto column = types[6]->createColumn();
            size_t total_compressed_size = 0;
            for (const auto & row_group : file_metadata.row_groups)
                total_compressed_size += row_group.total_compressed_size;

            assert_cast<ColumnUInt64 &>(*column).insertValue(total_compressed_size);
            res.addColumn(std::move(column));
        }
        /// columns
        else if (name == names[7])
        {
            auto column = types[7]->createColumn();
            fillColumnsMetadata(metadata, *column);
            res.addColumn(std::move(column));
        }
        /// row_groups
        else if (name == names[8])
        {
            auto column = types[8]->createColumn();
            fillRowGroupsMetadata(metadata, *column);
            res.addColumn(std::move(column));
        }
    }

    done = true;
    return res;
}

void ParquetMetadataInputFormat::fillColumnsMetadata(const Parquet::FooterMetadata & metadata, IColumn & column)
{
    auto & array_column = assert_cast<ColumnArray &>(column);
    auto & tuple_column = assert_cast<ColumnTuple &>(array_column.getData());
    const size_t num_row_groups = metadata.metadata().row_groups.size();
    for (size_t column_i = 0; column_i != metadata.columns().size(); ++column_i)
    {
        const auto & column_info = metadata.columns()[column_i];
        /// name
        insertString(tuple_column.getColumn(0), column_info.name);
        /// path
        insertString(tuple_column.getColumn(1), column_info.path);
        /// max_definition_level
        assert_cast<ColumnUInt64 &>(tuple_column.getColumn(2)).insertValue(column_info.max_definition_level);
        /// max_repetition_level
        assert_cast<ColumnUInt64 &>(tuple_column.getColumn(3)).insertValue(column_info.max_repetition_level);
        /// physical_type
        insertString(tuple_column.getColumn(4), physicalTypeName(column_info.physical_type));
        /// logical_type
        insertString(tuple_column.getColumn(5), column_info.logical_type);

        if (num_row_groups > 0)
        {
            /// compression
            insertString(tuple_column.getColumn(6), compressionName(metadata.columnChunk(0, column_i).codec));

            /// total_uncompressed_size/total_compressed_size
            size_t total_uncompressed_size = 0;
            size_t total_compressed_size = 0;
            for (size_t row_group_i = 0; row_group_i != num_row_groups; ++row_group_i)
            {
                const auto & chunk = metadata.columnChunk(row_group_i, column_i);
                total_uncompressed_size += chunk.total_uncompressed_size;
                total_compressed_size += chunk.total_compressed_size;
            }
            assert_cast<ColumnUInt64 &>(tuple_column.getColumn(7)).insertValue(total_uncompressed_size);
            assert_cast<ColumnUInt64 &>(tuple_column.getColumn(8)).insertValue(total_compressed_size);

            /// space_saved
            String space_saved = fmt::format("{:.4}%", (1 - static_cast<double>(total_compressed_size) / static_cast<double>(total_uncompressed_size)) * 100);
            insertString(tuple_column.getColumn(9), space_saved);

            /// encodings, of the last row group
            auto & encodings_array_column = assert_cast<ColumnArray &>(tuple_column.getColumn(10));
            auto & encodings_nested_column = assert_cast<ColumnString &>(encodings_array_column.getData());
            for (const auto & encoding : metadata.columnChunk(num_row_groups - 1, column_i).encodings)
                insertString(encodings_nested_column, encodingName(encoding));
            encodings_array_column.getOffsets().push_back(encodings_nested_column.size());
        }
        else
        {
            insertString(tuple_column.getColumn(6), "NONE");
            tuple_column.getColumn(7).insertDefault();
            tuple_column.getColumn(8).insertDefault();
            tuple_column.getColumn(9).insertDefault();
            tuple_column.getColumn(10).insertDefault();
        }
    }
    array_column.getOffsets().push_back(tuple_column.size());
}

void ParquetMetadataInputFormat::fillRowGroupsMetadata(const Parquet::FooterMetadata & metadata, IColumn & column)
{
    auto & row_groups_array_column = assert_cast<ColumnArray &>(column);
    auto & row_groups_column = assert_cast<ColumnTuple &>(row_groups_array_column.getData());
    const auto & row_groups = metadata.metadata().row_groups;
    for (size_t i = 0; i != row_groups.size(); ++i)
    {
        const auto & row_group = row_groups[i];
        /// file_offset
        assert_cast<ColumnUInt64 &>(row_groups_column.getColumn(0)).insertValue(row_group.file_offset);
        /// num_columns
        assert_cast<ColumnUInt64 &>(row_groups_column.getColumn(1)).insertValue(row_group.columns.size());
        /// num_rows
        assert_cast<ColumnUInt64 &>(row_groups_column.getColumn(2)).insertValue(row_group.num_rows);
        /// total_uncompressed_size
        assert_cast<ColumnUInt64 &>(row_groups_column.getColumn(3)).insertValue(row_group.total_byte_size);
        /// total_compressed_size
        assert_cast<ColumnUInt64 &>(row_groups_column.getColumn(4)).insertValue(row_group.total_compressed_size);
        /// columns
        fillColumnChunksMetadata(metadata, i, row_groups_column.getColumn(5));
    }
    row_groups_array_column.getOffsets().push_back(row_groups_column.size());
}

void ParquetMetadataInputFormat::fillColumnChunksMetadata(const Parquet::FooterMetadata & metadata, size_t row_group, IColumn & column)
{
    auto & array_column = assert_cast<ColumnArray &>(column);
    auto & tuple_column = assert_cast<ColumnTuple &>(array_column.getData());
    const size_t num_columns = metadata.metadata().row_groups[row_group].columns.size();
    for (size_t column_i = 0; column_i != num_columns; ++column_i)
    {
        const auto & chunk = metadata.columnChunk(row_group, column_i);
        const auto & column_info = metadata.columns()[column_i];
        /// name
        insertString(tuple_column.getColumn(0), column_info.name);
        /// path
        insertString(tuple_column.getColumn(1), column_info.path);
        /// total_compressed_size
        assert_cast<ColumnUInt64 &>(tuple_column.getColumn(2)).insertValue(chunk.total_compressed_size);
        /// total_uncompressed_size
        assert_cast<ColumnUInt64 &>(tuple_column.getColumn(3)).insertValue(chunk.total_uncompressed_size);
        /// have_statistics
        const auto statistics = metadata.statistics(row_group, column_i);
        assert_cast<ColumnUInt8 &>(tuple_column.getColumn(4)).insertValue(statistics.has_value());
        /// statistics
        if (statistics)
            fillColumnStatistics(metadata, column_i, *statistics, tuple_column.getColumn(5));
        else
            tuple_column.getColumn(5).insertDefault();
        /// bloom_filter_bytes
        assert_cast<ColumnInt64 &>(tuple_column.getColumn(6)).insertValue(chunk.__isset.bloom_filter_length ? chunk.bloom_filter_length : 0);
        /// A file written before SizeStatistics existed, or by a writer that omits it, has no size
        /// statistics at all; that is not the same as a column whose histograms are legitimately
        /// empty, so the presence is reported separately.
        assert_cast<ColumnUInt8 &>(tuple_column.getColumn(7)).insertValue(chunk.__isset.size_statistics);
        fillColumnSizeStatistics(metadata, row_group, column_i, tuple_column.getColumn(8));
        /// Present only for columns with the `GEOMETRY` or `GEOGRAPHY` logical type.
        assert_cast<ColumnUInt8 &>(tuple_column.getColumn(9)).insertValue(chunk.__isset.geospatial_statistics);
        fillColumnGeospatialStatistics(metadata, row_group, column_i, tuple_column.getColumn(10));
    }
    array_column.getOffsets().push_back(tuple_column.size());
}

void ParquetMetadataInputFormat::fillColumnGeospatialStatistics(
    const Parquet::FooterMetadata & metadata, size_t row_group, size_t column_index, IColumn & column)
{
    auto & geospatial_statistics_column = assert_cast<ColumnTuple &>(column);
    const auto & chunk = metadata.columnChunk(row_group, column_index);
    if (!chunk.__isset.geospatial_statistics)
    {
        geospatial_statistics_column.insertDefault();
        return;
    }

    const auto & geospatial_statistics = chunk.geospatial_statistics;
    const auto & bbox = geospatial_statistics.bbox;
    const bool has_xy = geospatial_statistics.__isset.bbox;
    const bool has_z = has_xy && bbox.__isset.zmin && bbox.__isset.zmax;
    const bool has_m = has_xy && bbox.__isset.mmin && bbox.__isset.mmax;
    /// The bounds are in XYZM order, the columns are xmin, xmax, ymin, ymax, ...
    const std::pair<double, double> bounds[] = {{bbox.xmin, bbox.xmax}, {bbox.ymin, bbox.ymax}, {bbox.zmin, bbox.zmax}, {bbox.mmin, bbox.mmax}};
    const bool present[] = {has_xy, has_xy, has_z, has_m};
    for (size_t dimension = 0; dimension < 4; ++dimension)
    {
        const auto [lower, upper] = bounds[dimension];
        const bool valid = present[dimension] && !std::isnan(lower) && !std::isnan(upper);
        insertNullable<Float64>(geospatial_statistics_column.getColumn(dimension * 2), valid ? std::optional<Float64>(lower) : std::nullopt);
        insertNullable<Float64>(geospatial_statistics_column.getColumn(dimension * 2 + 1), valid ? std::optional<Float64>(upper) : std::nullopt);
    }

    auto & types_column = assert_cast<ColumnArray &>(geospatial_statistics_column.getColumn(8));
    auto & types_data = assert_cast<ColumnInt32 &>(types_column.getData());
    auto geometry_types = geospatial_statistics.geospatial_types;
    std::sort(geometry_types.begin(), geometry_types.end());
    geometry_types.erase(std::unique(geometry_types.begin(), geometry_types.end()), geometry_types.end());
    for (const auto geometry_type : geometry_types)
        types_data.insertValue(geometry_type);
    types_column.getOffsets().push_back(types_data.size());
}

void ParquetMetadataInputFormat::fillColumnSizeStatistics(
    const Parquet::FooterMetadata & metadata, size_t row_group, size_t column_index, IColumn & column)
{
    auto & size_statistics_column = assert_cast<ColumnTuple &>(column);
    const auto & chunk = metadata.columnChunk(row_group, column_index);
    const auto * size_statistics = chunk.__isset.size_statistics ? &chunk.size_statistics : nullptr;

    insertNullable<UInt64>(
        size_statistics_column.getColumn(0),
        size_statistics && size_statistics->__isset.unencoded_byte_array_data_bytes
            ? std::optional<UInt64>(static_cast<UInt64>(size_statistics->unencoded_byte_array_data_bytes))
            : std::nullopt);

    auto insert_histogram = [&](const auto * histogram, IColumn & histogram_column)
    {
        auto & array_column = assert_cast<ColumnArray &>(histogram_column);
        auto & data_column = assert_cast<ColumnUInt64 &>(array_column.getData());
        if (histogram)
        {
            for (const auto level_count : *histogram)
                data_column.insertValue(static_cast<UInt64>(level_count));
        }
        array_column.getOffsets().push_back(data_column.size());
    };

    insert_histogram(size_statistics ? &size_statistics->repetition_level_histogram : nullptr, size_statistics_column.getColumn(1));
    insert_histogram(size_statistics ? &size_statistics->definition_level_histogram : nullptr, size_statistics_column.getColumn(2));
}

void ParquetMetadataInputFormat::fillColumnStatistics(
    const Parquet::FooterMetadata & metadata, size_t column_index, const Parquet::FooterMetadata::Statistics & statistics, IColumn & column)
{
    auto & statistics_column = assert_cast<ColumnTuple &>(column);
    /// num_values
    insertNullable<UInt64>(statistics_column.getColumn(0), static_cast<UInt64>(statistics.num_values));
    /// null_count
    insertNullable<UInt64>(statistics_column.getColumn(1),
        statistics.null_count ? std::optional<UInt64>(static_cast<UInt64>(*statistics.null_count)) : std::nullopt);
    /// distinct_count. It can be set but still be 0 because of a bug: https://github.com/apache/arrow/issues/27644
    /// If we see distinct_count = 0 with non 0 values in chunk, set it to NULL
    std::optional<UInt64> distinct_count;
    if (statistics.distinct_count && !(*statistics.distinct_count == 0 && statistics.num_values != 0))
        distinct_count = static_cast<UInt64>(*statistics.distinct_count);
    insertNullable<UInt64>(statistics_column.getColumn(2), distinct_count);
    /// min/max
    if (statistics.min && statistics.max)
    {
        insertNullable<String>(statistics_column.getColumn(3), metadata.formatStatisticsValue(column_index, *statistics.min));
        insertNullable<String>(statistics_column.getColumn(4), metadata.formatStatisticsValue(column_index, *statistics.max));
    }
    else
    {
        statistics_column.getColumn(3).insertDefault();
        statistics_column.getColumn(4).insertDefault();
    }
}

void ParquetMetadataInputFormat::resetParser()
{
    IInputFormat::resetParser();
    done = false;
}

ParquetMetadataSchemaReader::ParquetMetadataSchemaReader(ReadBuffer & in_)
    : ISchemaReader(in_)
{
}

NamesAndTypesList ParquetMetadataSchemaReader::readSchema()
{
    return getHeaderForParquetMetadata();
}

void registerInputFormatParquetMetadata(FormatFactory & factory);
void registerInputFormatParquetMetadata(FormatFactory & factory)
{
    factory.registerRandomAccessInputFormat(
        "ParquetMetadata",
        [](ReadBuffer & buf,
           const Block & sample,
           const FormatSettings & settings,
           const ReadSettings &,
           bool /* is_remote_fs */,
           FormatParserSharedResourcesPtr,
           FormatFilterInfoPtr) -> InputFormatPtr
        { return std::make_shared<ParquetMetadataInputFormat>(buf, std::make_shared<const Block>(sample), settings); },
        /*supports_reset_parser=*/ true);
    factory.markFormatSupportsSubsetOfColumns("ParquetMetadata");

    factory.setDocumentation("ParquetMetadata", Documentation{
        .description = R"DOCS_MD(
## Description {#description}

Special format for reading Parquet file metadata (https://parquet.apache.org/docs/file-format/metadata/). It always outputs one row with the next structure/content:
- `num_columns` - the number of columns
- ``num_rows` - the total number of rows
- `num_row_groups` - the total number of row groups
- `format_version` - parquet format version, always 1.0 or 2.6
- `total_uncompressed_size` - total uncompressed bytes size of the data, calculated as the sum of total_byte_size from all row groups
- `total_compressed_size` - total compressed bytes size of the data, calculated as the sum of total_compressed_size from all row groups
- `columns` - the list of columns metadata with the next structure:
  - `name` - column name
  - `path` - column path (differs from name for nested column)
  - `max_definition_level` - maximum definition level
  - `max_repetition_level` - maximum repetition level
  - `physical_type` - column physical type
  - `logical_type` - column logical type
  - `compression` - compression used for this column
  - `total_uncompressed_size` - total uncompressed bytes size of the column, calculated as the sum of total_uncompressed_size of the column from all row groups
  - `total_compressed_size` - total compressed bytes size of the column,  calculated as the sum of total_compressed_size of the column from all row groups
  - `space_saved` - percent of space saved by compression, calculated as (1 - total_compressed_size/total_uncompressed_size).
  - `encodings` - the list of encodings used for this column
- `row_groups` - the list of row groups metadata with the next structure:
  - `num_columns` - the number of columns in the row group
  - `num_rows` - the number of rows in the row group
  - `total_uncompressed_size` - total uncompressed bytes size of the row group
  - `total_compressed_size` - total compressed bytes size of the row group
  - `columns` - the list of column chunks metadata with the next structure:
    - `name` - column name
    - `path` - column path
    - `total_compressed_size` - total compressed bytes size of the column
    - `total_uncompressed_size` - total uncompressed bytes size of the row group
    - `have_statistics` - boolean flag that indicates if column chunk metadata contains column statistics
    - `statistics` - column chunk statistics (all fields are NULL if have_statistics = false) with the next structure:
      - `num_values` - the number of non-null values in the column chunk
      - `null_count` - the number of NULL values in the column chunk
      - `distinct_count` - the number of distinct values in the column chunk
      - `min` - the minimum value of the column chunk
      - `max` - the maximum column of the column chunk
    - `have_size_statistics` - boolean flag that indicates if column chunk metadata contains size statistics
    - `size_statistics` - column chunk size statistics (all fields are NULL or empty if have_size_statistics = false) with the next structure:
      - `unencoded_byte_array_data_bytes` - the total size of the unencoded byte array values as recorded in the footer, NULL when the writer did not record it
      - `repetition_level_histogram` - the number of values at each repetition level as recorded in the footer, empty when the writer did not record it
      - `definition_level_histogram` - the number of values at each definition level as recorded in the footer, empty when the writer did not record it
    - `have_geospatial_statistics` - boolean flag that indicates if column chunk metadata contains geospatial statistics, which are written only for columns with the `GEOMETRY` or `GEOGRAPHY` logical type
    - `geospatial_statistics` - column chunk geospatial statistics (all fields are NULL or empty if have_geospatial_statistics = false) with the next structure:
      - `xmin`, `xmax`, `ymin`, `ymax`, `zmin`, `zmax`, `mmin`, `mmax` - the bounding box of the geometries in the column chunk, NULL for the dimensions that the writer did not record
      - `geospatial_types` - the WKB (ISO) codes of the geometry types in the column chunk (for example, 1 for `Point`, 3 for `Polygon`, 1001 for `Point Z`), empty when unknown

## Example usage {#example-usage}

Example:

```sql
SELECT * 
FROM file(data.parquet, ParquetMetadata) 
FORMAT PrettyJSONEachRow
```

```json
{
    "num_columns": "2",
    "num_rows": "100000",
    "num_row_groups": "2",
    "format_version": "2.6",
    "metadata_size": "577",
    "total_uncompressed_size": "282436",
    "total_compressed_size": "26633",
    "columns": [
        {
            "name": "number",
            "path": "number",
            "max_definition_level": "0",
            "max_repetition_level": "0",
            "physical_type": "INT32",
            "logical_type": "Int(bitWidth=16, isSigned=false)",
            "compression": "LZ4",
            "total_uncompressed_size": "133321",
            "total_compressed_size": "13293",
            "space_saved": "90.03%",
            "encodings": [
                "RLE_DICTIONARY",
                "PLAIN",
                "RLE"
            ]
        },
        {
            "name": "concat('Hello', toString(modulo(number, 1000)))",
            "path": "concat('Hello', toString(modulo(number, 1000)))",
            "max_definition_level": "0",
            "max_repetition_level": "0",
            "physical_type": "BYTE_ARRAY",
            "logical_type": "None",
            "compression": "LZ4",
            "total_uncompressed_size": "149115",
            "total_compressed_size": "13340",
            "space_saved": "91.05%",
            "encodings": [
                "RLE_DICTIONARY",
                "PLAIN",
                "RLE"
            ]
        }
    ],
    "row_groups": [
        {
            "num_columns": "2",
            "num_rows": "65409",
            "total_uncompressed_size": "179809",
            "total_compressed_size": "14163",
            "columns": [
                {
                    "name": "number",
                    "path": "number",
                    "total_compressed_size": "7070",
                    "total_uncompressed_size": "85956",
                    "have_statistics": true,
                    "statistics": {
                        "num_values": "65409",
                        "null_count": "0",
                        "distinct_count": null,
                        "min": "0",
                        "max": "999"
                    }
                },
                {
                    "name": "concat('Hello', toString(modulo(number, 1000)))",
                    "path": "concat('Hello', toString(modulo(number, 1000)))",
                    "total_compressed_size": "7093",
                    "total_uncompressed_size": "93853",
                    "have_statistics": true,
                    "statistics": {
                        "num_values": "65409",
                        "null_count": "0",
                        "distinct_count": null,
                        "min": "Hello0",
                        "max": "Hello999"
                    }
                }
            ]
        },
        ...
    ]
}
```
)DOCS_MD"});
}

void registerParquetMetadataSchemaReader(FormatFactory & factory);
void registerParquetMetadataSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "ParquetMetadata",
        [](ReadBuffer & buf, const FormatSettings &)
        {
            return std::make_shared<ParquetMetadataSchemaReader>(buf);
        }
    );
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatParquetMetadata(FormatFactory &);
void registerParquetMetadataSchemaReader(FormatFactory &);
void registerInputFormatParquetMetadata(FormatFactory &)
{
}

void registerParquetMetadataSchemaReader(FormatFactory &) {}
}

#endif
