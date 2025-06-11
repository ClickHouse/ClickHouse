#include "ParquetMetadataInputFormat.h"

#if USE_PARQUET

#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromMemory.h>
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
#include <arrow/api.h>
#include <arrow/status.h>
#include <parquet/file_reader.h>
#include <parquet/statistics.h>
#include "ArrowBufferedStreams.h"
#include <DataTypes/NestedUtils.h>


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
                         },
                         Names{"name", "path", "total_compressed_size", "total_uncompressed_size", "have_statistics", "statistics", "bloom_filter_bytes"}))},
             Names{"file_offset", "num_columns", "num_rows", "total_uncompressed_size", "total_compressed_size", "columns"}))},
    };
    return names_and_types;
}

void checkHeader(const Block & header)
{
    auto expected_names_and_types = getHeaderForParquetMetadata();
    std::unordered_map<String, DataTypePtr> name_to_type;
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

static std::shared_ptr<parquet::FileMetaData> getFileMetadata(
    ReadBuffer & in,
    const FormatSettings & format_settings,
    std::atomic<int> & is_stopped)
{
    auto arrow_file = asArrowFile(in, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES, /* avoid_buffering */ true);
    return parquet::ReadMetaData(arrow_file);
}

ParquetMetadataInputFormat::ParquetMetadataInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), &in_), format_settings(format_settings_)
{
    checkHeader(getPort().getHeader());
}

Chunk ParquetMetadataInputFormat::read()
{
    Chunk res;
    if (done)
        return res;

    auto metadata = getFileMetadata(*in, format_settings, is_stopped);

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
            assert_cast<ColumnUInt64 &>(*column).insertValue(metadata->num_columns());
            res.addColumn(std::move(column));
        }
        /// num_rows
        else if (name == names[1])
        {
            auto column = types[1]->createColumn();
            assert_cast<ColumnUInt64 &>(*column).insertValue(metadata->num_rows());
            res.addColumn(std::move(column));
        }
        /// num_row_groups
        else if (name == names[2])
        {
            auto column = types[2]->createColumn();
            assert_cast<ColumnUInt64 &>(*column).insertValue(metadata->num_row_groups());
            res.addColumn(std::move(column));
        }
        /// format_version
        else if (name == names[3])
        {
            auto column = types[3]->createColumn();
            /// Parquet file doesn't know its exact version, only whether it's 1.x or 2.x
            /// (FileMetaData.version = 1 or 2).
            String version = metadata->version() == parquet::ParquetVersion::PARQUET_1_0 ? "1" : "2";
            assert_cast<ColumnString &>(*column).insertData(version.data(), version.size());
            res.addColumn(std::move(column));
        }
        /// metadata_size
        else if (name == names[4])
        {
            auto column = types[4]->createColumn();
            assert_cast<ColumnUInt64 &>(*column).insertValue(metadata->size());
            res.addColumn(std::move(column));
        }
        /// total_uncompressed_size
        else if (name == names[5])
        {
            auto column = types[5]->createColumn();
            size_t total_uncompressed_size = 0;
            for (int32_t i = 0; i != metadata->num_row_groups(); ++i)
                total_uncompressed_size += metadata->RowGroup(i)->total_byte_size();

            assert_cast<ColumnUInt64 &>(*column).insertValue(total_uncompressed_size);
            res.addColumn(std::move(column));
        }
        /// total_compressed_size
        else if (name == names[6])
        {
            auto column = types[6]->createColumn();
            size_t total_compressed_size = 0;
            for (int32_t i = 0; i != metadata->num_row_groups(); ++i)
                total_compressed_size += metadata->RowGroup(i)->total_compressed_size();

            assert_cast<ColumnUInt64 &>(*column).insertValue(total_compressed_size);
            res.addColumn(std::move(column));
        }
        /// columns
        else if (name == names[7])
        {
            auto column = types[7]->createColumn();
            fillColumnsMetadata(metadata, column);
            res.addColumn(std::move(column));
        }
        /// row_groups
        else if (name == names[8])
        {
            auto column = types[8]->createColumn();
            fillRowGroupsMetadata(metadata, column);
            res.addColumn(std::move(column));
        }
    }

    done = true;
    return res;
}

void ParquetMetadataInputFormat::fillColumnsMetadata(const std::shared_ptr<parquet::FileMetaData> & metadata, MutableColumnPtr & column)
{
    auto & array_column = assert_cast<ColumnArray &>(*column);
    auto & tuple_column = assert_cast<ColumnTuple &>(array_column.getData());
    int32_t num_columns = metadata->num_columns();
    for (int32_t column_i = 0; column_i != num_columns; ++column_i)
    {
        const auto * column_info = metadata->schema()->Column(column_i);
        /// name
        String column_name = column_info->name();
        assert_cast<ColumnString &>(tuple_column.getColumn(0)).insertData(column_name.data(), column_name.size());
        /// path
        String path = column_info->path()->ToDotString();
        assert_cast<ColumnString &>(tuple_column.getColumn(1)).insertData(path.data(), path.size());
        /// max_definition_level
        assert_cast<ColumnUInt64 &>(tuple_column.getColumn(2)).insertValue(column_info->max_definition_level());
        /// max_repetition_level
        assert_cast<ColumnUInt64 &>(tuple_column.getColumn(3)).insertValue(column_info->max_repetition_level());
        /// physical_type
        std::string_view physical_type = magic_enum::enum_name(column_info->physical_type());
        assert_cast<ColumnString &>(tuple_column.getColumn(4)).insertData(physical_type.data(), physical_type.size());
        /// logical_type
        String logical_type = column_info->logical_type()->ToString();
        assert_cast<ColumnString &>(tuple_column.getColumn(5)).insertData(logical_type.data(), logical_type.size());

        if (metadata->num_row_groups() > 0)
        {
            auto column_chunk_metadata = metadata->RowGroup(0)->ColumnChunk(column_i);
            /// compression
            std::string_view compression = magic_enum::enum_name(column_chunk_metadata->compression());
            assert_cast<ColumnString &>(tuple_column.getColumn(6)).insertData(compression.data(), compression.size());

            /// total_uncompressed_size/total_compressed_size
            size_t total_uncompressed_size = 0;
            size_t total_compressed_size = 0;
            for (int32_t row_group_i = 0; row_group_i != metadata->num_row_groups(); ++row_group_i)
            {
                column_chunk_metadata = metadata->RowGroup(row_group_i)->ColumnChunk(column_i);
                total_uncompressed_size += column_chunk_metadata->total_uncompressed_size();
                total_compressed_size += column_chunk_metadata->total_compressed_size();
            }
            assert_cast<ColumnUInt64 &>(tuple_column.getColumn(7)).insertValue(total_uncompressed_size);
            assert_cast<ColumnUInt64 &>(tuple_column.getColumn(8)).insertValue(total_compressed_size);

            /// space_saved
            String space_saved = fmt::format("{:.4}%", (1 - double(total_compressed_size) / total_uncompressed_size) * 100);
            assert_cast<ColumnString &>(tuple_column.getColumn(9)).insertData(space_saved.data(), space_saved.size());

            /// encodings
            auto & encodings_array_column = assert_cast<ColumnArray &>(tuple_column.getColumn(10));
            auto & encodings_nested_column = assert_cast<ColumnString &>(encodings_array_column.getData());
            for (auto codec : column_chunk_metadata->encodings())
            {
                auto codec_name = magic_enum::enum_name(codec);
                encodings_nested_column.insertData(codec_name.data(), codec_name.size());
            }
            encodings_array_column.getOffsets().push_back(encodings_nested_column.size());
        }
        else
        {
            String compression = "NONE";
            assert_cast<ColumnString &>(tuple_column.getColumn(6)).insertData(compression.data(), compression.size());
            tuple_column.getColumn(7).insertDefault();
            tuple_column.getColumn(8).insertDefault();
            tuple_column.getColumn(9).insertDefault();
            tuple_column.getColumn(10).insertDefault();
        }
    }
    array_column.getOffsets().push_back(tuple_column.size());
}

void ParquetMetadataInputFormat::fillRowGroupsMetadata(const std::shared_ptr<parquet::FileMetaData> & metadata, MutableColumnPtr & column)
{
    auto & row_groups_array_column = assert_cast<ColumnArray &>(*column);
    auto & row_groups_column = assert_cast<ColumnTuple &>(row_groups_array_column.getData());
    for (int32_t i = 0; i != metadata->num_row_groups(); ++i)
    {
        auto row_group_metadata = metadata->RowGroup(i);
        /// file_offset
        assert_cast<ColumnUInt64 &>(row_groups_column.getColumn(0)).insertValue(row_group_metadata->file_offset());
        /// num_columns
        assert_cast<ColumnUInt64 &>(row_groups_column.getColumn(1)).insertValue(row_group_metadata->num_columns());
        /// num_rows
        assert_cast<ColumnUInt64 &>(row_groups_column.getColumn(2)).insertValue(row_group_metadata->num_rows());
        /// total_uncompressed_size
        assert_cast<ColumnUInt64 &>(row_groups_column.getColumn(3)).insertValue(row_group_metadata->total_byte_size());
        /// total_compressed_size
        assert_cast<ColumnUInt64 &>(row_groups_column.getColumn(4)).insertValue(row_group_metadata->total_compressed_size());
        /// columns
        fillColumnChunksMetadata(row_group_metadata, row_groups_column.getColumn(5));
    }
    row_groups_array_column.getOffsets().push_back(row_groups_column.size());
}

void ParquetMetadataInputFormat::fillColumnChunksMetadata(const std::unique_ptr<parquet::RowGroupMetaData> & row_group_metadata, IColumn & column)
{
    auto & array_column = assert_cast<ColumnArray &>(column);
    auto & tuple_column = assert_cast<ColumnTuple &>(array_column.getData());
    for (int32_t column_i = 0; column_i != row_group_metadata->num_columns(); ++column_i)
    {
        auto column_chunk_metadata = row_group_metadata->ColumnChunk(column_i);
        /// name
        String column_name = row_group_metadata->schema()->Column(column_i)->name();
        assert_cast<ColumnString &>(tuple_column.getColumn(0)).insertData(column_name.data(), column_name.size());
        /// path
        String path = row_group_metadata->schema()->Column(column_i)->path()->ToDotString();
        assert_cast<ColumnString &>(tuple_column.getColumn(1)).insertData(path.data(), path.size());
        /// total_compressed_size
        assert_cast<ColumnUInt64 &>(tuple_column.getColumn(2)).insertValue(column_chunk_metadata->total_compressed_size());
        /// total_uncompressed_size
        assert_cast<ColumnUInt64 &>(tuple_column.getColumn(3)).insertValue(column_chunk_metadata->total_uncompressed_size());
        /// have_statistics
        bool have_statistics = column_chunk_metadata->is_stats_set();
        assert_cast<ColumnUInt8 &>(tuple_column.getColumn(4)).insertValue(have_statistics);
        if (have_statistics)
            fillColumnStatistics(column_chunk_metadata->statistics(), tuple_column.getColumn(5), row_group_metadata->schema()->Column(column_i)->type_length());
        else
            tuple_column.getColumn(5).insertDefault();
        assert_cast<ColumnInt64 &>(tuple_column.getColumn(6)).insertValue(column_chunk_metadata->bloom_filter_length().value_or(0));
    }
    array_column.getOffsets().push_back(tuple_column.size());
}

template <typename T>
static void getMinMaxNumberStatistics(const std::shared_ptr<parquet::Statistics> & statistics, String & min, String & max)
{
    const auto & typed_statistics = dynamic_cast<parquet::TypedStatistics<T> &>(*statistics);
    min = std::to_string(typed_statistics.min());
    max = std::to_string(typed_statistics.max());
}

void ParquetMetadataInputFormat::fillColumnStatistics(const std::shared_ptr<parquet::Statistics> & statistics, IColumn & column, int32_t type_length)
{
    auto & statistics_column = assert_cast<ColumnTuple &>(column);
    /// num_values
    auto & nullable_num_values = assert_cast<ColumnNullable &>(statistics_column.getColumn(0));
    assert_cast<ColumnUInt64 &>(nullable_num_values.getNestedColumn()).insertValue(statistics->num_values());
    nullable_num_values.getNullMapData().push_back(0);

    /// null_count
    if (statistics->HasNullCount())
    {
        auto & nullable_null_count = assert_cast<ColumnNullable &>(statistics_column.getColumn(1));
        assert_cast<ColumnUInt64 &>(nullable_null_count.getNestedColumn()).insertValue(statistics->null_count());
        nullable_null_count.getNullMapData().push_back(0);
    }
    else
    {
        statistics_column.getColumn(1).insertDefault();
    }

    /// distinct_count
    if (statistics->HasDistinctCount())
    {
        auto & nullable_distinct_count = assert_cast<ColumnNullable &>(statistics_column.getColumn(2));
        size_t distinct_count = statistics->distinct_count();
        /// It can be set but still be 0 because of a bug: https://github.com/apache/arrow/issues/27644
        /// If we see distinct_count = 0 with non 0 values in chunk, set it to NULL
        if (distinct_count == 0 && statistics->num_values() != 0)
        {
            nullable_distinct_count.insertDefault();
        }
        else
        {
            assert_cast<ColumnUInt64 &>(nullable_distinct_count.getNestedColumn()).insertValue(distinct_count);
            nullable_distinct_count.getNullMapData().push_back(0);
        }
    }
    else
    {
        statistics_column.getColumn(2).insertDefault();
    }

    /// min/max
    if (statistics->HasMinMax() && statistics->physical_type() != parquet::Type::type::UNDEFINED)
    {
        String min;
        String max;
        switch (statistics->physical_type())
        {
            case parquet::Type::type::FLOAT:
            {
                getMinMaxNumberStatistics<parquet::FloatType>(statistics, min, max);
                break;
            }
            case parquet::Type::type::DOUBLE:
            {
                getMinMaxNumberStatistics<parquet::DoubleType>(statistics, min, max);
                break;
            }
            case parquet::Type::type::INT32:
            {
                getMinMaxNumberStatistics<parquet::Int32Type>(statistics, min, max);
                break;
            }
            case parquet::Type::type::INT64:
            {
                getMinMaxNumberStatistics<parquet::Int64Type>(statistics, min, max);
                break;
            }
            case parquet::Type::type::INT96:
            {
                const auto & int96_statistics = dynamic_cast<parquet::TypedStatistics<parquet::Int96Type> &>(*statistics);
                min = parquet::Int96ToString(int96_statistics.min());
                max = parquet::Int96ToString(int96_statistics.max());
                break;
            }
            case parquet::Type::type::BOOLEAN:
            {
                getMinMaxNumberStatistics<parquet::BooleanType>(statistics, min, max);
                break;
            }
            case parquet::Type::type::BYTE_ARRAY:
            {
                const auto & byte_array_statistics = dynamic_cast<parquet::ByteArrayStatistics &>(*statistics);
                min = parquet::ByteArrayToString(byte_array_statistics.min());
                max = parquet::ByteArrayToString(byte_array_statistics.max());
                break;
            }
            case parquet::Type::type::FIXED_LEN_BYTE_ARRAY:
            {
                const auto & flba_statistics = dynamic_cast<parquet::FLBAStatistics &>(*statistics);
                min = parquet::FixedLenByteArrayToString(flba_statistics.min(), type_length);
                max = parquet::FixedLenByteArrayToString(flba_statistics.max(), type_length);
                break;
            }
            case parquet::Type::type::UNDEFINED:
            {
                break; /// unreachable
            }
        }

        auto & nullable_min = assert_cast<ColumnNullable &>(statistics_column.getColumn(3));
        assert_cast<ColumnString &>(nullable_min.getNestedColumn()).insertData(min.data(), min.size());
        nullable_min.getNullMapData().push_back(0);
        auto & nullable_max = assert_cast<ColumnNullable &>(statistics_column.getColumn(4));
        assert_cast<ColumnString &>(nullable_max.getNestedColumn()).insertData(max.data(), max.size());
        nullable_max.getNullMapData().push_back(0);
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

void registerInputFormatParquetMetadata(FormatFactory & factory)
{
    factory.registerRandomAccessInputFormat(
        "ParquetMetadata",
        [](ReadBuffer & buf,
            const Block & sample,
            const FormatSettings & settings,
            const ReadSettings &,
            bool /* is_remote_fs */,
            size_t /* max_download_threads */,
            size_t /* max_parsing_threads */)
        {
            return std::make_shared<ParquetMetadataInputFormat>(buf, sample, settings);
        });
    factory.markFormatSupportsSubsetOfColumns("ParquetMetadata");
}

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
void registerInputFormatParquetMetadata(FormatFactory &)
{
}

void registerParquetMetadataSchemaReader(FormatFactory &) {}
}

#endif
