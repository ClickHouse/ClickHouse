#include "ParquetRecordReader.h"

#include <bit>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/logger_useful.h>
#include <Common/Stopwatch.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/castColumn.h>

#include <arrow/status.h>
#include <parquet/arrow/reader.h>
#include <parquet/column_reader.h>
#include <parquet/properties.h>

#include "ParquetLeafColReader.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int PARQUET_EXCEPTION;
}

#define THROW_PARQUET_EXCEPTION(s)                                            \
    do                                                                        \
    {                                                                         \
        try { (s); }                                                          \
        catch (const ::parquet::ParquetException & e)                         \
        {                                                                     \
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Parquet exception: {}", e.what());   \
        }                                                                     \
    } while (false)

namespace
{

std::unique_ptr<parquet::ParquetFileReader> createFileReader(
    std::shared_ptr<::arrow::io::RandomAccessFile> arrow_file,
    parquet::ReaderProperties reader_properties,
    std::shared_ptr<parquet::FileMetaData> metadata = nullptr)
{
    std::unique_ptr<parquet::ParquetFileReader> res;
    THROW_PARQUET_EXCEPTION(res = parquet::ParquetFileReader::Open(
            std::move(arrow_file),
            reader_properties,
            metadata));
    return res;
}

class ColReaderFactory
{
public:
    ColReaderFactory(
        const parquet::ArrowReaderProperties & arrow_properties_,
        const parquet::ColumnDescriptor & col_descriptor_,
        DataTypePtr ch_type_,
        std::unique_ptr<parquet::ColumnChunkMetaData> meta_,
        std::unique_ptr<parquet::PageReader> page_reader_)
        : arrow_properties(arrow_properties_)
        , col_descriptor(col_descriptor_)
        , ch_type(std::move(ch_type_))
        , meta(std::move(meta_))
        , page_reader(std::move(page_reader_)) {}

    std::unique_ptr<ParquetColumnReader> makeReader();

private:
    const parquet::ArrowReaderProperties & arrow_properties;
    const parquet::ColumnDescriptor & col_descriptor;
    DataTypePtr ch_type;
    std::unique_ptr<parquet::ColumnChunkMetaData> meta;
    std::unique_ptr<parquet::PageReader> page_reader;


    UInt32 getScaleFromLogicalTimestamp(parquet::LogicalType::TimeUnit::unit tm_unit);
    UInt32 getScaleFromArrowTimeUnit(arrow::TimeUnit::type tm_unit);

    std::unique_ptr<ParquetColumnReader> fromInt32();
    std::unique_ptr<ParquetColumnReader> fromInt64();
    std::unique_ptr<ParquetColumnReader> fromByteArray();
    std::unique_ptr<ParquetColumnReader> fromFLBA();

    std::unique_ptr<ParquetColumnReader> fromInt32INT(const parquet::IntLogicalType & int_type);
    std::unique_ptr<ParquetColumnReader> fromInt64INT(const parquet::IntLogicalType & int_type);

    template<class DataType>
    auto makeLeafReader()
    {
        return std::make_unique<ParquetLeafColReader<typename DataType::ColumnType>>(
            col_descriptor, std::make_shared<DataType>(), std::move(meta), std::move(page_reader));
    }

    template<class DecimalType>
    auto makeDecimalLeafReader()
    {
        auto data_type = std::make_shared<DataTypeDecimal<DecimalType>>(
            col_descriptor.type_precision(), col_descriptor.type_scale());
        return std::make_unique<ParquetLeafColReader<ColumnDecimal<DecimalType>>>(
            col_descriptor, std::move(data_type), std::move(meta), std::move(page_reader));
    }

    std::unique_ptr<ParquetColumnReader> throwUnsupported(std::string msg = "")
    {
        throw Exception(
            ErrorCodes::PARQUET_EXCEPTION,
            "Unsupported logical type: {} and physical type: {} for field `{}`{}",
            col_descriptor.logical_type()->ToString(), col_descriptor.physical_type(), col_descriptor.name(), msg);
    }
};

UInt32 ColReaderFactory::getScaleFromLogicalTimestamp(parquet::LogicalType::TimeUnit::unit tm_unit)
{
    switch (tm_unit)
    {
        case parquet::LogicalType::TimeUnit::MILLIS:
            return 3;
        case parquet::LogicalType::TimeUnit::MICROS:
            return 6;
        case parquet::LogicalType::TimeUnit::NANOS:
            return 9;
        default:
            throwUnsupported(PreformattedMessage::create(", invalid timestamp unit: {}", tm_unit));
            return 0;
    }
}

UInt32 ColReaderFactory::getScaleFromArrowTimeUnit(arrow::TimeUnit::type tm_unit)
{
    switch (tm_unit)
    {
        case arrow::TimeUnit::MILLI:
            return 3;
        case arrow::TimeUnit::MICRO:
            return 6;
        case arrow::TimeUnit::NANO:
            return 9;
        default:
            throwUnsupported(PreformattedMessage::create(", invalid arrow time unit: {}", tm_unit));
            return 0;
    }
}

std::unique_ptr<ParquetColumnReader> ColReaderFactory::fromInt32()
{
    switch (col_descriptor.logical_type()->type())
    {
        case parquet::LogicalType::Type::INT:
            return fromInt32INT(dynamic_cast<const parquet::IntLogicalType &>(*col_descriptor.logical_type()));
        case parquet::LogicalType::Type::NONE:
            return makeLeafReader<DataTypeInt32>();
        case parquet::LogicalType::Type::DATE:
            return makeLeafReader<DataTypeDate32>();
        case parquet::LogicalType::Type::DECIMAL:
            return makeDecimalLeafReader<Decimal32>();
        default:
            return throwUnsupported();
    }
}

std::unique_ptr<ParquetColumnReader> ColReaderFactory::fromInt64()
{
    switch (col_descriptor.logical_type()->type())
    {
        case parquet::LogicalType::Type::INT:
            return fromInt64INT(dynamic_cast<const parquet::IntLogicalType &>(*col_descriptor.logical_type()));
        case parquet::LogicalType::Type::NONE:
            return makeLeafReader<DataTypeInt64>();
        case parquet::LogicalType::Type::TIMESTAMP:
        {
            const auto & tm_type = dynamic_cast<const parquet::TimestampLogicalType &>(*col_descriptor.logical_type());
            auto read_type = std::make_shared<DataTypeDateTime64>(getScaleFromLogicalTimestamp(tm_type.time_unit()));
            return std::make_unique<ParquetLeafColReader<ColumnDecimal<DateTime64>>>(
                col_descriptor, std::move(read_type), std::move(meta), std::move(page_reader));
        }
        case parquet::LogicalType::Type::DECIMAL:
            return makeDecimalLeafReader<Decimal64>();
        default:
            return throwUnsupported();
    }
}

std::unique_ptr<ParquetColumnReader> ColReaderFactory::fromByteArray()
{
    switch (col_descriptor.logical_type()->type())
    {
        case parquet::LogicalType::Type::STRING:
        case parquet::LogicalType::Type::NONE:
            return makeLeafReader<DataTypeString>();
        default:
            return throwUnsupported();
    }
}

std::unique_ptr<ParquetColumnReader> ColReaderFactory::fromFLBA()
{
    switch (col_descriptor.logical_type()->type())
    {
        case parquet::LogicalType::Type::DECIMAL:
        {
            if (col_descriptor.type_length() > 0)
            {
                if (col_descriptor.type_length() <= static_cast<int>(sizeof(Decimal128)))
                    return makeDecimalLeafReader<Decimal128>();
                if (col_descriptor.type_length() <= static_cast<int>(sizeof(Decimal256)))
                    return makeDecimalLeafReader<Decimal256>();
            }

            return throwUnsupported(PreformattedMessage::create(
                ", invalid type length: {}", col_descriptor.type_length()));
        }
        default:
            return throwUnsupported();
    }
}

std::unique_ptr<ParquetColumnReader> ColReaderFactory::fromInt32INT(const parquet::IntLogicalType & int_type)
{
    switch (int_type.bit_width())
    {
        case 32:
        {
            if (int_type.is_signed())
                return makeLeafReader<DataTypeInt32>();
            return makeLeafReader<DataTypeUInt32>();
        }
        default:
            return throwUnsupported(PreformattedMessage::create(", bit width: {}", int_type.bit_width()));
    }
}

std::unique_ptr<ParquetColumnReader> ColReaderFactory::fromInt64INT(const parquet::IntLogicalType & int_type)
{
    switch (int_type.bit_width())
    {
        case 64:
        {
            if (int_type.is_signed())
                return makeLeafReader<DataTypeInt64>();
            return makeLeafReader<DataTypeUInt64>();
        }
        default:
            return throwUnsupported(PreformattedMessage::create(", bit width: {}", int_type.bit_width()));
    }
}

// refer: GetArrowType method in schema_internal.cc of arrow
std::unique_ptr<ParquetColumnReader> ColReaderFactory::makeReader()
{
    // this method should to be called only once for each instance
    SCOPE_EXIT({ page_reader = nullptr; });
    assert(page_reader);

    switch (col_descriptor.physical_type())
    {
        case parquet::Type::BOOLEAN:
            break;
        case parquet::Type::INT32:
            return fromInt32();
        case parquet::Type::INT64:
            return fromInt64();
        case parquet::Type::INT96:
        {
            DataTypePtr read_type = ch_type;
            if (!isDateTime64(ch_type))
            {
                auto scale = getScaleFromArrowTimeUnit(arrow_properties.coerce_int96_timestamp_unit());
                read_type = std::make_shared<DataTypeDateTime64>(scale);
            }
            return std::make_unique<ParquetLeafColReader<ColumnDecimal<DateTime64>>>(
                col_descriptor, read_type, std::move(meta), std::move(page_reader));
        }
        case parquet::Type::FLOAT:
            return makeLeafReader<DataTypeFloat32>();
        case parquet::Type::DOUBLE:
            return makeLeafReader<DataTypeFloat64>();
        case parquet::Type::BYTE_ARRAY:
            return fromByteArray();
        case parquet::Type::FIXED_LEN_BYTE_ARRAY:
            return fromFLBA();
        default:
            break;
    }

    return throwUnsupported();
}

} // anonymous namespace

ParquetRecordReader::ParquetRecordReader(
    Block header_,
    parquet::ArrowReaderProperties arrow_properties_,
    parquet::ReaderProperties reader_properties_,
    std::shared_ptr<::arrow::io::RandomAccessFile> arrow_file,
    const FormatSettings & format_settings,
    std::vector<int> row_groups_indices_,
    std::shared_ptr<parquet::FileMetaData> metadata)
    : file_reader(createFileReader(std::move(arrow_file), reader_properties_, std::move(metadata)))
    , arrow_properties(arrow_properties_)
    , header(std::move(header_))
    , max_block_size(format_settings.parquet.max_block_size)
    , row_groups_indices(std::move(row_groups_indices_))
    , left_rows(getTotalRows(*file_reader->metadata()))
{
    log = &Poco::Logger::get("ParquetRecordReader");

    std::unordered_map<String, parquet::schema::NodePtr> parquet_columns;
    const auto * root = file_reader->metadata()->schema()->group_node();
    for (int i = 0; i < root->field_count(); ++i)
    {
        const auto & node = root->field(i);
        parquet_columns.emplace(node->name(), node);
    }

    parquet_col_indice.reserve(header.columns());
    column_readers.reserve(header.columns());
    for (const auto & col_with_name : header)
    {
        auto it = parquet_columns.find(col_with_name.name);
        if (it == parquet_columns.end())
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "no column with '{}' in parquet file", col_with_name.name);

        const auto & node = it->second;
        if (!node->is_primitive())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "arrays and maps are not implemented in native parquet reader");

        auto idx = file_reader->metadata()->schema()->ColumnIndex(*node);
        chassert(idx >= 0);
        parquet_col_indice.push_back(idx);
    }
    if (arrow_properties.pre_buffer())
    {
        THROW_PARQUET_EXCEPTION(file_reader->PreBuffer(
            row_groups_indices, parquet_col_indice, arrow_properties.io_context(), arrow_properties.cache_options()));
    }
}

Chunk ParquetRecordReader::readChunk()
{
    if (!left_rows)
    {
        return Chunk{};
    }
    if (!cur_row_group_left_rows)
    {
        loadNextRowGroup();
    }

    Columns columns(header.columns());
    auto num_rows_read = std::min(max_block_size, cur_row_group_left_rows);
    for (size_t i = 0; i < header.columns(); i++)
    {
        columns[i] = castColumn(
            column_readers[i]->readBatch(num_rows_read, header.getByPosition(i).name),
            header.getByPosition(i).type);
    }
    left_rows -= num_rows_read;
    cur_row_group_left_rows -= num_rows_read;

    return Chunk{std::move(columns), num_rows_read};
}

void ParquetRecordReader::loadNextRowGroup()
{
    Stopwatch watch(CLOCK_MONOTONIC);
    cur_row_group_reader = file_reader->RowGroup(row_groups_indices[next_row_group_idx]);

    column_readers.clear();
    for (size_t i = 0; i < parquet_col_indice.size(); i++)
    {
        ColReaderFactory factory(
            arrow_properties,
            *file_reader->metadata()->schema()->Column(parquet_col_indice[i]),
            header.getByPosition(i).type,
            cur_row_group_reader->metadata()->ColumnChunk(parquet_col_indice[i]),
            cur_row_group_reader->GetColumnPageReader(parquet_col_indice[i]));
        column_readers.emplace_back(factory.makeReader());
    }

    auto duration = watch.elapsedNanoseconds() / 1e6;
    LOG_DEBUG(log, "begin to read row group {} consumed {} ms", row_groups_indices[next_row_group_idx], duration);

    ++next_row_group_idx;
    cur_row_group_left_rows = cur_row_group_reader->metadata()->num_rows();
}

Int64 ParquetRecordReader::getTotalRows(const parquet::FileMetaData & meta_data)
{
    Int64 res = 0;
    for (auto idx : row_groups_indices)
    {
        res += meta_data.RowGroup(idx)->num_rows();
    }
    return res;
}

}
