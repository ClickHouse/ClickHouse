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
#include <memory>
#include <numeric>

#include "ParquetLeafColReader.h"
#include <arrow/io/memory.h>

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
            "Unsupported logical type: {} and physical type: {} for field =={}=={}",
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
                else if (col_descriptor.type_length() <= static_cast<int>(sizeof(Decimal256)))
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
            else
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
            else
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
    std::shared_ptr<::arrow::io::RandomAccessFile> arrow_file_,
    const FormatSettings & format_settings,
    std::vector<int> row_groups_indices_,
    const std::vector<int> & column_indices_,
    std::shared_ptr<const KeyCondition> key_condition_,
    std::shared_ptr<parquet::FileMetaData> metadata)
    : arrow_file(std::move(arrow_file_))
    , file_reader(createFileReader(arrow_file, reader_properties_, std::move(metadata)))
    , arrow_properties(arrow_properties_)
    , header(std::move(header_))
    , max_block_size(format_settings.parquet.max_block_size)
    , row_groups_indices(std::move(row_groups_indices_))
    , column_index_filter(std::make_unique<ParquetFileColumnIndexFilter>(
          *file_reader,
          *arrow_file->GetSize(),
          column_indices_,
          format_settings.parquet.case_insensitive_column_matching,
          key_condition_))
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
    if (next_row_group_idx >= row_groups_indices.size() && current_row_group_finished)
    {
        return Chunk{};
    }
    if (current_row_group_finished)
    {
        loadNextRowGroup();
    }

    Columns columns(header.columns());
    //auto num_rows_read = std::min(max_block_size, cur_row_group_left_rows);
    auto num_rows_read = max_block_size;
    bool has_any_col_finished = false;
    bool has_any_col_unfinished = false;
    for (size_t i = 0; i < header.columns(); i++)
    {
        columns[i] = castColumn(
            column_readers[i]->readBatch(num_rows_read, header.getByPosition(i).name),
            header.getByPosition(i).type);
        has_any_col_finished |= !column_readers[i]->hasMoreToRead();
        has_any_col_unfinished |= column_readers[i]->hasMoreToRead();
    }
    if (has_any_col_finished == has_any_col_unfinished)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Some columns are finished, but some are not");
    }
    current_row_group_finished = !has_any_col_unfinished;
    // Each column should read the same number rows.
    auto read_rows = columns[0]->size();
    return Chunk{std::move(columns), read_rows};

}

void ParquetRecordReader::loadNextRowGroup()
{
    Stopwatch watch(CLOCK_MONOTONIC);
    auto row_group_index = row_groups_indices[next_row_group_idx];
    cur_row_group_reader = file_reader->RowGroup(row_group_index);
    auto row_group_metadata = file_reader->metadata()->RowGroup(row_group_index);

    // Prior to Arrow 3.0.0, is_compressed was always set to false in column headers,
    // even if compression was used. See ARROW-17100.
    const bool always_compressed
        = file_reader->metadata()->writer_version().VersionLt(parquet::ApplicationVersion::PARQUET_CPP_10353_FIXED_VERSION());

    column_readers.clear();
    for (size_t i = 0; i < parquet_col_indice.size(); i++)
    {
        const auto & col_desc = *file_reader->metadata()->schema()->Column(parquet_col_indice[i]);
        auto [file_read_range, row_read_sequence] = column_index_filter->calculateReadSequence(row_group_index, col_desc.name());
        auto page_reader = buildPageReader(
            *arrow_file, file_read_range, *cur_row_group_reader->metadata()->ColumnChunk(parquet_col_indice[i]), always_compressed);
        ColReaderFactory factory(
            arrow_properties,
            col_desc,
            header.getByPosition(i).type,
            cur_row_group_reader->metadata()->ColumnChunk(parquet_col_indice[i]),
            std::move(page_reader));
        column_readers.emplace_back(factory.makeReader());
        column_readers.back()->setupReadState(row_read_sequence);
    }

    auto duration = watch.elapsedNanoseconds() / 1e6;
    LOG_DEBUG(log, "begin to read row group {} consumed {} ms", row_group_index, duration);

    ++next_row_group_idx;
    current_row_group_finished = true;
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


static std::shared_ptr<parquet::ArrowInputStream> getStream(arrow::io::RandomAccessFile & reader, const std::vector<arrow::io::ReadRange> & ranges)
{
    const Int64 nbytes = std::accumulate(
        ranges.begin(), ranges.end(), 0, [](const int64_t total, const arrow::io::ReadRange & range) { return total + range.length; });
    auto allocate_result = arrow::AllocateResizableBuffer(nbytes);
    std::shared_ptr<arrow::Buffer> buffer;
    if (allocate_result.ok())
        buffer.reset(allocate_result.ValueUnsafe().release());
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Allocate buffer failed");

    Int64 readPos = 0;
    std::ranges::for_each(
        ranges,
        [&](const arrow::io::ReadRange & range)
        {
            const Int64 offset = range.offset;
            const Int64 length = range.length;
            auto result = reader.ReadAt(offset, length, buffer->mutable_data() + readPos);
            Int64 bytes_read = 0;
            if (result.ok())
                bytes_read = *result;
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "read at offset {} failed", offset);
            assert(bytes_read == length);
            readPos += bytes_read;
        });
    assert(nbytes == readPos);

    return std::make_shared<::arrow::io::BufferReader>(buffer);
}

std::unique_ptr<parquet::PageReader> ParquetRecordReader::buildPageReader(
    arrow::io::RandomAccessFile & reader,
    const std::vector<arrow::io::ReadRange> & ranges,
    parquet::ColumnChunkMetaData & column_metadata,
    bool always_compressed)
{

    std::shared_ptr<parquet::ArrowInputStream> input_stream = getStream(reader, ranges);
    const parquet::ReaderProperties properties;
    return parquet::PageReader::Open(
        input_stream,
        column_metadata.num_values(),
        column_metadata.compression(),
        properties,
        always_compressed);
}
}
