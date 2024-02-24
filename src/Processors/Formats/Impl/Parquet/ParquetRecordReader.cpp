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
    extern const int PARQUET_EXCEPTION;
}

#define THROW_PARQUET_EXCEPTION(s)                                            \
    do                                                                        \
    {                                                                         \
        try { (s); }                                                          \
        catch (const ::parquet::ParquetException & e)                         \
        {                                                                     \
            auto msg = PreformattedMessage::create("Excepted when reading parquet: {}", e.what()); \
            throw Exception(std::move(msg), ErrorCodes::PARQUET_EXCEPTION);   \
        }                                                                     \
    } while (false)

namespace
{

std::unique_ptr<parquet::ParquetFileReader> createFileReader(
    std::shared_ptr<::arrow::io::RandomAccessFile> arrow_file)
{
    std::unique_ptr<parquet::ParquetFileReader> res;
    THROW_PARQUET_EXCEPTION(res = parquet::ParquetFileReader::Open(std::move(arrow_file)));
    return res;
}

std::unique_ptr<ParquetColumnReader> createColReader(
    const parquet::ColumnDescriptor & col_descriptor,
    DataTypePtr ch_type,
    std::unique_ptr<parquet::ColumnChunkMetaData> meta,
    std::unique_ptr<parquet::PageReader> reader)
{
    if (col_descriptor.logical_type()->is_date() && parquet::Type::INT32 == col_descriptor.physical_type())
    {
        return std::make_unique<ParquetLeafColReader<ColumnInt32>>(
            col_descriptor, std::make_shared<DataTypeDate32>(), std::move(meta), std::move(reader));
    }
    else if (col_descriptor.logical_type()->is_decimal())
    {
        switch (col_descriptor.physical_type())
        {
            case parquet::Type::INT32:
            {
                auto data_type = std::make_shared<DataTypeDecimal32>(
                    col_descriptor.type_precision(), col_descriptor.type_scale());
                return std::make_unique<ParquetLeafColReader<ColumnDecimal<Decimal32>>>(
                    col_descriptor, data_type, std::move(meta), std::move(reader));
            }
            case parquet::Type::INT64:
            {
                auto data_type = std::make_shared<DataTypeDecimal64>(
                    col_descriptor.type_precision(), col_descriptor.type_scale());
                return std::make_unique<ParquetLeafColReader<ColumnDecimal<Decimal64>>>(
                    col_descriptor, data_type, std::move(meta), std::move(reader));
            }
            case parquet::Type::FIXED_LEN_BYTE_ARRAY:
            {
                if (col_descriptor.type_length() <= static_cast<int>(sizeof(Decimal128)))
                {
                    auto data_type = std::make_shared<DataTypeDecimal128>(
                        col_descriptor.type_precision(), col_descriptor.type_scale());
                    return std::make_unique<ParquetLeafColReader<ColumnDecimal<Decimal128>>>(
                        col_descriptor, data_type, std::move(meta), std::move(reader));
                }
                else
                {
                    auto data_type = std::make_shared<DataTypeDecimal256>(
                        col_descriptor.type_precision(), col_descriptor.type_scale());
                    return std::make_unique<ParquetLeafColReader<ColumnDecimal<Decimal256>>>(
                        col_descriptor, data_type, std::move(meta), std::move(reader));
                }
            }
            default:
                throw Exception(
                    ErrorCodes::PARQUET_EXCEPTION,
                    "Type not supported for decimal: {}",
                    col_descriptor.physical_type());
        }
    }
    else
    {
        switch (col_descriptor.physical_type())
        {
            case parquet::Type::INT32:
                return std::make_unique<ParquetLeafColReader<ColumnInt32>>(
                    col_descriptor, std::make_shared<DataTypeInt32>(), std::move(meta), std::move(reader));
            case parquet::Type::INT64:
                return std::make_unique<ParquetLeafColReader<ColumnInt64>>(
                    col_descriptor, std::make_shared<DataTypeInt64>(), std::move(meta), std::move(reader));
            case parquet::Type::FLOAT:
                return std::make_unique<ParquetLeafColReader<ColumnFloat32>>(
                    col_descriptor, std::make_shared<DataTypeFloat32>(), std::move(meta), std::move(reader));
            case parquet::Type::INT96:
            {
                DataTypePtr read_type = ch_type;
                if (!isDateTime64(ch_type))
                {
                    read_type = std::make_shared<DataTypeDateTime64>(ParquetRecordReader::default_datetime64_scale);
                }
                return std::make_unique<ParquetLeafColReader<ColumnDecimal<DateTime64>>>(
                    col_descriptor, read_type, std::move(meta), std::move(reader));
            }
            case parquet::Type::DOUBLE:
                return std::make_unique<ParquetLeafColReader<ColumnFloat64>>(
                    col_descriptor, std::make_shared<DataTypeFloat64>(), std::move(meta), std::move(reader));
            case parquet::Type::BYTE_ARRAY:
                return std::make_unique<ParquetLeafColReader<ColumnString>>(
                    col_descriptor, std::make_shared<DataTypeString>(), std::move(meta), std::move(reader));
            default:
                throw Exception(
                    ErrorCodes::PARQUET_EXCEPTION, "Type not supported: {}", col_descriptor.physical_type());
        }
    }
}

} // anonymous namespace

ParquetRecordReader::ParquetRecordReader(
    Block header_,
    parquet::ArrowReaderProperties reader_properties_,
    std::shared_ptr<::arrow::io::RandomAccessFile> arrow_file,
    const FormatSettings & format_settings,
    std::vector<int> row_groups_indices_)
    : file_reader(createFileReader(std::move(arrow_file)))
    , reader_properties(reader_properties_)
    , header(std::move(header_))
    , max_block_size(format_settings.parquet.max_block_size)
    , row_groups_indices(std::move(row_groups_indices_))
    , left_rows(getTotalRows(*file_reader->metadata()))
{
    // Only little endian system is supported currently
    static_assert(std::endian::native == std::endian::little);

    log = &Poco::Logger::get("ParquetRecordReader");

    parquet_col_indice.reserve(header.columns());
    column_readers.reserve(header.columns());
    for (const auto & col_with_name : header)
    {
        auto idx = file_reader->metadata()->schema()->ColumnIndex(col_with_name.name);
        if (idx < 0)
        {
            auto msg = PreformattedMessage::create("can not find column with name: {}", col_with_name.name);
            throw Exception(std::move(msg), ErrorCodes::PARQUET_EXCEPTION);
        }
        parquet_col_indice.push_back(idx);
    }
    if (reader_properties.pre_buffer())
    {
        THROW_PARQUET_EXCEPTION(file_reader->PreBuffer(
            row_groups_indices, parquet_col_indice, reader_properties.io_context(), reader_properties.cache_options()));
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
        column_readers.emplace_back(createColReader(
            *file_reader->metadata()->schema()->Column(parquet_col_indice[i]),
            header.getByPosition(i).type,
            cur_row_group_reader->metadata()->ColumnChunk(parquet_col_indice[i]),
            cur_row_group_reader->GetColumnPageReader(parquet_col_indice[i])));
    }

    auto duration = watch.elapsedNanoseconds() / 1e6;
    LOG_DEBUG(log, "reading row group {} consumed {} ms", row_groups_indices[next_row_group_idx], duration);

    ++next_row_group_idx;
    cur_row_group_left_rows = cur_row_group_reader->metadata()->num_rows();
}

Int64 ParquetRecordReader::getTotalRows(const parquet::FileMetaData & meta_data)
{
    Int64 res = 0;
    for (size_t i = 0; i < row_groups_indices.size(); i++)
    {
        res += meta_data.RowGroup(row_groups_indices[i])->num_rows();
    }
    return res;
}

}
