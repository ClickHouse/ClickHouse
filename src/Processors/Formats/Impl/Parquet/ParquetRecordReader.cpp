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
    extern const int BAD_ARGUMENTS;
    extern const int PARQUET_EXCEPTION;
}

#define THROW_PARQUET_EXCEPTION(s)                                      \
    do                                                                  \
    {                                                                   \
        try { (s); }                                                    \
        catch (const ::parquet::ParquetException & e)                   \
        {                                                               \
            auto msg = PreformattedMessage::create("Excepted when reading parquet: {}", e.what()); \
            throw Exception(std::move(msg), ErrorCodes::PARQUET_EXCEPTION);   \
        }                                                               \
    } while (false)

namespace
{

Int64 getTotalRows(const parquet::FileMetaData & meta_data)
{
    Int64 res = 0;
    for (int i = 0; i < meta_data.num_row_groups(); i++)
    {
        res += meta_data.RowGroup(i)->num_rows();
    }
    return res;
}

std::unique_ptr<ParquetColumnReader> createReader(
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
                if (col_descriptor.type_length() <= static_cast<int>(DecimalUtils::max_precision<Decimal128>))
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

} // anonymouse namespace

ParquetRecordReader::ParquetRecordReader(
    Block header_,
    std::shared_ptr<::arrow::io::RandomAccessFile> file,
    const parquet::ReaderProperties& properties)
    : header(std::move(header_))
{
    // Only little endian system is supported currently
    static_assert(std::endian::native == std::endian::little);

    log = &Poco::Logger::get("ParquetRecordReader");
    THROW_PARQUET_EXCEPTION(file_reader = parquet::ParquetFileReader::Open(std::move(file), properties));
    left_rows = getTotalRows(*file_reader->metadata());

    parquet_col_indice.reserve(header.columns());
    column_readers.reserve(header.columns());
    for (const auto & col_with_name : header)
    {
        auto idx = file_reader->metadata()->schema()->ColumnIndex(col_with_name.name);
        if (idx < 0)
        {
            auto msg = PreformattedMessage::create("can not find column with name: {}", col_with_name.name);
            throw Exception(std::move(msg), ErrorCodes::BAD_ARGUMENTS);
        }
        parquet_col_indice.push_back(idx);
    }
}

Chunk ParquetRecordReader::readChunk(size_t num_rows)
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
    auto num_rows_read = std::min(num_rows, cur_row_group_left_rows);
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
    cur_row_group_reader = file_reader->RowGroup(next_row_group_idx);

    column_readers.clear();
    for (size_t i = 0; i < parquet_col_indice.size(); i++)
    {
        column_readers.emplace_back(createReader(
            *file_reader->metadata()->schema()->Column(parquet_col_indice[i]),
            header.getByPosition(i).type,
            cur_row_group_reader->metadata()->ColumnChunk(parquet_col_indice[i]),
            cur_row_group_reader->GetColumnPageReader(parquet_col_indice[i])));
    }
    LOG_DEBUG(log, "reading row group {} consumed {} ms", next_row_group_idx, watch.elapsedNanoseconds() / 1e6);
    ++next_row_group_idx;
    cur_row_group_left_rows = cur_row_group_reader->metadata()->num_rows();
}

}
