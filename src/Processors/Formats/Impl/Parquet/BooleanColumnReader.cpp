#include "SelectiveColumnReader.h"

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Processors/Formats/Impl/Parquet/ParquetColumnReaderFactory.h>
#include <Processors/Formats/Impl/Parquet/ParquetReader.h>
#include <Common/assert_cast.h>

namespace ErrorCodes
{
extern const int PARQUET_EXCEPTION;
extern const int LOGICAL_ERROR;
}

namespace DB
{

BooleanColumnReader::BooleanColumnReader(PageReaderCreator page_reader_creator_, ScanSpec scan_spec_)
    : SelectiveColumnReader(page_reader_creator_, scan_spec_)
{
}
MutableColumnPtr BooleanColumnReader::createColumn()
{
    return ColumnUInt8::create();
}

void BooleanColumnReader::initBitReader()
{
    if (!state.data.buffer)
    {
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "page buffer is not initialized");
    }
    if (bit_reader && bit_reader->bytes_left() > 0)
        return;
    bit_reader = std::make_unique<arrow::bit_util::BitReader>(state.data.buffer, state.data.buffer_size);
}

void BooleanColumnReader::computeRowSet(OptionalRowSet & row_set, size_t rows_to_read)
{
    if (!buffer.empty())
    {
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "buffer is not empty");
    }
    if (!row_set)
        return;
    readAndDecodePage();
    initBitReader();

    buffer.resize(rows_to_read);
    size_t count = bit_reader->GetBatch(1, buffer.data(), static_cast<int>(rows_to_read));
    chassert(count == rows_to_read);
    computeRowSetPlain(buffer.data(), row_set, scan_spec.filter, rows_to_read);
}

void BooleanColumnReader::computeRowSetSpace(
    OptionalRowSet & row_set, PaddedPODArray<UInt8, 4096> & null_map, size_t null_count, size_t rows_to_read)
{
    if (!buffer.empty())
    {
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "buffer is not empty");
    }
    if (!row_set)
        return;
    readAndDecodePage();
    initBitReader();

    buffer.resize(rows_to_read - null_count);
    size_t count [[maybe_unused]] = bit_reader->GetBatch(1, buffer.data(), static_cast<int>(rows_to_read - null_count));
    chassert(count == rows_to_read - null_count);
    computeRowSetPlainSpace(buffer.data(), row_set, scan_spec.filter, null_map, rows_to_read);
}

void BooleanColumnReader::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    if (!buffer.empty())
    {
        chassert(rows_to_read == buffer.size());
        ColumnUInt8 * uint8_col = static_cast<ColumnUInt8 *>(column.get());
        auto & data = uint8_col->getData();
        plain_decoder->decodeBoolean(data, buffer, row_set, rows_to_read);
    }
    else
    {
        size_t rows_read = 0;
        while (rows_read < rows_to_read)
        {
            readAndDecodePage();
            initBitReader();

            auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
            buffer.resize(rows_can_read);
            size_t count [[maybe_unused]] = bit_reader->GetBatch(1, buffer.data(), static_cast<int>(rows_can_read));
            chassert(count == rows_can_read);
            auto * number_column = static_cast<ColumnUInt8 *>(column.get());
            auto & data = number_column->getData();
            plain_decoder->decodeBoolean(data, buffer, row_set, rows_can_read);
            buffer.resize(0);
            if (row_set)
                row_set->addOffset(rows_can_read);
            rows_read += rows_can_read;
        }
    }
}
void BooleanColumnReader::readSpace(
    MutableColumnPtr & column, OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
{
    if (!buffer.empty())
    {
        chassert(rows_to_read == buffer.size());
        ColumnUInt8 * uint8_col = static_cast<ColumnUInt8 *>(column.get());
        auto & data = uint8_col->getData();
        plain_decoder->decodeBooleanSpace(data, buffer, row_set, null_map, rows_to_read);
    }
    else
    {
        size_t rows_read = 0;
        while (rows_read < rows_to_read)
        {
            readAndDecodePage();
            initBitReader();

            auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
            buffer.resize(rows_can_read - null_count);
            size_t count [[maybe_unused]] = bit_reader->GetBatch(1, buffer.data(), static_cast<int>(rows_can_read - null_count));
            chassert(count == rows_can_read - null_count);
            auto * number_column = static_cast<ColumnUInt8 *>(column.get());
            auto & data = number_column->getData();
            plain_decoder->decodeBooleanSpace(data, buffer, row_set, null_map, rows_can_read);
            buffer.resize(0);
            if (row_set)
                row_set->addOffset(rows_can_read);
            rows_read += rows_can_read;
        }
    }
    buffer.clear();
}
size_t BooleanColumnReader::skipValuesInCurrentPage(size_t rows_to_skip)
{
    if (!state.page || !rows_to_skip)
        return rows_to_skip;
    size_t skipped = std::min(state.offsets.remain_rows, rows_to_skip);
    state.offsets.consume(skipped);

    if (!buffer.empty())
    {
        // only support skip all
        chassert(buffer.size() == skipped);
        buffer.clear();
    }
    else
    {
        initBitReader();
        if (!bit_reader->Advance(skipped))
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "skip rows failed. don't have enough data");
        state.idx_buffer.clear();
    }
    return rows_to_skip - skipped;
}
}
