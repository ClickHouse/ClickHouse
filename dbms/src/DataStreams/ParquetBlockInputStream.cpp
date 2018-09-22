#include <algorithm>
#include <iterator>
#include <vector>

// TODO: clear includes
#include <Core/ColumnWithTypeAndName.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <common/DateLUTImpl.h>
#include <DataStreams/ParquetBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/BufferBase.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteHelpers.h>
#include <ext/range.h>

#include <arrow/buffer.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>


#include <IO/copyData.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

ParquetBlockInputStream::ParquetBlockInputStream(ReadBuffer & istr_, const Block & header_)
    : istr(istr_)
    , header(header_)
{
}

Block ParquetBlockInputStream::getHeader() const
{
    return header;
}

/// Inserts numeric data right into internal column data to reduce an overhead
template <typename NumericType>
void ParquetBlockInputStream::fillColumnWithNumericData(std::shared_ptr<arrow::Column> & arrow_column, MutableColumnPtr & internal_column)
{
    PaddedPODArray<NumericType> & column_data = static_cast<ColumnVector<NumericType> &>(*internal_column).getData();
    column_data.reserve(arrow_column->length());

    for (size_t chunk_i = 0; chunk_i != static_cast<size_t>(arrow_column->data()->num_chunks()); ++chunk_i)
    {
        std::shared_ptr<arrow::Array> chunk = arrow_column->data()->chunk(chunk_i);
        /// buffers[0] is a null bitmap and buffers[1] are actual values
        std::shared_ptr<arrow::Buffer> buffer = chunk->data()->buffers[1];

        const NumericType * raw_data = reinterpret_cast<const NumericType *>(buffer->data());
        column_data.insert_assume_reserved(raw_data, raw_data + chunk->length());
    }
}

/// Inserts chars and offsets right into internal column data to reduce an overhead.
/// Internal offsets are shifted by one to the right in comparison with Arrow ones. So the last offset should map to the end of all chars.
/// Also internal strings are null terminated.
void ParquetBlockInputStream::fillColumnWithStringData(std::shared_ptr<arrow::Column> & arrow_column, MutableColumnPtr & internal_column)
{
    PaddedPODArray<UInt8>  & column_chars_t = static_cast<ColumnString &>(*internal_column).getChars();
    PaddedPODArray<UInt64> & column_offsets = static_cast<ColumnString &>(*internal_column).getOffsets();

    size_t chars_t_size = 0;
    for (size_t chunk_i = 0; chunk_i != static_cast<size_t>(arrow_column->data()->num_chunks()); ++chunk_i)
    {
        arrow::BinaryArray & chunk = static_cast<arrow::BinaryArray &>(*(arrow_column->data()->chunk(chunk_i)));
        const size_t chunk_length = chunk.length();

        chars_t_size += chunk.value_offset(chunk_length - 1) + chunk.value_length(chunk_length - 1);
        chars_t_size += chunk_length; /// additional space for null bytes
    }

    column_chars_t.reserve(chars_t_size);
    column_offsets.reserve(arrow_column->length());

    for (size_t chunk_i = 0; chunk_i != static_cast<size_t>(arrow_column->data()->num_chunks()); ++chunk_i)
    {
        arrow::BinaryArray & chunk = static_cast<arrow::BinaryArray &>(*(arrow_column->data()->chunk(chunk_i)));
        std::shared_ptr<arrow::Buffer> buffer = chunk.value_data();
        const size_t chunk_length = chunk.length();

        for (size_t offset_i = 0; offset_i != chunk_length; ++offset_i)
        {
            const UInt8 * raw_data = buffer->data() + chunk.value_offset(offset_i);
            column_chars_t.insert_assume_reserved(raw_data, raw_data + chunk.value_length(offset_i));
            column_chars_t.emplace_back('\0');

            column_offsets.emplace_back(column_chars_t.size());
        }
    }
}

void ParquetBlockInputStream::fillColumnWithBooleanData(std::shared_ptr<arrow::Column> & arrow_column, MutableColumnPtr & internal_column)
{
    PaddedPODArray<UInt8> & column_data = static_cast<ColumnVector<UInt8> &>(*internal_column).getData();
    column_data.resize(arrow_column->length());

    for (size_t chunk_i = 0; chunk_i != static_cast<size_t>(arrow_column->data()->num_chunks()); ++chunk_i)
    {
        arrow::BooleanArray & chunk = static_cast<arrow::BooleanArray &>(*(arrow_column->data()->chunk(chunk_i)));
        /// buffers[0] is a null bitmap and buffers[1] are actual values
        std::shared_ptr<arrow::Buffer> buffer = chunk.data()->buffers[1];

        for (size_t bool_i = 0; bool_i != static_cast<size_t>(chunk.length()); ++bool_i)
            column_data[bool_i] = chunk.Value(bool_i);
    }
}

/// Arrow stores Parquet::DATE in Int32, while ClickHouse stores Date in UInt16. Therefore, it should be checked before saving
void ParquetBlockInputStream::fillColumnWithDate32Data(std::shared_ptr<arrow::Column> & arrow_column, MutableColumnPtr & internal_column)
{
    PaddedPODArray<UInt16> & column_data = static_cast<ColumnVector<UInt16> &>(*internal_column).getData();
    column_data.reserve(arrow_column->length());

    for (size_t chunk_i = 0; chunk_i != static_cast<size_t>(arrow_column->data()->num_chunks()); ++chunk_i)
    {
        arrow::Date32Array & chunk = static_cast<arrow::Date32Array &>(*(arrow_column->data()->chunk(chunk_i)));

        for (size_t value_i = 0; value_i != static_cast<size_t>(chunk.length()); ++value_i)
        {
            UInt32 days_num = static_cast<UInt32>(chunk.Value(value_i));
            if (days_num > DATE_LUT_MAX_DAY_NUM)
            {
                // TODO: will it rollback correctly?
                throw Exception(
                    "Input value " + std::to_string(days_num) + " of a column \"" + arrow_column->name() + "\" is greater than "
                    "max allowed Date value, which is " + std::to_string(DATE_LUT_MAX_DAY_NUM)
                );
            }

            column_data.emplace_back(days_num);
        }
    }
}

/// Creates a null bytemap from arrow's null bitmap
void ParquetBlockInputStream::fillByteMapFromArrowColumn(std::shared_ptr<arrow::Column> & arrow_column, MutableColumnPtr & bytemap)
{
    PaddedPODArray<UInt8> & bytemap_data = static_cast<ColumnVector<UInt8> &>(*bytemap).getData();
    bytemap_data.reserve(arrow_column->length());

    for (size_t chunk_i = 0; chunk_i != static_cast<size_t>(arrow_column->data()->num_chunks()); ++chunk_i)
    {
        std::shared_ptr<arrow::Array> chunk = arrow_column->data()->chunk(chunk_i);

        for (size_t value_i = 0; value_i != static_cast<size_t>(chunk->length()); ++value_i)
            bytemap_data.emplace_back(chunk->IsNull(value_i));
    }
}

#define FOR_ARROW_NUMERIC_TYPES(M)      \
        M(arrow::Type::UINT8,  UInt8)   \
        M(arrow::Type::INT8,   Int8)    \
        M(arrow::Type::UINT16, UInt16)  \
        M(arrow::Type::INT16,  Int16)   \
        M(arrow::Type::UINT32, UInt32)  \
        M(arrow::Type::INT32,  Int32)   \
        M(arrow::Type::UINT64, UInt64)  \
        M(arrow::Type::INT64,  Int64)   \
        M(arrow::Type::FLOAT,  Float32) \
        M(arrow::Type::DOUBLE, Float64)


using NameToColumnPtr = std::unordered_map<std::string, std::shared_ptr<arrow::Column>>;

const std::unordered_map<arrow::Type::type, std::shared_ptr<IDataType>> ParquetBlockInputStream::arrow_type_to_internal_type = {
    {arrow::Type::UINT8,  std::make_shared<DataTypeUInt8>()},
    {arrow::Type::INT8,   std::make_shared<DataTypeInt8>()},
    {arrow::Type::UINT16, std::make_shared<DataTypeUInt16>()},
    {arrow::Type::INT16,  std::make_shared<DataTypeInt16>()},
    {arrow::Type::UINT32, std::make_shared<DataTypeUInt32>()},
    {arrow::Type::INT32,  std::make_shared<DataTypeInt32>()},
    {arrow::Type::UINT64, std::make_shared<DataTypeUInt64>()},
    {arrow::Type::INT64,  std::make_shared<DataTypeInt64>()},
    {arrow::Type::FLOAT,  std::make_shared<DataTypeFloat32>()},
    {arrow::Type::DOUBLE, std::make_shared<DataTypeFloat64>()},

    {arrow::Type::BOOL,   std::make_shared<DataTypeUInt8>()},
    {arrow::Type::DATE32, std::make_shared<DataTypeDate>()},

    {arrow::Type::STRING, std::make_shared<DataTypeString>()}//,
    // TODO: add other types that are convertable to internal ones:
    // 0. ENUM?
    // 1. UUID -> String
    // 2. JSON -> String
};


Block ParquetBlockInputStream::readImpl()
{
    Block res;

    if (istr.eof())
        return res;

    std::string file_data;

    {
        WriteBufferFromString file_buffer(file_data);
        copyData(istr, file_buffer);
    }

    arrow::Buffer buffer(file_data);
    // TODO: maybe use parquet::RandomAccessSource?
    auto reader = parquet::ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(buffer));
    parquet::arrow::FileReader filereader(::arrow::default_memory_pool(), std::move(reader));
    std::shared_ptr<arrow::Table> table;

    // TODO: also catch a ParquetException thrown by filereader?
    arrow::Status read_status = filereader.ReadTable(&table);
    if (!read_status.ok())
        throw Exception("Error while reading parquet data: " + read_status.ToString()/*, ErrorCodes::TODO*/);

    if (0 == table->num_rows())
        throw Exception("Empty table in input data"/*, ErrorCodes::TODO*/);

    if (header.columns() > static_cast<size_t>(table->num_columns()))
        // TODO: What if some columns were not presented? Insert NULLs? What if a column is not nullable?
        throw Exception("Number of columns is less than the table has" /*, ErrorCodes::TODO*/);


    NameToColumnPtr name_to_column_ptr;
    for (size_t i = 0; i != static_cast<size_t>(table->num_columns()); ++i)
    {
        std::shared_ptr<arrow::Column> arrow_column = table->column(i);
        name_to_column_ptr[arrow_column->name()] = arrow_column;
    }

    for (size_t column_i = 0; column_i != header.columns(); ++column_i)
    {
        ColumnWithTypeAndName header_column = header.getByPosition(column_i);

        if (name_to_column_ptr.find(header_column.name) == name_to_column_ptr.end())
            // TODO: What if some columns were not presented? Insert NULLs? What if a column is not nullable?
            throw Exception("Column \"" + header_column.name + "\" is not presented in input data" /*, ErrorCodes::TODO*/);

        std::shared_ptr<arrow::Column> arrow_column = name_to_column_ptr[header_column.name];
        arrow::Type::type arrow_type = arrow_column->type()->id();

        if (arrow_type_to_internal_type.find(arrow_type) == arrow_type_to_internal_type.end())
        {
            throw Exception(
                "The type \"" + arrow_column->type()->name() + "\" of an input column \"" + arrow_column->name() + "\""
                " is not supported for conversion from a Parquet data format"
                /*, ErrorCodes::TODO*/
            );
        }

        // TODO: check if a column is const?
        if (!header_column.type->isNullable() && arrow_column->null_count())
        {
            throw Exception("Can not insert NULL data into non-nullable column \"" + header_column.name + "\""/*, ErrorCodes::TODO*/);
        }

        const bool target_column_is_nullable = header_column.type->isNullable() || arrow_column->null_count();

        const DataTypePtr internal_nested_type = arrow_type_to_internal_type.at(arrow_type);
        const DataTypePtr internal_type = target_column_is_nullable ? makeNullable(internal_nested_type) : internal_nested_type;
        const std::string internal_nested_type_name = internal_nested_type->getName();

        const DataTypePtr column_nested_type =
            header_column.type->isNullable()
                ? static_cast<const DataTypeNullable *>(header_column.type.get())->getNestedType()
                : header_column.type;


        const DataTypePtr column_type = header_column.type;
        const std::string column_nested_type_name = column_nested_type->getName();

        // TODO: can it be done with typeid_cast?
        if (internal_nested_type_name != column_nested_type_name)
        {
            throw Exception(
                "Input data type \"" + internal_nested_type_name + "\" for a column \"" + header_column.name + "\""
                " is not compatible with a column type \"" + column_nested_type_name + "\""/*, ErrorCodes::TODO*/
            );
        }

        ColumnWithTypeAndName column;
        column.name = header_column.name;
        column.type = internal_type;

        /// Data
        MutableColumnPtr read_column = internal_nested_type->createColumn();

        switch (arrow_type)
        {
            case arrow::Type::STRING:
                fillColumnWithStringData(arrow_column, read_column);
                break;
            case arrow::Type::BOOL:
                fillColumnWithBooleanData(arrow_column, read_column);
                break;
            case arrow::Type::DATE32:
                fillColumnWithDate32Data(arrow_column, read_column);
                break;
#define DISPATCH(ARROW_NUMERIC_TYPE, CPP_NUMERIC_TYPE) \
            case ARROW_NUMERIC_TYPE: \
                fillColumnWithNumericData<CPP_NUMERIC_TYPE>(arrow_column, read_column); \
                break;

            FOR_ARROW_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
            // TODO: support TIMESTAMP_MICROS and TIMESTAMP_MILLIS with truncated micro- and milliseconds?
            // TODO: read JSON as a string?
            // TODO: read UUID as a string?
            default:
                throw Exception("Unsupported parquet type \"" + arrow_column->type()->name() + "\""/*, ErrorCodes::TODO*/);
        }

        if (column.type->isNullable())
        {
            MutableColumnPtr null_bytemap = DataTypeUInt8().createColumn();
            fillByteMapFromArrowColumn(arrow_column, null_bytemap);
            column.column = ColumnNullable::create(std::move(read_column), std::move(null_bytemap));
        }
        else
        {
            column.column = std::move(read_column);
        }
        res.insert(std::move(column));
    }

    return res;
}

}
