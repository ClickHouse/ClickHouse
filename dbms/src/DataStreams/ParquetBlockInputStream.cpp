#include <algorithm>
#include <iterator>
#include <vector>

#include <Core/ColumnWithTypeAndName.h>
#include <DataStreams/ParquetBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/BufferBase.h>
#include <IO/ReadBufferFromMemory.h>
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


void ParquetBlockInputStream::readData(const IDataType & type, IColumn & column, ReadBuffer & istr, size_t rows)
{
    IDataType::InputStreamGetter input_stream_getter = [&] (const IDataType::SubstreamPath &) { return &istr; };
    type.deserializeBinaryBulkWithMultipleStreams(column, input_stream_getter, rows, /* avg_value_size_hint = */0, false, {});

    if (column.size() != rows)
        throw Exception("Cannot read all data in ParquetBlockInputStream.", ErrorCodes::CANNOT_READ_ALL_DATA);
}

using NameToColumnPtr = std::unordered_map<std::string, std::shared_ptr<arrow::Column>>;

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

    // TODO: is it possible to read metadata only and then read columns one by one?
    arrow::Buffer buffer(file_data);
    // TODO: maybe use parquet::RandomAccessSource?
    auto reader = parquet::ParquetFileReader::Open(std::make_shared<::arrow::io::BufferReader>(buffer));
    parquet::arrow::FileReader filereader(::arrow::default_memory_pool(), std::move(reader));

    std::shared_ptr<arrow::Table> table;
    // TODO: Use an internal Exception?
    PARQUET_THROW_NOT_OK(filereader.ReadTable(&table));

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

    for (size_t i = 0; i != header.columns(); ++i)
    {
        ColumnWithTypeAndName header_column = header.getByPosition(i);

        if (name_to_column_ptr.find(header_column.name) == name_to_column_ptr.end())
            // TODO: What if some columns were not presented? Insert NULLs? What if a column is not nullable?
            throw Exception("Column \"" + header_column.name + "\" is not presented in input data" /*, ErrorCodes::TODO*/);

        // TODO: timezones?
        // TODO: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
            // TODO: how to interpet a JSON doc?

        std::shared_ptr<arrow::Column> arrow_column = name_to_column_ptr[header_column.name];
        arrow::Type::type arrow_type = arrow_column->type()->id();

        if (arrow_type_to_native_type.find(arrow_type) == arrow_type_to_native_type.end())
        {
            throw Exception("Unsupported type " + arrow_column->type()->name() + " of a column " + arrow_column->name()/*, ErrorCodes::TODO*/);
        }


        // TODO: support NULL values

        DataTypePtr native_type = arrow_type_to_native_type[arrow_type];
        if (header_column.type->getName() != native_type->getName())
        {
            throw Exception("Input data type " + native_type->getName() + " for column \"" + header_column.name + "\" is not compatible with an actual type " + header_column.type->getName());
        }

        ColumnWithTypeAndName column;
        column.name = header_column.name;
        column.type = native_type;

        /// Data
        MutableColumnPtr read_column = column.type->createColumn();

        for (size_t chunk_i = 0; chunk_i != static_cast<size_t>(arrow_column->data()->num_chunks()); ++chunk_i)
        {
            std::shared_ptr<arrow::Array> chunk = arrow_column->data()->chunk(chunk_i);
            /// arrow::Array has two buffers: null bitmap and actual values
            std::shared_ptr<arrow::Buffer> buffer = chunk->data()->buffers[1]; /// getting values
            // TODO: make less copying?
            ReadBufferFromMemory values_buffer(buffer->data(), buffer->size());
            size_t rows_num = chunk->length();

            readData(*column.type, *read_column, values_buffer, rows_num);
        }

        // TODO: process a String type
        // TODO: if (... == "String") { ... }

        column.column = std::move(read_column);
        res.insert(std::move(column));
    }

    return res;
}

}
