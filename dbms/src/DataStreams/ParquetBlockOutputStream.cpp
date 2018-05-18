// TODO: clean includes
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <IO/WriteHelpers.h>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <parquet/util/memory.h>
#include <parquet/exception.h>

#include <DataStreams/ParquetBlockOutputStream.h>

namespace DB
{

ParquetBlockOutputStream::ParquetBlockOutputStream(WriteBuffer & ostr_, const Block & header_)
    : ostr(ostr_)
    , header(header_)
{
}

template <typename NumericType, typename ArrowBuilderType>
void ParquetBlockOutputStream::fillArrowArrayWithNumericColumnData(ColumnPtr write_column, std::shared_ptr<arrow::Array> & arrow_array)
{
    const PaddedPODArray<NumericType> & internal_data = static_cast<const ColumnVector<NumericType> &>(*write_column).getData();

    ArrowBuilderType numeric_builder;
    // TODO: check status.ok()
    /*arrow::Status status = */numeric_builder.AppendValues(internal_data.data(), internal_data.size());
    /*arrow::Status status = */numeric_builder.Finish(&arrow_array);
}

void ParquetBlockOutputStream::fillArrowArrayWithStringColumnData(ColumnPtr write_column, std::shared_ptr<arrow::Array> & arrow_array)
{
    const ColumnString & internal_column = static_cast<const ColumnString &>(*write_column);
    arrow::StringBuilder string_builder;

    for (size_t string_i = 0; string_i != internal_column.size(); ++string_i)
    {
        StringRef && string_ref = internal_column.getDataAt(string_i);
        // TODO: check status.ok()
        /*arrow::Status status = */string_builder.Append(string_ref.data, string_ref.size);
    }

    // TODO: check status.ok()
    /*arrow::Status status = */string_builder.Finish(&arrow_array);
}

void ParquetBlockOutputStream::fillArrowArrayWithDateColumnData(ColumnPtr write_column, std::shared_ptr<arrow::Array> & arrow_array)
{
    const PaddedPODArray<UInt16> & internal_data = static_cast<const ColumnVector<UInt16> &>(*write_column).getData();
    arrow::Date32Builder date32_builder;

    for (size_t value_i = 0; value_i != internal_data.size(); ++value_i)
    {
        // TODO: check status.ok()
        /// Implicitly converts UInt16 to Int32
        /*arrow::Status status = */date32_builder.Append(internal_data[value_i]);
    }

    /*arrow::Status status = */date32_builder.Finish(&arrow_array);
}

#define FOR_INTERNAL_NUMERIC_TYPES(M) \
        M(UInt8,   arrow::UInt8Builder) \
        M(Int8,    arrow::Int8Builder) \
        M(UInt16,  arrow::UInt16Builder) \
        M(Int16,   arrow::Int16Builder) \
        M(UInt32,  arrow::UInt32Builder) \
        M(Int32,   arrow::Int32Builder) \
        M(UInt64,  arrow::UInt64Builder) \
        M(Int64,   arrow::Int64Builder) \
        M(Float32, arrow::FloatBuilder) \
        M(Float64, arrow::DoubleBuilder)


// TODO: create a better row_group_size estimation
/* static constexpr const UInt64 GiB_in_bytes = 1 << 30; */

void ParquetBlockOutputStream::write(const Block & block)
{
    block.checkNumberOfRows();

    const size_t columns_num = block.columns();

    /// For arrow::Schema and arrow::Table creation
    std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
    std::vector<std::shared_ptr<arrow::Array>> arrow_arrays;
    arrow_fields.reserve(columns_num);
    arrow_arrays.reserve(columns_num);

    for (size_t column_i = 0; column_i < columns_num; ++column_i)
    {
        const ColumnWithTypeAndName & column = block.safeGetByPosition(column_i);

        // TODO: support NULLs
        arrow_fields.emplace_back(new arrow::Field(column.name, internal_type_to_arrow_type[column.type->getName()], /*nullable = */false));
        // TODO: !keep in mind that arrow and parquet types are not interchangeable!
        std::shared_ptr<arrow::Array> arrow_array;

        String internal_type_name = column.type->getName();

        if ("String" == internal_type_name)
        {
            fillArrowArrayWithStringColumnData(column.column, arrow_array);
        }
#define DISPATCH(CPP_NUMERIC_TYPE, ARROW_BUILDER_TYPE) \
        else if (#CPP_NUMERIC_TYPE == internal_type_name) \
        { \
            fillArrowArrayWithNumericColumnData<CPP_NUMERIC_TYPE, ARROW_BUILDER_TYPE>(column.column, arrow_array); \
        }

        FOR_INTERNAL_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
        else if ("Date" == internal_type_name)
        {
            fillArrowArrayWithDateColumnData(column.column, arrow_array); \
        }
        // TODO: are there internal types that are convertable to parquet/arrow once?
        else
        {
            throw Exception(
                "Internal type " + column.type->getName() + " of a column \"" + column.name + "\" "
                "is not supported for a conversion into a Parquet format"/*, ErrorCodes::TODO*/
            );
        }

        arrow_arrays.emplace_back(std::move(arrow_array));
    }

    std::shared_ptr<arrow::Schema> arrow_schema = std::make_shared<arrow::Schema>(std::move(arrow_fields));
    std::shared_ptr<arrow::Table> arrow_table = arrow::Table::Make(arrow_schema, arrow_arrays);

    // TODO: get rid of extra copying
    std::shared_ptr<parquet::InMemoryOutputStream> sink = std::make_shared<parquet::InMemoryOutputStream>();

    // TODO: calculate row_group_size
    /* const UInt64 row_group_size = std::min(1, GiB_in_bytes / sizeof(UInt64) / arrow_table->num_rows()); */

    // TODO: check Status.ok()
    arrow::Status status = parquet::arrow::WriteTable(
        *arrow_table, arrow::default_memory_pool(), sink,
        /* row_group_size = */arrow_table->num_rows(), parquet::default_writer_properties(),
        parquet::arrow::default_arrow_writer_properties()
    );
    std::shared_ptr<arrow::Buffer> table_buffer = sink->GetBuffer();

    writeString(reinterpret_cast<const char *>(table_buffer->data()), table_buffer->size(), ostr);
}

/* void ParquetBlockOutputStream::flush() */
/* { */
/*     ostr.next(); */
/* } */

};
