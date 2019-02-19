// TODO: clean includes
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeNullable.h>
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

void ParquetBlockOutputStream::flush()
{
    ostr.next();
}

void checkAppendStatus(arrow::Status & append_status, const std::string & column_name)
{
    if (!append_status.ok())
    {
        throw Exception(
            "Error while building a parquet column \"" + column_name + "\": " + append_status.ToString()/*,
            ErrorCodes::TODO*/
        );
    }
}

void checkFinishStatus(arrow::Status & finish_status, const std::string & column_name)
{
    if (!finish_status.ok())
    {
        throw Exception(
            "Error while writing a parquet column \"" + column_name + "\": " + finish_status.ToString()/*,
            ErrorCodes::TODO*/
        );
    }
}

template <typename NumericType, typename ArrowBuilderType>
void ParquetBlockOutputStream::fillArrowArrayWithNumericColumnData(
    ColumnPtr write_column,
    std::shared_ptr<arrow::Array> & arrow_array,
    const PaddedPODArray<UInt8> * null_bytemap
) {
    const PaddedPODArray<NumericType> & internal_data = static_cast<const ColumnVector<NumericType> &>(*write_column).getData();
    ArrowBuilderType numeric_builder;
    arrow::Status append_status;

    const UInt8 * arrow_null_bytemap_raw_ptr = nullptr;
    PaddedPODArray<UInt8> arrow_null_bytemap;
    if (null_bytemap)
    {
        /// Invert values since Arrow interprets 1 as a non-null value, while CH as a null
        arrow_null_bytemap.reserve(null_bytemap->size());
        for (size_t i = 0; i != null_bytemap->size(); ++i)
            arrow_null_bytemap.emplace_back(1 ^ (*null_bytemap)[i]);

        arrow_null_bytemap_raw_ptr = arrow_null_bytemap.data();
    }

    append_status = numeric_builder.AppendValues(internal_data.data(), internal_data.size(), arrow_null_bytemap_raw_ptr);
    checkAppendStatus(append_status, write_column->getName());

    arrow::Status finish_status = numeric_builder.Finish(&arrow_array);
    checkFinishStatus(finish_status, write_column->getName());
}

void ParquetBlockOutputStream::fillArrowArrayWithStringColumnData(
    ColumnPtr write_column,
    std::shared_ptr<arrow::Array> & arrow_array,
    const PaddedPODArray<UInt8> * null_bytemap
) {
    const ColumnString & internal_column = static_cast<const ColumnString &>(*write_column);
    arrow::StringBuilder string_builder;
    arrow::Status append_status;

    for (size_t string_i = 0; string_i != internal_column.size(); ++string_i)
    {
        if (null_bytemap && (*null_bytemap)[string_i])
        {
            append_status = string_builder.AppendNull();
        }
        else
        {
            StringRef string_ref = internal_column.getDataAt(string_i);
            append_status = string_builder.Append(string_ref.data, string_ref.size);
        }

        checkAppendStatus(append_status, write_column->getName());
    }

    arrow::Status finish_status = string_builder.Finish(&arrow_array);
    checkFinishStatus(finish_status, write_column->getName());
}

void ParquetBlockOutputStream::fillArrowArrayWithDateColumnData(
    ColumnPtr write_column,
    std::shared_ptr<arrow::Array> & arrow_array,
    const PaddedPODArray<UInt8> * null_bytemap
) {
    const PaddedPODArray<UInt16> & internal_data = static_cast<const ColumnVector<UInt16> &>(*write_column).getData();
    arrow::Date32Builder date32_builder;
    arrow::Status append_status;

    for (size_t value_i = 0; value_i != internal_data.size(); ++value_i)
    {
        if (null_bytemap && (*null_bytemap)[value_i])
            append_status = date32_builder.AppendNull();
        else
            /// Implicitly converts UInt16 to Int32
            append_status = date32_builder.Append(internal_data[value_i]);

        checkAppendStatus(append_status, write_column->getName());
    }

    arrow::Status finish_status = date32_builder.Finish(&arrow_array);
    checkFinishStatus(finish_status, write_column->getName());
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

const std::unordered_map<String, std::shared_ptr<arrow::DataType>> ParquetBlockOutputStream::internal_type_to_arrow_type = {
    {"UInt8",   arrow::uint8()},
    {"Int8",    arrow::int8()},
    {"UInt16",  arrow::uint16()},
    {"Int16",   arrow::int16()},
    {"UInt32",  arrow::uint32()},
    {"Int32",   arrow::int32()},
    {"UInt64",  arrow::uint64()},
    {"Int64",   arrow::int64()},
    {"Float32", arrow::float32()},
    {"Float64", arrow::float64()},

    {"Date",  arrow::date32()},

    // TODO: ClickHouse can actually store non-utf8 strings!
    {"String", arrow::utf8()}//,
    // TODO: add other types:
    // 1. FixedString
    // 2. DateTime
};

const PaddedPODArray<UInt8> * extractNullBytemapPtr(ColumnPtr column)
{
    ColumnPtr null_column = static_cast<const ColumnNullable &>(*column).getNullMapColumnPtr();
    const PaddedPODArray<UInt8> & null_bytemap = static_cast<const ColumnVector<UInt8> &>(*null_column).getData();
    return &null_bytemap;
}

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
        // TODO: constructed every iteration
        const ColumnWithTypeAndName & column = block.safeGetByPosition(column_i);

        const bool is_column_nullable = column.type->isNullable();
        const DataTypePtr column_nested_type =
            is_column_nullable
                ? static_cast<const DataTypeNullable *>(column.type.get())->getNestedType()
                : column.type;
        const DataTypePtr column_type = column.type;
        // TODO: do not mix std::string and String
        const std::string column_nested_type_name = column_nested_type->getName();

        if (internal_type_to_arrow_type.find(column_nested_type_name) == internal_type_to_arrow_type.end())
        {
            throw Exception(
                "The type \"" + column_nested_type_name + "\" of a column \"" + column.name + "\""
                " is not supported for conversion into a Parquet data format"
                /*, ErrorCodes::TODO*/
            );
        }

        arrow_fields.emplace_back(new arrow::Field(
            column.name,
            internal_type_to_arrow_type.at(column_nested_type_name),
            is_column_nullable
        ));
        std::shared_ptr<arrow::Array> arrow_array;

        ColumnPtr nested_column = is_column_nullable ? static_cast<const ColumnNullable &>(*column.column).getNestedColumnPtr() : column.column;
        const PaddedPODArray<UInt8> * null_bytemap = is_column_nullable ? extractNullBytemapPtr(column.column) : nullptr;

        // TODO: use typeid_cast
        if ("String" == column_nested_type_name)
        {
            fillArrowArrayWithStringColumnData(nested_column, arrow_array, null_bytemap);
        }
        else if ("Date" == column_nested_type_name)
        {
            fillArrowArrayWithDateColumnData(nested_column, arrow_array, null_bytemap);
        }
#define DISPATCH(CPP_NUMERIC_TYPE, ARROW_BUILDER_TYPE) \
        else if (#CPP_NUMERIC_TYPE == column_nested_type_name) \
        { \
            fillArrowArrayWithNumericColumnData<CPP_NUMERIC_TYPE, ARROW_BUILDER_TYPE>(nested_column, arrow_array, null_bytemap); \
        }

        FOR_INTERNAL_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
        // TODO: there are also internal types that are convertable to parquet/arrow once:
        // 1. FixedString(N)
        // 2. DateTime
        else
        {
            throw Exception(
                "Internal type \"" + column_nested_type_name + "\" of a column \"" + column.name + "\""
                " is not supported for conversion into a Parquet data format"/*, ErrorCodes::TODO*/
            );
        }

        arrow_arrays.emplace_back(std::move(arrow_array));
    }

    std::shared_ptr<arrow::Schema> arrow_schema = std::make_shared<arrow::Schema>(std::move(arrow_fields));
    std::shared_ptr<arrow::Table> arrow_table = arrow::Table::Make(arrow_schema, arrow_arrays);

    // TODO: get rid of extra copying
    std::shared_ptr<parquet::InMemoryOutputStream> sink = std::make_shared<parquet::InMemoryOutputStream>();

    // TODO: calculate row_group_size depending on a number of rows and table size

    arrow::Status write_status = parquet::arrow::WriteTable(
        *arrow_table, arrow::default_memory_pool(), sink,
        /* row_group_size = */arrow_table->num_rows(), parquet::default_writer_properties(),
        parquet::arrow::default_arrow_writer_properties()
    );
    if (!write_status.ok())
        throw Exception("Error while writing a table: " + write_status.ToString()/*, ErrorCodes::TODO*/);

    std::shared_ptr<arrow::Buffer> table_buffer = sink->GetBuffer();
    writeString(reinterpret_cast<const char *>(table_buffer->data()), table_buffer->size(), ostr);
}

};
