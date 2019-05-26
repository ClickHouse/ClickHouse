#include <Common/config.h>
#if USE_PARQUET
#    include "ParquetBlockOutputStream.h"

// TODO: clean includes
#    include <Columns/ColumnDecimal.h>
#    include <Columns/ColumnFixedString.h>
#    include <Columns/ColumnNullable.h>
#    include <Columns/ColumnString.h>
#    include <Columns/ColumnVector.h>
#    include <Columns/ColumnsNumber.h>
#    include <Core/ColumnWithTypeAndName.h>
#    include <Core/callOnTypeIndex.h>
#    include <DataTypes/DataTypeDateTime.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypesDecimal.h>
#    include <DataStreams/SquashingBlockOutputStream.h>
#    include <Formats/FormatFactory.h>
#    include <IO/WriteHelpers.h>
#    include <arrow/api.h>
#    include <arrow/io/api.h>
#    include <arrow/util/decimal.h>
#    include <parquet/arrow/writer.h>
#    include <parquet/exception.h>
#    include <parquet/util/memory.h>

#    include <Core/iostream_debug_helpers.h> // REMOVE ME

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
    extern const int UNKNOWN_TYPE;
}

ParquetBlockOutputStream::ParquetBlockOutputStream(WriteBuffer & ostr, const Block & header, const FormatSettings & format_settings) : ostr{ostr}, header{header}, format_settings{format_settings}
{
}

void ParquetBlockOutputStream::flush()
{
    ostr.next();
}

void checkStatus(arrow::Status & status, const std::string & column_name)
{
    if (!status.ok())
        throw Exception{"Error with a parquet column \"" + column_name + "\": " + status.ToString(), ErrorCodes::UNKNOWN_EXCEPTION};
}

template <typename NumericType, typename ArrowBuilderType>
void fillArrowArrayWithNumericColumnData(
    ColumnPtr write_column, std::shared_ptr<arrow::Array> & arrow_array, const PaddedPODArray<UInt8> * null_bytemap)
{
    const PaddedPODArray<NumericType> & internal_data = static_cast<const ColumnVector<NumericType> &>(*write_column).getData();
    ArrowBuilderType builder;
    arrow::Status status;

    const UInt8 * arrow_null_bytemap_raw_ptr = nullptr;
    PaddedPODArray<UInt8> arrow_null_bytemap;
    if (null_bytemap)
    {
        /// Invert values since Arrow interprets 1 as a non-null value, while CH as a null
        arrow_null_bytemap.reserve(null_bytemap->size());
        for (size_t i = 0, size = null_bytemap->size(); i < size; ++i)
            arrow_null_bytemap.emplace_back(1 ^ (*null_bytemap)[i]);

        arrow_null_bytemap_raw_ptr = arrow_null_bytemap.data();
    }

    status = builder.AppendValues(internal_data.data(), internal_data.size(), arrow_null_bytemap_raw_ptr);
    checkStatus(status, write_column->getName());

    status = builder.Finish(&arrow_array);
    checkStatus(status, write_column->getName());
}

template <typename ColumnType>
void fillArrowArrayWithStringColumnData(
    ColumnPtr write_column, std::shared_ptr<arrow::Array> & arrow_array, const PaddedPODArray<UInt8> * null_bytemap)
{
    const auto & internal_column = static_cast<const ColumnType &>(*write_column);
    arrow::StringBuilder builder;
    arrow::Status status;

    for (size_t string_i = 0, size = internal_column.size(); string_i < size; ++string_i)
    {
        if (null_bytemap && (*null_bytemap)[string_i])
        {
            status = builder.AppendNull();
        }
        else
        {
            StringRef string_ref = internal_column.getDataAt(string_i);
            status = builder.Append(string_ref.data, string_ref.size);
        }

        checkStatus(status, write_column->getName());
    }

    status = builder.Finish(&arrow_array);
    checkStatus(status, write_column->getName());
}

void fillArrowArrayWithDateColumnData(
    ColumnPtr write_column, std::shared_ptr<arrow::Array> & arrow_array, const PaddedPODArray<UInt8> * null_bytemap)
{
    const PaddedPODArray<UInt16> & internal_data = static_cast<const ColumnVector<UInt16> &>(*write_column).getData();
    //arrow::Date32Builder date_builder;
    arrow::UInt16Builder builder;
    arrow::Status status;

    for (size_t value_i = 0, size = internal_data.size(); value_i < size; ++value_i)
    {
        if (null_bytemap && (*null_bytemap)[value_i])
            status = builder.AppendNull();
        else
            /// Implicitly converts UInt16 to Int32
            status = builder.Append(internal_data[value_i]);
        checkStatus(status, write_column->getName());
    }

    status = builder.Finish(&arrow_array);
    checkStatus(status, write_column->getName());
}

void fillArrowArrayWithDateTimeColumnData(
    ColumnPtr write_column, std::shared_ptr<arrow::Array> & arrow_array, const PaddedPODArray<UInt8> * null_bytemap)
{
    auto & internal_data = static_cast<const ColumnVector<UInt32> &>(*write_column).getData();
    //arrow::Date64Builder builder;
    arrow::UInt32Builder builder;
    arrow::Status status;

    for (size_t value_i = 0, size = internal_data.size(); value_i < size; ++value_i)
    {
        if (null_bytemap && (*null_bytemap)[value_i])
            status = builder.AppendNull();
        else
            /// Implicitly converts UInt16 to Int32
            //status = date_builder.Append(static_cast<int64_t>(internal_data[value_i]) * 1000); // now ms. TODO check other units
            status = builder.Append(internal_data[value_i]);

        checkStatus(status, write_column->getName());
    }

    status = builder.Finish(&arrow_array);
    checkStatus(status, write_column->getName());
}

template <typename DataType>
void fillArrowArrayWithDecimalColumnData(
    ColumnPtr write_column,
    std::shared_ptr<arrow::Array> & arrow_array,
    const PaddedPODArray<UInt8> * null_bytemap,
    const DataType * decimal_type)
{
    const auto & column = static_cast<const typename DataType::ColumnType &>(*write_column);
    arrow::DecimalBuilder builder(arrow::decimal(decimal_type->getPrecision(), decimal_type->getScale()));
    arrow::Status status;

    for (size_t value_i = 0, size = column.size(); value_i < size; ++value_i)
    {
        if (null_bytemap && (*null_bytemap)[value_i])
            status = builder.AppendNull();
        else
            status = builder.Append(
                arrow::Decimal128(reinterpret_cast<const uint8_t *>(&column.getElement(value_i).value))); // TODO: try copy column

        checkStatus(status, write_column->getName());
    }
    status = builder.Finish(&arrow_array);
    checkStatus(status, write_column->getName());

/* TODO column copy
    const auto & internal_data = static_cast<const typename DataType::ColumnType &>(*write_column).getData();
    //ArrowBuilderType numeric_builder;
    arrow::DecimalBuilder builder(arrow::decimal(decimal_type->getPrecision(), decimal_type->getScale()));
    arrow::Status status;

    const uint8_t * arrow_null_bytemap_raw_ptr = nullptr;
    PaddedPODArray<UInt8> arrow_null_bytemap;
    if (null_bytemap)
    {
        /// Invert values since Arrow interprets 1 as a non-null value, while CH as a null
        arrow_null_bytemap.reserve(null_bytemap->size());
        for (size_t i = 0, size = null_bytemap->size(); i < size; ++i)
            arrow_null_bytemap.emplace_back(1 ^ (*null_bytemap)[i]);

        arrow_null_bytemap_raw_ptr = arrow_null_bytemap.data();
    }

    status = builder.AppendValues(reinterpret_cast<const uint8_t*>(internal_data.data()), internal_data.size(), arrow_null_bytemap_raw_ptr);
    checkStatus(status, write_column->getName());

    status = builder.Finish(&arrow_array);
    checkStatus(status, write_column->getName());
*/
}

#    define FOR_INTERNAL_NUMERIC_TYPES(M) \
        M(UInt8, arrow::UInt8Builder) \
        M(Int8, arrow::Int8Builder) \
        M(UInt16, arrow::UInt16Builder) \
        M(Int16, arrow::Int16Builder) \
        M(UInt32, arrow::UInt32Builder) \
        M(Int32, arrow::Int32Builder) \
        M(UInt64, arrow::UInt64Builder) \
        M(Int64, arrow::Int64Builder) \
        M(Float32, arrow::FloatBuilder) \
        M(Float64, arrow::DoubleBuilder)

const std::unordered_map<String, std::shared_ptr<arrow::DataType>> internal_type_to_arrow_type = {
    {"UInt8", arrow::uint8()},
    {"Int8", arrow::int8()},
    {"UInt16", arrow::uint16()},
    {"Int16", arrow::int16()},
    {"UInt32", arrow::uint32()},
    {"Int32", arrow::int32()},
    {"UInt64", arrow::uint64()},
    {"Int64", arrow::int64()},
    {"Float32", arrow::float32()},
    {"Float64", arrow::float64()},

    //{"Date", arrow::date64()},
    //{"Date", arrow::date32()},
    {"Date", arrow::uint16()}, // CHECK
    //{"DateTime", arrow::date64()}, // BUG! saves as date32
    {"DateTime", arrow::uint32()},

    // TODO: ClickHouse can actually store non-utf8 strings!
    {"String", arrow::utf8()},
    {"FixedString", arrow::utf8()},
};

const PaddedPODArray<UInt8> * extractNullBytemapPtr(ColumnPtr column)
{
    ColumnPtr null_column = static_cast<const ColumnNullable &>(*column).getNullMapColumnPtr();
    const PaddedPODArray<UInt8> & null_bytemap = static_cast<const ColumnVector<UInt8> &>(*null_column).getData();
    return &null_bytemap;
}


class OstreamOutputStream : public parquet::OutputStream
{
public:
    explicit OstreamOutputStream(WriteBuffer & ostr_) : ostr(ostr_) {}
    virtual ~OstreamOutputStream() {}
    virtual void Close() {}
    virtual int64_t Tell() { return total_length; }
    virtual void Write(const uint8_t * data, int64_t length)
    {
        ostr.write(reinterpret_cast<const char *>(data), length);
        total_length += length;
    }

private:
    WriteBuffer & ostr;
    int64_t total_length = 0;

    PARQUET_DISALLOW_COPY_AND_ASSIGN(OstreamOutputStream);
};


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
        const auto & column_nested_type
            = is_column_nullable ? static_cast<const DataTypeNullable *>(column.type.get())->getNestedType() : column.type;
        const std::string column_nested_type_name = column_nested_type->getFamilyName();

        if (isDecimal(column_nested_type))
        {
            const auto add_decimal_field = [&](const auto & types) -> bool {
                using Types = std::decay_t<decltype(types)>;
                using ToDataType = typename Types::LeftType;

                if constexpr (
                    std::is_same_v<
                        ToDataType,
                        DataTypeDecimal<
                            Decimal32>> || std::is_same_v<ToDataType, DataTypeDecimal<Decimal64>> || std::is_same_v<ToDataType, DataTypeDecimal<Decimal128>>)
                {
                    const auto & decimal_type = static_cast<const ToDataType *>(column_nested_type.get());
                    arrow_fields.emplace_back(std::make_shared<arrow::Field>(
                        column.name, arrow::decimal(decimal_type->getPrecision(), decimal_type->getScale()), is_column_nullable));
                }

                return false;
            };
            callOnIndexAndDataType<void>(column_nested_type->getTypeId(), add_decimal_field);
        }
        else
        {
            if (internal_type_to_arrow_type.find(column_nested_type_name) == internal_type_to_arrow_type.end())
            {
                throw Exception{"The type \"" + column_nested_type_name + "\" of a column \"" + column.name
                                    + "\""
                                      " is not supported for conversion into a Parquet data format",
                                ErrorCodes::UNKNOWN_TYPE};
            }

            arrow_fields.emplace_back(std::make_shared<arrow::Field>(column.name, internal_type_to_arrow_type.at(column_nested_type_name), is_column_nullable));
        }

        std::shared_ptr<arrow::Array> arrow_array;

        ColumnPtr nested_column
            = is_column_nullable ? static_cast<const ColumnNullable &>(*column.column).getNestedColumnPtr() : column.column;
        const PaddedPODArray<UInt8> * null_bytemap = is_column_nullable ? extractNullBytemapPtr(column.column) : nullptr;

        if ("String" == column_nested_type_name)
        {
            fillArrowArrayWithStringColumnData<ColumnString>(nested_column, arrow_array, null_bytemap);
        }
        else if ("FixedString" == column_nested_type_name)
        {
            fillArrowArrayWithStringColumnData<ColumnFixedString>(nested_column, arrow_array, null_bytemap);
        }
        else if ("Date" == column_nested_type_name)
        {
            fillArrowArrayWithDateColumnData(nested_column, arrow_array, null_bytemap);
        }
        else if ("DateTime" == column_nested_type_name)
        {
            fillArrowArrayWithDateTimeColumnData(nested_column, arrow_array, null_bytemap);
        }

        else if (isDecimal(column_nested_type))
        {
            auto fill_decimal = [&](const auto & types) -> bool
            {
                using Types = std::decay_t<decltype(types)>;
                using ToDataType = typename Types::LeftType;
                if constexpr (
                    std::is_same_v<
                        ToDataType,
                        DataTypeDecimal<
                            Decimal32>> || std::is_same_v<ToDataType, DataTypeDecimal<Decimal64>> || std::is_same_v<ToDataType, DataTypeDecimal<Decimal128>>)
                {
                    const auto & decimal_type = static_cast<const ToDataType *>(column_nested_type.get());
                    fillArrowArrayWithDecimalColumnData(nested_column, arrow_array, null_bytemap, decimal_type);
                }
                return false;
            };
            callOnIndexAndDataType<void>(column_nested_type->getTypeId(), fill_decimal);
        }
#    define DISPATCH(CPP_NUMERIC_TYPE, ARROW_BUILDER_TYPE) \
        else if (#CPP_NUMERIC_TYPE == column_nested_type_name) \
        { \
            fillArrowArrayWithNumericColumnData<CPP_NUMERIC_TYPE, ARROW_BUILDER_TYPE>(nested_column, arrow_array, null_bytemap); \
        }

        FOR_INTERNAL_NUMERIC_TYPES(DISPATCH)
#    undef DISPATCH
        else
        {
            throw Exception{"Internal type \"" + column_nested_type_name + "\" of a column \"" + column.name
                                + "\""
                                  " is not supported for conversion into a Parquet data format",
                            ErrorCodes::UNKNOWN_TYPE};
        }


        arrow_arrays.emplace_back(std::move(arrow_array));
    }

    std::shared_ptr<arrow::Schema> arrow_schema = std::make_shared<arrow::Schema>(std::move(arrow_fields));

    std::shared_ptr<arrow::Table> arrow_table = arrow::Table::Make(arrow_schema, arrow_arrays);

    auto sink = std::make_shared<OstreamOutputStream>(ostr);

    if (!file_writer)
    {

        parquet::WriterProperties::Builder builder;
#if USE_SNAPPY
        builder.compression(parquet::Compression::SNAPPY);
#endif
        auto props = builder.build();
        auto status = parquet::arrow::FileWriter::Open(
            *arrow_table->schema(),
            arrow::default_memory_pool(),
            sink,
            props, /*parquet::default_writer_properties(),*/
            parquet::arrow::default_arrow_writer_properties(),
            &file_writer);
        if (!status.ok())
            throw Exception{"Error while opening a table: " + status.ToString(), ErrorCodes::UNKNOWN_EXCEPTION};
    }

    // TODO: calculate row_group_size depending on a number of rows and table size
    auto status = file_writer->WriteTable(*arrow_table, format_settings.parquet.row_group_size);

    if (!status.ok())
        throw Exception{"Error while writing a table: " + status.ToString(), ErrorCodes::UNKNOWN_EXCEPTION};
}

void ParquetBlockOutputStream::writeSuffix()
{
    if (file_writer)
    {
        auto status = file_writer->Close();
        if (!status.ok())
            throw Exception{"Error while closing a table: " + status.ToString(), ErrorCodes::UNKNOWN_EXCEPTION};
    }
}


void registerOutputFormatParquet(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "Parquet", [](WriteBuffer & buf, const Block & sample, const Context & /*context*/, const FormatSettings & format_settings)
        {
            BlockOutputStreamPtr impl = std::make_shared<ParquetBlockOutputStream>(buf, sample, format_settings);
            auto res = std::make_shared<SquashingBlockOutputStream>(impl, impl->getHeader(), format_settings.parquet.row_group_size, 0);
            res->disableFlush();
            return res;
        });
}

}


#else

namespace DB
{
class FormatFactory;
void registerOutputFormatParquet(FormatFactory &)
{
}
}


#endif
