#include "CHColumnToArrowColumn.h"

#if USE_ARROW || USE_PARQUET

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Core/callOnTypeIndex.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Processors/Formats/IOutputFormat.h>
#include <arrow/api.h>
#include <arrow/util/decimal.h>
#include <DataTypes/DataTypeLowCardinality.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int UNKNOWN_EXCEPTION;
        extern const int UNKNOWN_TYPE;
    }

    static const std::initializer_list<std::pair<String, std::shared_ptr<arrow::DataType>>> internal_type_to_arrow_type =
    {
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

    static const PaddedPODArray<UInt8> * extractNullBytemapPtr(ColumnPtr column)
    {
        ColumnPtr null_column = assert_cast<const ColumnNullable &>(*column).getNullMapColumnPtr();
        const PaddedPODArray<UInt8> & null_bytemap = assert_cast<const ColumnVector<UInt8> &>(*null_column).getData();
        return &null_bytemap;
    }

    static void checkStatus(const arrow::Status & status, const String & column_name, const String & format_name)
    {
        if (!status.ok())
            throw Exception{"Error with a " + format_name + " column \"" + column_name + "\": " + status.ToString(), ErrorCodes::UNKNOWN_EXCEPTION};
    }

    template <typename NumericType, typename ArrowBuilderType>
    static void fillArrowArrayWithNumericColumnData(
        ColumnPtr write_column,
        std::shared_ptr<arrow::Array> & arrow_array,
        const PaddedPODArray<UInt8> * null_bytemap,
        const String & format_name)
    {
        const PaddedPODArray<NumericType> & internal_data = assert_cast<const ColumnVector<NumericType> &>(*write_column).getData();
        ArrowBuilderType builder;
        arrow::Status status;

        const UInt8 * arrow_null_bytemap_raw_ptr = nullptr;
        PaddedPODArray<UInt8> arrow_null_bytemap;
        if (null_bytemap)
        {
            /// Invert values since Arrow interprets 1 as a non-null value, while CH as a null
            arrow_null_bytemap.reserve(null_bytemap->size());
            for (auto is_null : *null_bytemap)
                arrow_null_bytemap.emplace_back(!is_null);

            arrow_null_bytemap_raw_ptr = arrow_null_bytemap.data();
        }

        if constexpr (std::is_same_v<NumericType, UInt8>)
            status = builder.AppendValues(
                reinterpret_cast<const uint8_t *>(internal_data.data()),
                internal_data.size(),
                reinterpret_cast<const uint8_t *>(arrow_null_bytemap_raw_ptr));
        else
            status = builder.AppendValues(internal_data.data(), internal_data.size(), reinterpret_cast<const uint8_t *>(arrow_null_bytemap_raw_ptr));
        checkStatus(status, write_column->getName(), format_name);

        status = builder.Finish(&arrow_array);
        checkStatus(status, write_column->getName(), format_name);
    }

    template <typename ColumnType>
    static void fillArrowArrayWithStringColumnData(
        ColumnPtr write_column,
        std::shared_ptr<arrow::Array> & arrow_array,
        const PaddedPODArray<UInt8> * null_bytemap,
        const String & format_name)
    {
        const auto & internal_column = assert_cast<const ColumnType &>(*write_column);
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

            checkStatus(status, write_column->getName(), format_name);
        }

        status = builder.Finish(&arrow_array);
        checkStatus(status, write_column->getName(), format_name);
    }

    static void fillArrowArrayWithDateColumnData(
        ColumnPtr write_column,
        std::shared_ptr<arrow::Array> & arrow_array,
        const PaddedPODArray<UInt8> * null_bytemap,
        const String & format_name)
    {
        const PaddedPODArray<UInt16> & internal_data = assert_cast<const ColumnVector<UInt16> &>(*write_column).getData();
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
            checkStatus(status, write_column->getName(), format_name);
        }

        status = builder.Finish(&arrow_array);
        checkStatus(status, write_column->getName(), format_name);
    }

    static void fillArrowArrayWithDateTimeColumnData(
        ColumnPtr write_column,
        std::shared_ptr<arrow::Array> & arrow_array,
        const PaddedPODArray<UInt8> * null_bytemap,
        const String & format_name)
    {
        const auto & internal_data = assert_cast<const ColumnVector<UInt32> &>(*write_column).getData();
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

            checkStatus(status, write_column->getName(), format_name);
        }

        status = builder.Finish(&arrow_array);
        checkStatus(status, write_column->getName(), format_name);
    }

    template <typename DataType>
    static void fillArrowArrayWithDecimalColumnData(
        ColumnPtr write_column,
        std::shared_ptr<arrow::Array> & arrow_array,
        const PaddedPODArray<UInt8> * null_bytemap,
        const DataType * decimal_type,
        const String & format_name)
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

            checkStatus(status, write_column->getName(), format_name);
        }
        status = builder.Finish(&arrow_array);
        checkStatus(status, write_column->getName(), format_name);
    }

    void CHColumnToArrowColumn::chChunkToArrowTable(
        std::shared_ptr<arrow::Table> & res,
        const Block & header,
        const Chunk & chunk,
        size_t columns_num,
        String format_name)
    {
        /// For arrow::Schema and arrow::Table creation
        std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
        std::vector<std::shared_ptr<arrow::Array>> arrow_arrays;
        arrow_fields.reserve(columns_num);
        arrow_arrays.reserve(columns_num);

        for (size_t column_i = 0; column_i < columns_num; ++column_i)
        {
            // TODO: constructed every iteration
            ColumnWithTypeAndName column = header.safeGetByPosition(column_i);
            column.column = recursiveRemoveLowCardinality(chunk.getColumns()[column_i]);
            column.type = recursiveRemoveLowCardinality(column.type);

            const bool is_column_nullable = column.type->isNullable();
            const auto & column_nested_type
                = is_column_nullable ? static_cast<const DataTypeNullable *>(column.type.get())->getNestedType() : column.type;
            const String column_nested_type_name = column_nested_type->getFamilyName();

            if (isDecimal(column_nested_type))
            {
                const auto add_decimal_field = [&](const auto & types) -> bool {
                    using Types = std::decay_t<decltype(types)>;
                    using ToDataType = typename Types::LeftType;

                    if constexpr (
                        std::is_same_v<ToDataType, DataTypeDecimal<Decimal32>>
                    || std::is_same_v<ToDataType, DataTypeDecimal<Decimal64>>
                    || std::is_same_v<ToDataType, DataTypeDecimal<Decimal128>>)
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
                if (const auto * arrow_type_it = std::find_if(internal_type_to_arrow_type.begin(), internal_type_to_arrow_type.end(),
                    [=](auto && elem) { return elem.first == column_nested_type_name; });
                    arrow_type_it != internal_type_to_arrow_type.end())
                {
                    arrow_fields.emplace_back(std::make_shared<arrow::Field>(column.name, arrow_type_it->second, is_column_nullable));
                } else
                {
                    throw Exception{"The type \"" + column_nested_type_name + "\" of a column \"" + column.name + "\""
                                    " is not supported for conversion into a " + format_name + " data format",
                                    ErrorCodes::UNKNOWN_TYPE};
                }
            }

            ColumnPtr nested_column
                = is_column_nullable ? assert_cast<const ColumnNullable &>(*column.column).getNestedColumnPtr() : column.column;
            const PaddedPODArray<UInt8> * null_bytemap = is_column_nullable ? extractNullBytemapPtr(column.column) : nullptr;

            std::shared_ptr<arrow::Array> arrow_array;

            if ("String" == column_nested_type_name)
            {
                fillArrowArrayWithStringColumnData<ColumnString>(nested_column, arrow_array, null_bytemap, format_name);
            }
            else if ("FixedString" == column_nested_type_name)
            {
                fillArrowArrayWithStringColumnData<ColumnFixedString>(nested_column, arrow_array, null_bytemap, format_name);
            }
            else if ("Date" == column_nested_type_name)
            {
                fillArrowArrayWithDateColumnData(nested_column, arrow_array, null_bytemap, format_name);
            }
            else if ("DateTime" == column_nested_type_name)
            {
                fillArrowArrayWithDateTimeColumnData(nested_column, arrow_array, null_bytemap, format_name);
            }
            else if (isDecimal(column_nested_type))
            {
                auto fill_decimal = [&](const auto & types) -> bool
                {
                    using Types = std::decay_t<decltype(types)>;
                    using ToDataType = typename Types::LeftType;
                    if constexpr (
                        std::is_same_v<ToDataType,DataTypeDecimal<Decimal32>>
                    || std::is_same_v<ToDataType, DataTypeDecimal<Decimal64>>
                    || std::is_same_v<ToDataType, DataTypeDecimal<Decimal128>>)
                    {
                        const auto & decimal_type = static_cast<const ToDataType *>(column_nested_type.get());
                        fillArrowArrayWithDecimalColumnData(nested_column, arrow_array, null_bytemap, decimal_type, format_name);
                    }
                    return false;
                };
                callOnIndexAndDataType<void>(column_nested_type->getTypeId(), fill_decimal);
            }
#define DISPATCH(CPP_NUMERIC_TYPE, ARROW_BUILDER_TYPE) \
            else if (#CPP_NUMERIC_TYPE == column_nested_type_name) \
            { \
                fillArrowArrayWithNumericColumnData<CPP_NUMERIC_TYPE, ARROW_BUILDER_TYPE>(nested_column, arrow_array, null_bytemap, format_name); \
            }

            FOR_INTERNAL_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
            else
            {
                throw Exception{"Internal type \"" + column_nested_type_name + "\" of a column \"" + column.name + "\""
                                " is not supported for conversion into a " + format_name + " data format",
                                ErrorCodes::UNKNOWN_TYPE};
            }

            arrow_arrays.emplace_back(std::move(arrow_array));
        }

        std::shared_ptr<arrow::Schema> arrow_schema = std::make_shared<arrow::Schema>(std::move(arrow_fields));

        res = arrow::Table::Make(arrow_schema, arrow_arrays);
    }
}

#endif
