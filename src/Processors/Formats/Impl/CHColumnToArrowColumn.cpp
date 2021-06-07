#include "CHColumnToArrowColumn.h"

#if USE_ARROW || USE_PARQUET

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Core/callOnTypeIndex.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeArray.h>
#include <Processors/Formats/IOutputFormat.h>
#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/type.h>
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


    static void checkStatus(const arrow::Status & status, const String & column_name, const String & format_name)
    {
        if (!status.ok())
            throw Exception{"Error with a " + format_name + " column \"" + column_name + "\": " + status.ToString(), ErrorCodes::UNKNOWN_EXCEPTION};
    }

    template <typename NumericType, typename ArrowBuilderType>
    static void fillArrowArrayWithNumericColumnData(
        ColumnPtr write_column,
        const PaddedPODArray<UInt8> * null_bytemap,
        const String & format_name,
        arrow::ArrayBuilder* array_builder,
        size_t start,
        size_t end)
    {
        const PaddedPODArray<NumericType> & internal_data = assert_cast<const ColumnVector<NumericType> &>(*write_column).getData();
        ArrowBuilderType & builder = assert_cast<ArrowBuilderType &>(*array_builder);
        arrow::Status status;

        const UInt8 * arrow_null_bytemap_raw_ptr = nullptr;
        PaddedPODArray<UInt8> arrow_null_bytemap;
        if (null_bytemap)
        {
            /// Invert values since Arrow interprets 1 as a non-null value, while CH as a null
            arrow_null_bytemap.reserve(end - start);
            for (size_t i = start; i < end; ++i)
                arrow_null_bytemap.template emplace_back(!(*null_bytemap)[i]);

            arrow_null_bytemap_raw_ptr = arrow_null_bytemap.data();
        }

        if constexpr (std::is_same_v<NumericType, UInt8>)
            status = builder.AppendValues(
                reinterpret_cast<const uint8_t *>(internal_data.data() + start),
                end - start,
                reinterpret_cast<const uint8_t *>(arrow_null_bytemap_raw_ptr));
        else
            status = builder.AppendValues(internal_data.data() + start, end - start, reinterpret_cast<const uint8_t *>(arrow_null_bytemap_raw_ptr));
        checkStatus(status, write_column->getName(), format_name);
    }

    static void fillArrowArray(
        const String & column_name,
        ColumnPtr & column,
        const std::shared_ptr<const IDataType> & column_type,
        const PaddedPODArray<UInt8> * null_bytemap,
        arrow::ArrayBuilder * array_builder,
        String format_name,
        size_t start,
        size_t end);

    static void fillArrowArrayWithArrayColumnData(
        const String & column_name,
        ColumnPtr & column,
        const std::shared_ptr<const IDataType> & column_type,
        const PaddedPODArray<UInt8> * null_bytemap,
        arrow::ArrayBuilder * array_builder,
        String format_name,
        size_t start,
        size_t end)
    {
        const auto * column_array = static_cast<const ColumnArray *>(column.get());
        ColumnPtr nested_column = column_array->getDataPtr();
        DataTypePtr nested_type = typeid_cast<const DataTypeArray *>(column_type.get())->getNestedType();
        const auto & offsets = column_array->getOffsets();

        arrow::ListBuilder & builder = assert_cast<arrow::ListBuilder &>(*array_builder);
        arrow::ArrayBuilder * value_builder = builder.value_builder();
        arrow::Status components_status;

        for (size_t array_idx = start; array_idx < end; ++array_idx)
        {
            /// Start new array
            components_status = builder.Append();
            checkStatus(components_status, nested_column->getName(), format_name);
            fillArrowArray(column_name, nested_column, nested_type, null_bytemap, value_builder, format_name, offsets[array_idx - 1], offsets[array_idx]);
        }
    }

    template <typename ColumnType>
    static void fillArrowArrayWithStringColumnData(
        ColumnPtr write_column,
        const PaddedPODArray<UInt8> * null_bytemap,
        const String & format_name,
        arrow::ArrayBuilder* array_builder,
        size_t start,
        size_t end)
    {
        const auto & internal_column = assert_cast<const ColumnType &>(*write_column);
        arrow::StringBuilder & builder = assert_cast<arrow::StringBuilder &>(*array_builder);
        arrow::Status status;

        for (size_t string_i = start; string_i < end; ++string_i)
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
    }

    static void fillArrowArrayWithDateColumnData(
        ColumnPtr write_column,
        const PaddedPODArray<UInt8> * null_bytemap,
        const String & format_name,
        arrow::ArrayBuilder* array_builder,
        size_t start,
        size_t end)
    {
        const PaddedPODArray<UInt16> & internal_data = assert_cast<const ColumnVector<UInt16> &>(*write_column).getData();
        //arrow::Date32Builder date_builder;
        arrow::UInt16Builder & builder = assert_cast<arrow::UInt16Builder &>(*array_builder);
        arrow::Status status;

        for (size_t value_i = start; value_i < end; ++value_i)
        {
            if (null_bytemap && (*null_bytemap)[value_i])
                status = builder.AppendNull();
            else
                /// Implicitly converts UInt16 to Int32
                status = builder.Append(internal_data[value_i]);
            checkStatus(status, write_column->getName(), format_name);
        }
    }

    static void fillArrowArrayWithDateTimeColumnData(
        ColumnPtr write_column,
        const PaddedPODArray<UInt8> * null_bytemap,
        const String & format_name,
        arrow::ArrayBuilder* array_builder,
        size_t start,
        size_t end)
    {
        const auto & internal_data = assert_cast<const ColumnVector<UInt32> &>(*write_column).getData();
        //arrow::Date64Builder builder;
        arrow::UInt32Builder & builder = assert_cast<arrow::UInt32Builder &>(*array_builder);
        arrow::Status status;

        for (size_t value_i = start; value_i < end; ++value_i)
        {
            if (null_bytemap && (*null_bytemap)[value_i])
                status = builder.AppendNull();
            else
                /// Implicitly converts UInt16 to Int32
                //status = date_builder.Append(static_cast<int64_t>(internal_data[value_i]) * 1000); // now ms. TODO check other units
                status = builder.Append(internal_data[value_i]);

            checkStatus(status, write_column->getName(), format_name);
        }
    }

    static void fillArrowArray(
        const String & column_name,
        ColumnPtr & column,
        const std::shared_ptr<const IDataType> & column_type,
        const PaddedPODArray<UInt8> * null_bytemap,
        arrow::ArrayBuilder * array_builder,
        String format_name,
        size_t start,
        size_t end)
    {
        const String column_type_name = column_type->getFamilyName();

        if ("Nullable" == column_type_name)
        {
            const ColumnNullable * column_nullable = checkAndGetColumn<ColumnNullable>(column.get());
            ColumnPtr nested_column = column_nullable->getNestedColumnPtr();
            DataTypePtr nested_type = typeid_cast<const DataTypeNullable *>(column_type.get())->getNestedType();
            const ColumnPtr & null_column = column_nullable->getNullMapColumnPtr();
            const PaddedPODArray<UInt8> & bytemap = assert_cast<const ColumnVector<UInt8> &>(*null_column).getData();
            fillArrowArray(column_name, nested_column, nested_type, &bytemap, array_builder, format_name, start, end);
        }
        else if ("String" == column_type_name)
        {
            fillArrowArrayWithStringColumnData<ColumnString>(column, null_bytemap, format_name, array_builder, start, end);
        }
        else if ("FixedString" == column_type_name)
        {
            fillArrowArrayWithStringColumnData<ColumnFixedString>(column, null_bytemap, format_name, array_builder, start, end);
        }
        else if ("Date" == column_type_name)
        {
            fillArrowArrayWithDateColumnData(column, null_bytemap, format_name, array_builder, start, end);
        }
        else if ("DateTime" == column_type_name)
        {
            fillArrowArrayWithDateTimeColumnData(column, null_bytemap, format_name, array_builder, start, end);
        }
        else if ("Array" == column_type_name)
        {
            fillArrowArrayWithArrayColumnData(column_name, column, column_type, null_bytemap, array_builder, format_name, start, end);
        }
        else if (isDecimal(column_type))
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
                    fillArrowArrayWithDecimalColumnData<ToDataType>(column, null_bytemap, array_builder, format_name, start, end);
                }
                return false;
            };
            callOnIndexAndDataType<void>(column_type->getTypeId(), fill_decimal);
        }
    #define DISPATCH(CPP_NUMERIC_TYPE, ARROW_BUILDER_TYPE) \
                else if (#CPP_NUMERIC_TYPE == column_type_name) \
                { \
                    fillArrowArrayWithNumericColumnData<CPP_NUMERIC_TYPE, ARROW_BUILDER_TYPE>(column, null_bytemap, format_name, array_builder, start, end); \
                }

        FOR_INTERNAL_NUMERIC_TYPES(DISPATCH)
    #undef DISPATCH
        else
        {
            throw Exception
                {
                    "Internal type \"" + column_type_name + "\" of a column \"" + column_name + "\" is not supported for conversion into a " + format_name + " data format",
                    ErrorCodes::UNKNOWN_TYPE
                };
        }
    }

    template <typename DataType>
    static void fillArrowArrayWithDecimalColumnData(
        ColumnPtr write_column,
        const PaddedPODArray<UInt8> * null_bytemap,
        arrow::ArrayBuilder * array_builder,
        const String & format_name,
        size_t start,
        size_t end)
    {
        const auto & column = static_cast<const typename DataType::ColumnType &>(*write_column);
        arrow::DecimalBuilder & builder = assert_cast<arrow::DecimalBuilder &>(*array_builder);
        arrow::Status status;

        for (size_t value_i = start; value_i < end; ++value_i)
        {
            if (null_bytemap && (*null_bytemap)[value_i])
                status = builder.AppendNull();
            else
                status = builder.Append(
                    arrow::Decimal128(reinterpret_cast<const uint8_t *>(&column.getElement(value_i).value))); // TODO: try copy column

            checkStatus(status, write_column->getName(), format_name);
        }
        checkStatus(status, write_column->getName(), format_name);
    }

    static std::shared_ptr<arrow::DataType> getArrowType(DataTypePtr column_type, const std::string & column_name, const std::string & format_name, bool * is_column_nullable)
    {
        if (column_type->isNullable())
        {
            DataTypePtr nested_type = typeid_cast<const DataTypeNullable *>(column_type.get())->getNestedType();
            auto arrow_type = getArrowType(nested_type, column_name, format_name, is_column_nullable);
            *is_column_nullable = true;
            return arrow_type;
        }

        if (isDecimal(column_type))
        {
            std::shared_ptr<arrow::DataType> arrow_type;
            const auto create_arrow_type = [&](const auto & types) -> bool {
                using Types = std::decay_t<decltype(types)>;
                using ToDataType = typename Types::LeftType;

                if constexpr (
                    std::is_same_v<ToDataType, DataTypeDecimal<Decimal32>>
                    || std::is_same_v<ToDataType, DataTypeDecimal<Decimal64>>
                    || std::is_same_v<ToDataType, DataTypeDecimal<Decimal128>>)
                {
                    const auto & decimal_type = static_cast<const ToDataType *>(column_type.get());
                    arrow_type = arrow::decimal(decimal_type->getPrecision(), decimal_type->getScale());
                }

                return false;
            };
            callOnIndexAndDataType<void>(column_type->getTypeId(), create_arrow_type);
            return arrow_type;
        }

        if (isArray(column_type))
        {
            auto nested_type = typeid_cast<const DataTypeArray *>(column_type.get())->getNestedType();
            auto nested_arrow_type = getArrowType(nested_type, column_name, format_name, is_column_nullable);
            return arrow::list(nested_arrow_type);
        }

        const std::string type_name = column_type->getFamilyName();
        if (const auto * arrow_type_it = std::find_if(
                internal_type_to_arrow_type.begin(),
                internal_type_to_arrow_type.end(),
                [=](auto && elem) { return elem.first == type_name; });
            arrow_type_it != internal_type_to_arrow_type.end())
        {
            return arrow_type_it->second;
        }

        throw Exception{"The type \"" + column_name + "\" of a column \"" + column_name + "\""
                             " is not supported for conversion into a " + format_name + " data format",
                             ErrorCodes::UNKNOWN_TYPE};
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

            bool is_column_nullable = false;
            auto arrow_type = getArrowType(column.type, column.name, format_name, &is_column_nullable);
            arrow_fields.emplace_back(std::make_shared<arrow::Field>(column.name, arrow_type, is_column_nullable));

            arrow::MemoryPool* pool = arrow::default_memory_pool();
            std::unique_ptr<arrow::ArrayBuilder> array_builder;
            arrow::Status status = MakeBuilder(pool, arrow_fields[column_i]->type(), &array_builder);
            checkStatus(status, column.column->getName(), format_name);

            fillArrowArray(column.name, column.column, column.type, nullptr, array_builder.get(), format_name, 0, column.column->size());

            std::shared_ptr<arrow::Array> arrow_array;
            status = array_builder->Finish(&arrow_array);
            checkStatus(status, column.column->getName(), format_name);
            arrow_arrays.emplace_back(std::move(arrow_array));
        }

        std::shared_ptr<arrow::Schema> arrow_schema = std::make_shared<arrow::Schema>(std::move(arrow_fields));

        res = arrow::Table::Make(arrow_schema, arrow_arrays);
    }
}

#endif
