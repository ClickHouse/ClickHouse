#include "CHColumnToArrowColumn.h"

#if USE_ARROW || USE_PARQUET

// #include <base/Decimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Core/callOnTypeIndex.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Processors/Formats/IOutputFormat.h>
#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>

#define FOR_INTERNAL_NUMERIC_TYPES(M) \
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

#define FOR_ARROW_TYPES(M) \
        M(UINT8, arrow::UInt8Type) \
        M(INT8, arrow::Int8Type) \
        M(UINT16, arrow::UInt16Type) \
        M(INT16, arrow::Int16Type) \
        M(UINT32, arrow::UInt32Type) \
        M(INT32, arrow::Int32Type) \
        M(UINT64, arrow::UInt64Type) \
        M(INT64, arrow::Int64Type) \
        M(FLOAT, arrow::FloatType) \
        M(DOUBLE, arrow::DoubleType) \
        M(BINARY, arrow::BinaryType)

namespace DB
{
    namespace ErrorCodes
    {
        extern const int UNKNOWN_EXCEPTION;
        extern const int UNKNOWN_TYPE;
        extern const int LOGICAL_ERROR;
        extern const int DECIMAL_OVERFLOW;
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

        {"Date", arrow::uint16()},      /// uint16 is used instead of date32, because Apache Arrow cannot correctly serialize Date32Array.
        {"DateTime", arrow::uint32()},  /// uint32 is used instead of date64, because we don't need milliseconds.
        {"Date32", arrow::date32()},

        {"String", arrow::binary()},
        {"FixedString", arrow::binary()},
    };


    static void checkStatus(const arrow::Status & status, const String & column_name, const String & format_name)
    {
        if (!status.ok())
            throw Exception{fmt::format("Error with a {} column \"{}\": {}.", format_name, column_name, status.ToString()), ErrorCodes::UNKNOWN_EXCEPTION};
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

    static void fillArrowArrayWithDateTime64ColumnData(
        const DataTypeDateTime64 * type,
        ColumnPtr write_column,
        const PaddedPODArray<UInt8> * null_bytemap,
        const String & format_name,
        arrow::ArrayBuilder* array_builder,
        size_t start,
        size_t end)
    {
        const auto & column = assert_cast<const ColumnDecimal<DateTime64> &>(*write_column);
        arrow::TimestampBuilder & builder = assert_cast<arrow::TimestampBuilder &>(*array_builder);
        arrow::Status status;

        auto scale = type->getScale();
        bool need_rescale = scale % 3;
        auto rescale_multiplier = DecimalUtils::scaleMultiplier<DateTime64::NativeType>(3 - scale % 3);
        for (size_t value_i = start; value_i < end; ++value_i)
        {
            if (null_bytemap && (*null_bytemap)[value_i])
            {
                status = builder.AppendNull();
            }
            else
            {
                auto value = static_cast<Int64>(column[value_i].get<DecimalField<DateTime64>>().getValue());
                if (need_rescale)
                {
                    if (common::mulOverflow(value, rescale_multiplier, value))
                        throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
                }
                status = builder.Append(value);
            }
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
        size_t end,
        bool output_string_as_string,
        std::unordered_map<String, std::shared_ptr<arrow::Array>> & dictionary_values);

    template <typename Builder>
    static void fillArrowArrayWithArrayColumnData(
        const String & column_name,
        ColumnPtr & column,
        const std::shared_ptr<const IDataType> & column_type,
        const PaddedPODArray<UInt8> * null_bytemap,
        arrow::ArrayBuilder * array_builder,
        String format_name,
        size_t start,
        size_t end,
        bool output_string_as_string,
        std::unordered_map<String, std::shared_ptr<arrow::Array>> & dictionary_values)
    {
        const auto * column_array = assert_cast<const ColumnArray *>(column.get());
        ColumnPtr nested_column = column_array->getDataPtr();
        DataTypePtr nested_type = assert_cast<const DataTypeArray *>(column_type.get())->getNestedType();
        const auto & offsets = column_array->getOffsets();

        Builder & builder = assert_cast<Builder &>(*array_builder);
        arrow::ArrayBuilder * value_builder = builder.value_builder();
        arrow::Status components_status;

        for (size_t array_idx = start; array_idx < end; ++array_idx)
        {
            /// Start new array.
            components_status = builder.Append();
            checkStatus(components_status, nested_column->getName(), format_name);
            fillArrowArray(column_name, nested_column, nested_type, null_bytemap, value_builder, format_name, offsets[array_idx - 1], offsets[array_idx], output_string_as_string, dictionary_values);
        }
    }

    static void fillArrowArrayWithTupleColumnData(
        const String & column_name,
        ColumnPtr & column,
        const std::shared_ptr<const IDataType> & column_type,
        const PaddedPODArray<UInt8> * null_bytemap,
        arrow::ArrayBuilder * array_builder,
        String format_name,
        size_t start,
        size_t end,
        bool output_string_as_string,
        std::unordered_map<String, std::shared_ptr<arrow::Array>> & dictionary_values)
    {
        const auto * column_tuple = assert_cast<const ColumnTuple *>(column.get());
        const auto * type_tuple = assert_cast<const DataTypeTuple *>(column_type.get());
        const auto & nested_types = type_tuple->getElements();
        const auto & nested_names = type_tuple->getElementNames();

        arrow::StructBuilder & builder = assert_cast<arrow::StructBuilder &>(*array_builder);

        for (size_t i = 0; i != column_tuple->tupleSize(); ++i)
        {
            ColumnPtr nested_column = column_tuple->getColumnPtr(i);
            fillArrowArray(column_name + "." + nested_names[i], nested_column, nested_types[i], null_bytemap, builder.field_builder(i), format_name, start, end, output_string_as_string, dictionary_values);
        }

        for (size_t i = start; i != end; ++i)
        {
            auto status = builder.Append();
            checkStatus(status, column->getName(), format_name);
        }
    }

    template<typename T>
    static PaddedPODArray<Int64> extractIndexesImpl(ColumnPtr column, size_t start, size_t end)
    {
        const PaddedPODArray<T> & data = assert_cast<const ColumnVector<T> *>(column.get())->getData();
        PaddedPODArray<Int64> result;
        result.reserve(end - start);
        std::transform(data.begin() + start, data.begin() + end, std::back_inserter(result), [](T value) { return Int64(value); });
        return result;
    }

    static PaddedPODArray<Int64> extractIndexesImpl(ColumnPtr column, size_t start, size_t end)
    {
        switch (column->getDataType())
        {
            case TypeIndex::UInt8:
                return extractIndexesImpl<UInt8>(column, start, end);
            case TypeIndex::UInt16:
                return extractIndexesImpl<UInt16>(column, start, end);
            case TypeIndex::UInt32:
                return extractIndexesImpl<UInt32>(column, start, end);
            case TypeIndex::UInt64:
                return extractIndexesImpl<UInt64>(column, start, end);
            default:
                throw Exception(fmt::format("Indexes column must be ColumnUInt, got {}.", column->getName()),
                                ErrorCodes::LOGICAL_ERROR);
        }
    }

    template<typename ValueType>
    static void fillArrowArrayWithLowCardinalityColumnDataImpl(
        const String & column_name,
        ColumnPtr & column,
        const std::shared_ptr<const IDataType> & column_type,
        const PaddedPODArray<UInt8> * null_bytemap,
        arrow::ArrayBuilder * array_builder,
        String format_name,
        size_t start,
        size_t end,
        bool output_string_as_string,
        std::unordered_map<String, std::shared_ptr<arrow::Array>> & dictionary_values)
    {
        const auto * column_lc = assert_cast<const ColumnLowCardinality *>(column.get());
        arrow::DictionaryBuilder<ValueType> * builder = assert_cast<arrow::DictionaryBuilder<ValueType> *>(array_builder);
        auto & dict_values = dictionary_values[column_name];

        /// Convert dictionary from LowCardinality to Arrow dictionary only once and then reuse it.
        if (!dict_values)
        {
            auto value_type = assert_cast<arrow::DictionaryType *>(builder->type().get())->value_type();
            std::unique_ptr<arrow::ArrayBuilder> values_builder;
            arrow::MemoryPool* pool = arrow::default_memory_pool();
            arrow::Status status = MakeBuilder(pool, value_type, &values_builder);
            checkStatus(status, column->getName(), format_name);

            auto dict_column = column_lc->getDictionary().getNestedColumn();
            const auto & dict_type = assert_cast<const DataTypeLowCardinality *>(column_type.get())->getDictionaryType();
            fillArrowArray(column_name, dict_column, dict_type, nullptr, values_builder.get(), format_name, 0, dict_column->size(), output_string_as_string, dictionary_values);
            status = values_builder->Finish(&dict_values);
            checkStatus(status, column->getName(), format_name);
        }

        arrow::Status status = builder->InsertMemoValues(*dict_values);
        checkStatus(status, column->getName(), format_name);

        /// AppendIndices in DictionaryBuilder works only with int64_t data, so we cannot use
        /// fillArrowArray here and should copy all indexes to int64_t container.
        auto indexes = extractIndexesImpl(column_lc->getIndexesPtr(), start, end);
        const uint8_t * arrow_null_bytemap_raw_ptr = nullptr;
        PaddedPODArray<uint8_t> arrow_null_bytemap;
        if (null_bytemap)
        {
            /// Invert values since Arrow interprets 1 as a non-null value, while CH as a null
            arrow_null_bytemap.reserve(end - start);
            for (size_t i = start; i < end; ++i)
                arrow_null_bytemap.emplace_back(!(*null_bytemap)[i]);

            arrow_null_bytemap_raw_ptr = arrow_null_bytemap.data();
        }

        status = builder->AppendIndices(indexes.data(), indexes.size(), arrow_null_bytemap_raw_ptr);
        checkStatus(status, column->getName(), format_name);
    }


    static void fillArrowArrayWithLowCardinalityColumnData(
        const String & column_name,
        ColumnPtr & column,
        const std::shared_ptr<const IDataType> & column_type,
        const PaddedPODArray<UInt8> * null_bytemap,
        arrow::ArrayBuilder * array_builder,
        String format_name,
        size_t start,
        size_t end,
        bool output_string_as_string,
        std::unordered_map<String, std::shared_ptr<arrow::Array>> & dictionary_values)
    {
        auto value_type = assert_cast<arrow::DictionaryType *>(array_builder->type().get())->value_type();

#define DISPATCH(ARROW_TYPE_ID, ARROW_TYPE) \
        if (arrow::Type::ARROW_TYPE_ID == value_type->id()) \
        { \
            fillArrowArrayWithLowCardinalityColumnDataImpl<ARROW_TYPE>(column_name, column, column_type, null_bytemap, array_builder, format_name, start, end, output_string_as_string, dictionary_values); \
            return; \
        }

        FOR_ARROW_TYPES(DISPATCH)
#undef DISPATCH

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot fill arrow array with {} data.", column_type->getName());
    }

    template <typename ColumnType, typename ArrowBuilder>
    static void fillArrowArrayWithStringColumnData(
        ColumnPtr write_column,
        const PaddedPODArray<UInt8> * null_bytemap,
        const String & format_name,
        arrow::ArrayBuilder* array_builder,
        size_t start,
        size_t end)
    {
        const auto & internal_column = assert_cast<const ColumnType &>(*write_column);
        ArrowBuilder & builder = assert_cast<ArrowBuilder &>(*array_builder);
        arrow::Status status;

        for (size_t string_i = start; string_i < end; ++string_i)
        {
            if (null_bytemap && (*null_bytemap)[string_i])
            {
                status = builder.AppendNull();
            }
            else
            {
                std::string_view string_ref = internal_column.getDataAt(string_i).toView();
                status = builder.Append(string_ref.data(), string_ref.size());
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
        arrow::UInt16Builder & builder = assert_cast<arrow::UInt16Builder &>(*array_builder);
        arrow::Status status;

        for (size_t value_i = start; value_i < end; ++value_i)
        {
            if (null_bytemap && (*null_bytemap)[value_i])
                status = builder.AppendNull();
            else
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
        arrow::UInt32Builder & builder = assert_cast<arrow::UInt32Builder &>(*array_builder);
        arrow::Status status;

        for (size_t value_i = start; value_i < end; ++value_i)
        {
            if (null_bytemap && (*null_bytemap)[value_i])
                status = builder.AppendNull();
            else
                status = builder.Append(internal_data[value_i]);

            checkStatus(status, write_column->getName(), format_name);
        }
    }

    static void fillArrowArrayWithDate32ColumnData(
        ColumnPtr write_column,
        const PaddedPODArray<UInt8> * null_bytemap,
        const String & format_name,
        arrow::ArrayBuilder* array_builder,
        size_t start,
        size_t end)
    {
        const PaddedPODArray<Int32> & internal_data = assert_cast<const ColumnVector<Int32> &>(*write_column).getData();
        arrow::Date32Builder & builder = assert_cast<arrow::Date32Builder &>(*array_builder);
        arrow::Status status;

        for (size_t value_i = start; value_i < end; ++value_i)
        {
            if (null_bytemap && (*null_bytemap)[value_i])
                status = builder.AppendNull();
            else
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
        size_t end,
        bool output_string_as_string,
        std::unordered_map<String, std::shared_ptr<arrow::Array>> & dictionary_values)
    {
        const String column_type_name = column_type->getFamilyName();

        if (column_type->isNullable())
        {
            const ColumnNullable * column_nullable = assert_cast<const ColumnNullable *>(column.get());
            ColumnPtr nested_column = column_nullable->getNestedColumnPtr();
            DataTypePtr nested_type = assert_cast<const DataTypeNullable *>(column_type.get())->getNestedType();
            const ColumnPtr & null_column = column_nullable->getNullMapColumnPtr();
            const PaddedPODArray<UInt8> & bytemap = assert_cast<const ColumnVector<UInt8> &>(*null_column).getData();
            fillArrowArray(column_name, nested_column, nested_type, &bytemap, array_builder, format_name, start, end, output_string_as_string, dictionary_values);
        }
        else if (isString(column_type))
        {
            if (output_string_as_string)
                fillArrowArrayWithStringColumnData<ColumnString, arrow::StringBuilder>(column, null_bytemap, format_name, array_builder, start, end);
            else
                fillArrowArrayWithStringColumnData<ColumnString, arrow::BinaryBuilder>(column, null_bytemap, format_name, array_builder, start, end);
        }
        else if (isFixedString(column_type))
        {
            if (output_string_as_string)
                fillArrowArrayWithStringColumnData<ColumnFixedString, arrow::StringBuilder>(column, null_bytemap, format_name, array_builder, start, end);
            else
                fillArrowArrayWithStringColumnData<ColumnFixedString, arrow::BinaryBuilder>(column, null_bytemap, format_name, array_builder, start, end);
        }
        else if (isDate(column_type))
        {
            fillArrowArrayWithDateColumnData(column, null_bytemap, format_name, array_builder, start, end);
        }
        else if (isDateTime(column_type))
        {
            fillArrowArrayWithDateTimeColumnData(column, null_bytemap, format_name, array_builder, start, end);
        }
        else if (isDate32(column_type))
        {
            fillArrowArrayWithDate32ColumnData(column, null_bytemap, format_name, array_builder, start, end);
        }
        else if (isArray(column_type))
        {
            fillArrowArrayWithArrayColumnData<arrow::ListBuilder>(column_name, column, column_type, null_bytemap, array_builder, format_name, start, end, output_string_as_string, dictionary_values);
        }
        else if (isTuple(column_type))
        {
            fillArrowArrayWithTupleColumnData(column_name, column, column_type, null_bytemap, array_builder, format_name, start, end, output_string_as_string, dictionary_values);
        }
        else if (column_type->getTypeId() == TypeIndex::LowCardinality)
        {
            fillArrowArrayWithLowCardinalityColumnData(column_name, column, column_type, null_bytemap, array_builder, format_name, start, end, output_string_as_string, dictionary_values);
        }
        else if (isMap(column_type))
        {
            ColumnPtr column_array = assert_cast<const ColumnMap *>(column.get())->getNestedColumnPtr();
            DataTypePtr array_type = assert_cast<const DataTypeMap *>(column_type.get())->getNestedType();
            fillArrowArrayWithArrayColumnData<arrow::MapBuilder>(column_name, column_array, array_type, null_bytemap, array_builder, format_name, start, end, output_string_as_string, dictionary_values);
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
                    fillArrowArrayWithDecimalColumnData<ToDataType, Int128, arrow::Decimal128, arrow::Decimal128Builder>(column, null_bytemap, array_builder, format_name, start, end);
                    return true;
                }
                if constexpr (std::is_same_v<ToDataType,DataTypeDecimal<Decimal256>>)
                {
                    fillArrowArrayWithDecimalColumnData<ToDataType, Int256, arrow::Decimal256, arrow::Decimal256Builder>(column, null_bytemap, array_builder, format_name, start, end);
                    return true;
                }

                return false;
            };

            if (!callOnIndexAndDataType<void>(column_type->getTypeId(), fill_decimal))
                throw Exception{ErrorCodes::LOGICAL_ERROR, "Cannot fill arrow array with decimal data with type {}", column_type_name};
        }
        else if (isDateTime64(column_type))
        {
            const auto * datetime64_type = assert_cast<const DataTypeDateTime64 *>(column_type.get());
            fillArrowArrayWithDateTime64ColumnData(datetime64_type, column, null_bytemap, format_name, array_builder, start, end);
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
                    fmt::format("Internal type '{}' of a column '{}' is not supported for conversion into {} data format.", column_type_name, column_name, format_name),
                    ErrorCodes::UNKNOWN_TYPE
                };
        }
    }

    template <typename DataType, typename FieldType, typename ArrowDecimalType, typename ArrowBuilder>
    static void fillArrowArrayWithDecimalColumnData(
        ColumnPtr write_column,
        const PaddedPODArray<UInt8> * null_bytemap,
        arrow::ArrayBuilder * array_builder,
        const String & format_name,
        size_t start,
        size_t end)
    {
        const auto & column = assert_cast<const typename DataType::ColumnType &>(*write_column);
        ArrowBuilder & builder = assert_cast<ArrowBuilder &>(*array_builder);
        arrow::Status status;

        for (size_t value_i = start; value_i < end; ++value_i)
        {
            if (null_bytemap && (*null_bytemap)[value_i])
                status = builder.AppendNull();
            else
            {
                FieldType element = FieldType(column.getElement(value_i).value);
                status = builder.Append(ArrowDecimalType(reinterpret_cast<const uint8_t *>(&element))); // TODO: try copy column
            }

            checkStatus(status, write_column->getName(), format_name);
        }
        checkStatus(status, write_column->getName(), format_name);
    }

    static std::shared_ptr<arrow::DataType> getArrowTypeForLowCardinalityIndexes(ColumnPtr indexes_column)
    {
        /// Arrow docs recommend preferring signed integers over unsigned integers for representing dictionary indices.
        /// https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoded-layout
        switch (indexes_column->getDataType())
        {
            case TypeIndex::UInt8:
                return arrow::int8();
            case TypeIndex::UInt16:
                return arrow::int16();
            case TypeIndex::UInt32:
                return arrow::int32();
            case TypeIndex::UInt64:
                return arrow::int64();
            default:
                throw Exception(fmt::format("Indexes column for getUniqueIndex must be ColumnUInt, got {}.", indexes_column->getName()),
                                      ErrorCodes::LOGICAL_ERROR);
        }
    }

    static arrow::TimeUnit::type getArrowTimeUnit(const DataTypeDateTime64 * type)
    {
        UInt32 scale = type->getScale();
        if (scale == 0)
            return arrow::TimeUnit::SECOND;
        if (scale > 0 && scale <= 3)
            return arrow::TimeUnit::MILLI;
        if (scale > 3 && scale <= 6)
            return arrow::TimeUnit::MICRO;
        return arrow::TimeUnit::NANO;
    }

    static std::shared_ptr<arrow::DataType> getArrowType(
        DataTypePtr column_type, ColumnPtr column, const std::string & column_name, const std::string & format_name, bool output_string_as_string, bool * out_is_column_nullable)
    {
        if (column_type->isNullable())
        {
            DataTypePtr nested_type = assert_cast<const DataTypeNullable *>(column_type.get())->getNestedType();
            ColumnPtr nested_column = assert_cast<const ColumnNullable *>(column.get())->getNestedColumnPtr();
            auto arrow_type = getArrowType(nested_type, nested_column, column_name, format_name, output_string_as_string, out_is_column_nullable);
            *out_is_column_nullable = true;
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
                    || std::is_same_v<ToDataType, DataTypeDecimal<Decimal128>>
                    || std::is_same_v<ToDataType, DataTypeDecimal<Decimal256>>)
                {
                    const auto & decimal_type = assert_cast<const ToDataType *>(column_type.get());
                    arrow_type = arrow::decimal(decimal_type->getPrecision(), decimal_type->getScale());
                    return true;
                }

                return false;
            };
            if (!callOnIndexAndDataType<void>(column_type->getTypeId(), create_arrow_type))
                throw Exception{ErrorCodes::LOGICAL_ERROR, "Cannot convert decimal type {} to arrow type", column_type->getFamilyName()};
            return arrow_type;
        }

        if (isArray(column_type))
        {
            auto nested_type = assert_cast<const DataTypeArray *>(column_type.get())->getNestedType();
            auto nested_column = assert_cast<const ColumnArray *>(column.get())->getDataPtr();
            auto nested_arrow_type = getArrowType(nested_type, nested_column, column_name, format_name, output_string_as_string, out_is_column_nullable);
            return arrow::list(nested_arrow_type);
        }

        if (isTuple(column_type))
        {
            const auto & tuple_type = assert_cast<const DataTypeTuple *>(column_type.get());
            const auto & nested_types = tuple_type->getElements();
            const auto & nested_names = tuple_type->getElementNames();
            const auto * tuple_column = assert_cast<const ColumnTuple *>(column.get());
            std::vector<std::shared_ptr<arrow::Field>> nested_fields;
            for (size_t i = 0; i != nested_types.size(); ++i)
            {
                auto nested_arrow_type = getArrowType(nested_types[i], tuple_column->getColumnPtr(i), nested_names[i], format_name, output_string_as_string, out_is_column_nullable);
                nested_fields.push_back(std::make_shared<arrow::Field>(nested_names[i], nested_arrow_type, *out_is_column_nullable));
            }
            return arrow::struct_(nested_fields);
        }

        if (column_type->lowCardinality())
        {
            auto nested_type = assert_cast<const DataTypeLowCardinality *>(column_type.get())->getDictionaryType();
            const auto * lc_column = assert_cast<const ColumnLowCardinality *>(column.get());
            const auto & nested_column = lc_column->getDictionaryPtr();
            const auto & indexes_column = lc_column->getIndexesPtr();
            return arrow::dictionary(
                getArrowTypeForLowCardinalityIndexes(indexes_column),
                getArrowType(nested_type, nested_column, column_name, format_name, output_string_as_string, out_is_column_nullable));
        }

        if (isMap(column_type))
        {
            const auto * map_type = assert_cast<const DataTypeMap *>(column_type.get());
            const auto & key_type = map_type->getKeyType();
            const auto & val_type = map_type->getValueType();

            const auto & columns =  assert_cast<const ColumnMap *>(column.get())->getNestedData().getColumns();
            return arrow::map(
                getArrowType(key_type, columns[0], column_name, format_name, output_string_as_string, out_is_column_nullable),
                getArrowType(val_type, columns[1], column_name, format_name, output_string_as_string, out_is_column_nullable));
        }

        if (isDateTime64(column_type))
        {
            const auto * datetime64_type = assert_cast<const DataTypeDateTime64 *>(column_type.get());
            return arrow::timestamp(getArrowTimeUnit(datetime64_type), datetime64_type->getTimeZone().getTimeZone());
        }

        if (isStringOrFixedString(column_type) && output_string_as_string)
            return arrow::utf8();

        const std::string type_name = column_type->getFamilyName();
        if (const auto * arrow_type_it = std::find_if(
                internal_type_to_arrow_type.begin(),
                internal_type_to_arrow_type.end(),
                [=](auto && elem) { return elem.first == type_name; });
            arrow_type_it != internal_type_to_arrow_type.end())
        {
            return arrow_type_it->second;
        }

        throw Exception(ErrorCodes::UNKNOWN_TYPE,
            "The type '{}' of a column '{}' is not supported for conversion into {} data format.",
            column_type->getName(), column_name, format_name);
    }

    CHColumnToArrowColumn::CHColumnToArrowColumn(const Block & header, const std::string & format_name_, bool low_cardinality_as_dictionary_, bool output_string_as_string_)
        : format_name(format_name_), low_cardinality_as_dictionary(low_cardinality_as_dictionary_), output_string_as_string(output_string_as_string_)
    {
        arrow_fields.reserve(header.columns());
        header_columns.reserve(header.columns());
        for (auto column : header.getColumnsWithTypeAndName())
        {
            if (!low_cardinality_as_dictionary)
            {
                column.type = recursiveRemoveLowCardinality(column.type);
                column.column = recursiveRemoveLowCardinality(column.column);
            }

            header_columns.emplace_back(std::move(column));
        }
    }

    void CHColumnToArrowColumn::chChunkToArrowTable(
        std::shared_ptr<arrow::Table> & res,
        const Chunk & chunk,
        size_t columns_num)
    {
        /// For arrow::Schema and arrow::Table creation
        std::vector<std::shared_ptr<arrow::Array>> arrow_arrays;
        arrow_arrays.reserve(columns_num);
        for (size_t column_i = 0; column_i < columns_num; ++column_i)
        {
            const ColumnWithTypeAndName & header_column = header_columns[column_i];
            auto column = chunk.getColumns()[column_i];

            if (!low_cardinality_as_dictionary)
                column = recursiveRemoveLowCardinality(column);

            if (!is_arrow_fields_initialized)
            {
                bool is_column_nullable = false;
                auto arrow_type = getArrowType(header_column.type, column, header_column.name, format_name, output_string_as_string, &is_column_nullable);
                arrow_fields.emplace_back(std::make_shared<arrow::Field>(header_column.name, arrow_type, is_column_nullable));
            }

            arrow::MemoryPool* pool = arrow::default_memory_pool();
            std::unique_ptr<arrow::ArrayBuilder> array_builder;
            arrow::Status status = MakeBuilder(pool, arrow_fields[column_i]->type(), &array_builder);
            checkStatus(status, column->getName(), format_name);

            fillArrowArray(
                header_column.name, column, header_column.type, nullptr, array_builder.get(), format_name, 0, column->size(), output_string_as_string, dictionary_values);

            std::shared_ptr<arrow::Array> arrow_array;
            status = array_builder->Finish(&arrow_array);
            checkStatus(status, column->getName(), format_name);
            arrow_arrays.emplace_back(std::move(arrow_array));
        }

        std::shared_ptr<arrow::Schema> arrow_schema = std::make_shared<arrow::Schema>(arrow_fields);

        res = arrow::Table::Make(arrow_schema, arrow_arrays);
        is_arrow_fields_initialized = true;
    }
}

#endif
