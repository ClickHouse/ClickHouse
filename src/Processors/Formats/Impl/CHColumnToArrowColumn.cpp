#include "CHColumnToArrowColumn.h"

#if USE_ARROW || USE_PARQUET

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Common/DateLUTImpl.h>
#include <Core/callOnTypeIndex.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>

#define FOR_INTERNAL_NUMERIC_TYPES(M) \
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
        M(BINARY, arrow::BinaryType) \
        M(STRING, arrow::StringType)

namespace DB
{
    namespace ErrorCodes
    {
        extern const int UNKNOWN_EXCEPTION;
        extern const int UNKNOWN_TYPE;
        extern const int LOGICAL_ERROR;
        extern const int DECIMAL_OVERFLOW;
        extern const int ILLEGAL_COLUMN;
    }

    static const std::initializer_list<std::pair<String, std::shared_ptr<arrow::DataType>>> internal_type_to_arrow_type =
    {
        {"UInt8", arrow::uint8()},
        {"Int8", arrow::int8()},
        {"Enum8", arrow::int8()},
        {"UInt16", arrow::uint16()},
        {"Int16", arrow::int16()},
        {"Enum16", arrow::int16()},
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

        {"Int128", arrow::fixed_size_binary(sizeof(Int128))},
        {"UInt128", arrow::fixed_size_binary(sizeof(UInt128))},
        {"Int256", arrow::fixed_size_binary(sizeof(Int256))},
        {"UInt256", arrow::fixed_size_binary(sizeof(UInt256))},
    };


    static void checkStatus(const arrow::Status & status, const String & column_name, const String & format_name)
    {
        if (!status.ok())
            throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Error with a {} column \"{}\": {}.", format_name, column_name, status.ToString());
    }

    /// Invert values since Arrow interprets 1 as a non-null value, while CH as a null
    static PaddedPODArray<UInt8> revertNullByteMap(const PaddedPODArray<UInt8> * null_bytemap, size_t start, size_t end)
    {
        PaddedPODArray<UInt8> res;
        if (!null_bytemap)
            return res;

        res.reserve(end - start);
        for (size_t i = start; i < end; ++i)
            res.emplace_back(!(*null_bytemap)[i]);
        return res;
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

        PaddedPODArray<UInt8> arrow_null_bytemap = revertNullByteMap(null_bytemap, start, end);
        const UInt8 * arrow_null_bytemap_raw_ptr = arrow_null_bytemap.empty() ? nullptr : arrow_null_bytemap.data();

        if constexpr (std::is_same_v<NumericType, UInt8>)
            status = builder.AppendValues(
                reinterpret_cast<const uint8_t *>(internal_data.data() + start),
                end - start,
                reinterpret_cast<const uint8_t *>(arrow_null_bytemap_raw_ptr));
        else if constexpr (std::is_same_v<NumericType, Int8>)
            status = builder.AppendValues(
                reinterpret_cast<const int8_t *>(internal_data.data() + start),
                end - start,
                reinterpret_cast<const uint8_t *>(arrow_null_bytemap_raw_ptr));
        else
            status = builder.AppendValues(internal_data.data() + start, end - start, reinterpret_cast<const uint8_t *>(arrow_null_bytemap_raw_ptr));
        checkStatus(status, write_column->getName(), format_name);
    }

    static void fillArrowArrayWithBoolColumnData(
        ColumnPtr write_column,
        const PaddedPODArray<UInt8> * null_bytemap,
        const String & format_name,
        arrow::ArrayBuilder* array_builder,
        size_t start,
        size_t end)
    {
        const PaddedPODArray<UInt8> & internal_data = assert_cast<const ColumnVector<UInt8> &>(*write_column).getData();
        arrow::BooleanBuilder & builder = assert_cast<arrow::BooleanBuilder &>(*array_builder);
        arrow::Status status;

        PaddedPODArray<UInt8> arrow_null_bytemap = revertNullByteMap(null_bytemap, start, end);
        const UInt8 * arrow_null_bytemap_raw_ptr = arrow_null_bytemap.empty() ? nullptr : arrow_null_bytemap.data();

        status = builder.AppendValues(reinterpret_cast<const uint8_t *>(internal_data.data() + start), end - start, reinterpret_cast<const uint8_t *>(arrow_null_bytemap_raw_ptr));
        checkStatus(status, write_column->getName(), format_name);
    }

    static void fillArrowArrayWithDateTime64ColumnData(
        const DataTypePtr & type,
        ColumnPtr write_column,
        const PaddedPODArray<UInt8> * null_bytemap,
        const String & format_name,
        arrow::ArrayBuilder* array_builder,
        size_t start,
        size_t end)
    {
        const auto * datetime64_type = assert_cast<const DataTypeDateTime64 *>(type.get());
        const auto & column = assert_cast<const ColumnDecimal<DateTime64> &>(*write_column);
        arrow::TimestampBuilder & builder = assert_cast<arrow::TimestampBuilder &>(*array_builder);
        arrow::Status status;

        auto scale = datetime64_type->getScale();
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
                auto value = static_cast<Int64>(column[value_i].safeGet<DecimalField<DateTime64>>().getValue());
                if (need_rescale)
                {
                    if (common::mulOverflow(value, rescale_multiplier, value))
                        throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Decimal math overflow");
                }
                status = builder.Append(value);
            }
            checkStatus(status, write_column->getName(), format_name);
        }
    }

    static void fillArrowArray(
        const String & column_name,
        ColumnPtr & column,
        const DataTypePtr & column_type,
        const PaddedPODArray<UInt8> * null_bytemap,
        arrow::ArrayBuilder * array_builder,
        String format_name,
        size_t start,
        size_t end,
        const CHColumnToArrowColumn::Settings & settings,
        std::unordered_map<String, MutableColumnPtr> & dictionary_values);

    template <typename Builder>
    static void fillArrowArrayWithArrayColumnData(
        const String & column_name,
        ColumnPtr & column,
        const DataTypePtr & column_type,
        const PaddedPODArray<UInt8> *,
        arrow::ArrayBuilder * array_builder,
        String format_name,
        size_t start,
        size_t end,
        const CHColumnToArrowColumn::Settings & settings,
        std::unordered_map<String, MutableColumnPtr> & dictionary_values)
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

            /// Pass null null_map, because fillArrowArray will decide whether nested_type is nullable, if nullable, it will create a new null_map from nested_column
            /// Note that it is only needed by gluten(https://github.com/oap-project/gluten), because array type in gluten is by default nullable.
            /// And it does not influence the original ClickHouse logic, because null_map passed to fillArrowArrayWithArrayColumnData is always nullptr for ClickHouse doesn't allow nullable complex types including array type.
            fillArrowArray(column_name, nested_column, nested_type, nullptr, value_builder, format_name, offsets[array_idx - 1], offsets[array_idx], settings, dictionary_values);
        }
    }

    static void fillArrowArrayWithTupleColumnData(
        const String & column_name,
        ColumnPtr & column,
        const DataTypePtr & column_type,
        const PaddedPODArray<UInt8> * null_bytemap,
        arrow::ArrayBuilder * array_builder,
        String format_name,
        size_t start,
        size_t end,
        const CHColumnToArrowColumn::Settings & settings,
        std::unordered_map<String, MutableColumnPtr> & dictionary_values)
    {
        const auto * column_tuple = assert_cast<const ColumnTuple *>(column.get());
        const auto * type_tuple = assert_cast<const DataTypeTuple *>(column_type.get());
        const auto & nested_types = type_tuple->getElements();
        const auto & nested_names = type_tuple->getElementNames();

        arrow::StructBuilder & builder = assert_cast<arrow::StructBuilder &>(*array_builder);

        for (size_t i = 0; i != column_tuple->tupleSize(); ++i)
        {
            ColumnPtr nested_column = column_tuple->getColumnPtr(i);
            fillArrowArray(
                column_name + "." + nested_names[i],
                nested_column, nested_types[i], null_bytemap,
                builder.field_builder(static_cast<int>(i)),
                format_name,
                start, end,
                settings,
                dictionary_values);
        }

        for (size_t i = start; i != end; ++i)
        {
            auto status = builder.Append();
            checkStatus(status, column->getName(), format_name);
        }
    }

    template<typename T>
    static PaddedPODArray<Int64> extractIndexes(ColumnPtr column, size_t start, size_t end, bool shift)
    {
        const PaddedPODArray<T> & data = assert_cast<const ColumnVector<T> *>(column.get())->getData();
        PaddedPODArray<Int64> result;
        result.reserve(end - start);
        if (shift)
            std::transform(data.begin() + start, data.begin() + end, std::back_inserter(result), [](T value) { return Int64(value) - 1; });
        else
            std::transform(data.begin() + start, data.begin() + end, std::back_inserter(result), [](T value) { return Int64(value); });
        return result;
    }

    static PaddedPODArray<Int64> extractIndexes(ColumnPtr column, size_t start, size_t end, bool shift)
    {
        switch (column->getDataType())
        {
            case TypeIndex::UInt8:
                return extractIndexes<UInt8>(column, start, end, shift);
            case TypeIndex::UInt16:
                return extractIndexes<UInt16>(column, start, end, shift);
            case TypeIndex::UInt32:
                return extractIndexes<UInt32>(column, start, end, shift);
            case TypeIndex::UInt64:
                return extractIndexes<UInt64>(column, start, end, shift);
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Indexes column must be ColumnUInt, got {}.", column->getName());
        }
    }

    template <typename IndexesType, typename MappingType>
    static PaddedPODArray<Int64> extractIndexesWithRemapping(ColumnPtr indexes, ColumnPtr mapping, size_t start, size_t end, bool shift)
    {
        const PaddedPODArray<IndexesType> & indexes_data = assert_cast<const ColumnVector<IndexesType> *>(indexes.get())->getData();
        const PaddedPODArray<MappingType> & mapping_data = assert_cast<const ColumnVector<MappingType> *>(mapping.get())->getData();
        PaddedPODArray<Int64> result;
        result.reserve(end - start);
        std::transform(indexes_data.begin() + start, indexes_data.begin() + end, std::back_inserter(result), [&](IndexesType value) { return mapping_data[Int64(value)] - shift; });
        return result;
    }

    template <typename IndexesType>
    static PaddedPODArray<Int64> extractIndexesWithRemapping(ColumnPtr indexes, ColumnPtr mapping, size_t start, size_t end, bool shift)
    {
        switch (mapping->getDataType())
        {
            case TypeIndex::UInt8:
                return extractIndexesWithRemapping<IndexesType, UInt8>(indexes, mapping, start, end, shift);
            case TypeIndex::UInt16:
                return extractIndexesWithRemapping<IndexesType, UInt16>(indexes, mapping, start, end, shift);
            case TypeIndex::UInt32:
                return extractIndexesWithRemapping<IndexesType, UInt32>(indexes, mapping, start, end, shift);
            case TypeIndex::UInt64:
                return extractIndexesWithRemapping<IndexesType, UInt64>(indexes, mapping, start, end, shift);
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Indexes column must be ColumnUInt, got {}.", indexes->getName());
        }
    }

    static PaddedPODArray<Int64> extractIndexesWithRemapping(ColumnPtr indexes, ColumnPtr mapping, size_t start, size_t end, bool shift)
    {
        switch (indexes->getDataType())
        {
            case TypeIndex::UInt8:
                return extractIndexesWithRemapping<UInt8>(indexes, mapping, start, end, shift);
            case TypeIndex::UInt16:
                return extractIndexesWithRemapping<UInt16>(indexes, mapping, start, end, shift);
            case TypeIndex::UInt32:
                return extractIndexesWithRemapping<UInt32>(indexes, mapping, start, end, shift);
            case TypeIndex::UInt64:
                return extractIndexesWithRemapping<UInt64>(indexes, mapping, start, end, shift);
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Indexes column must be ColumnUInt, got {}.", indexes->getName());
        }
    }

    static void checkIfIndexesTypeIsExceeded(const std::shared_ptr<arrow::DataType> & arrow_type, size_t dict_size)
    {
        const auto & dict_indexes_arrow_type = assert_cast<const arrow::DictionaryType *>(arrow_type.get())->index_type();
        /// We can use Int32/UInt32/Int64/UInt64 type for indexes.
        const auto * indexes_int32_type = typeid_cast<const arrow::Int32Type *>(dict_indexes_arrow_type.get());
        const auto * indexes_uint32_type = typeid_cast<const arrow::UInt32Type *>(dict_indexes_arrow_type.get());
        const auto * indexes_int64_type = typeid_cast<const arrow::UInt32Type *>(dict_indexes_arrow_type.get());
        if ((indexes_int32_type && dict_size > INT32_MAX) || (indexes_uint32_type && dict_size > UINT32_MAX) || (indexes_int64_type && dict_size > INT64_MAX))
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Cannot convert ClickHouse LowCardinality column to Arrow Dictionary column:"
                " resulting dictionary size exceeds the max value of index type {}", dict_indexes_arrow_type->name());
    }

    template<typename ValueType>
    static void fillArrowArrayWithLowCardinalityColumnDataImpl(
        const String & column_name,
        ColumnPtr & column,
        const DataTypePtr & column_type,
        const PaddedPODArray<UInt8> *,
        arrow::ArrayBuilder * array_builder,
        String format_name,
        size_t start,
        size_t end,
        const CHColumnToArrowColumn::Settings & settings,
        std::unordered_map<String, MutableColumnPtr> & dictionary_values)
    {
        const auto * column_lc = assert_cast<const ColumnLowCardinality *>(column.get());
        arrow::DictionaryBuilder<ValueType> * builder = assert_cast<arrow::DictionaryBuilder<ValueType> *>(array_builder);
        auto & dict_values = dictionary_values[column_name];
        bool is_nullable = column_type->isLowCardinalityNullable();

        ColumnPtr mapping;
        if (!dict_values)
        {
            /// On first time just remember the first dictionary
            dict_values = IColumn::mutate(column_lc->getDictionaryPtr());
        }
        else
        {
            /// In ClickHouse blocks with same header can contain LowCardinality columns with
            /// different dictionaries.
            /// Arrow supports only single dictionary for all batches, but it allows to extend
            /// dictionary if previous dictionary is a prefix of a new one.
            /// So, if new LowCardinality column has different dictionary
            /// we extend previous one by using IColumnUnique::uniqueInsertRangeFrom
            /// and then remap indexes so they match with the new extended dictionary.
            const auto & new_dict = column_lc->getDictionary();
            auto & dict = dynamic_cast<IColumnUnique &>(*dict_values);
            if (dict.getHash() != new_dict.getHash())
            {
                const auto & new_values = new_dict.getNestedColumn();
                mapping = dict.uniqueInsertRangeFrom(*new_values, 0, new_values->size());
                checkIfIndexesTypeIsExceeded(array_builder->type(), dict.size());
            }
        }

        /// Convert dictionary values to arrow array.
        auto value_type = assert_cast<arrow::DictionaryType *>(builder->type().get())->value_type();
        std::unique_ptr<arrow::ArrayBuilder> values_builder;
        arrow::MemoryPool* pool = ArrowMemoryPool::instance();
        arrow::Status status = MakeBuilder(pool, value_type, &values_builder);
        checkStatus(status, column->getName(), format_name);

        auto dict_column = dynamic_cast<IColumnUnique &>(*dict_values).getNestedNotNullableColumn();
        const auto & dict_type = removeNullable(assert_cast<const DataTypeLowCardinality *>(column_type.get())->getDictionaryType());
        fillArrowArray(column_name, dict_column, dict_type, nullptr, values_builder.get(), format_name, is_nullable, dict_column->size(), settings, dictionary_values);
        std::shared_ptr<arrow::Array> arrow_dict_array;
        status = values_builder->Finish(&arrow_dict_array);
        checkStatus(status, column->getName(), format_name);

        status = builder->InsertMemoValues(*arrow_dict_array);
        checkStatus(status, column->getName(), format_name);

        /// AppendIndices in DictionaryBuilder works only with int64_t data, so we cannot use
        /// fillArrowArray here and should copy all indexes to int64_t container.
        PaddedPODArray<Int64> indexes;
        if (mapping)
            indexes = extractIndexesWithRemapping(column_lc->getIndexesPtr(), mapping, start, end, is_nullable);
        else
            indexes = extractIndexes(column_lc->getIndexesPtr(), start, end, is_nullable);
        const uint8_t * arrow_null_bytemap_raw_ptr = nullptr;
        PaddedPODArray<uint8_t> arrow_null_bytemap;
        if (column_type->isLowCardinalityNullable())
        {
            arrow_null_bytemap.reserve(end - start);
            for (size_t i = start; i < end; ++i)
                arrow_null_bytemap.emplace_back(!column_lc->isNullAt(i));

            arrow_null_bytemap_raw_ptr = arrow_null_bytemap.data();
        }

        status = builder->AppendIndices(indexes.data(), indexes.size(), arrow_null_bytemap_raw_ptr);
        checkStatus(status, column->getName(), format_name);
    }


    static void fillArrowArrayWithLowCardinalityColumnData(
        const String & column_name,
        ColumnPtr & column,
        const DataTypePtr & column_type,
        const PaddedPODArray<UInt8> * null_bytemap,
        arrow::ArrayBuilder * array_builder,
        String format_name,
        size_t start,
        size_t end,
        const CHColumnToArrowColumn::Settings & settings,
        std::unordered_map<String, MutableColumnPtr> & dictionary_values)
    {
        auto value_type = assert_cast<arrow::DictionaryType *>(array_builder->type().get())->value_type();

#define DISPATCH(ARROW_TYPE_ID, ARROW_TYPE) \
        if (arrow::Type::ARROW_TYPE_ID == value_type->id()) \
        { \
            fillArrowArrayWithLowCardinalityColumnDataImpl<ARROW_TYPE>(column_name, column, column_type, null_bytemap, array_builder, format_name, start, end, settings, dictionary_values); \
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
                status = builder.Append(string_ref.data(), static_cast<int>(string_ref.size()));
            }
            checkStatus(status, write_column->getName(), format_name);
        }
    }

    static void fillArrowArrayWithFixedStringColumnData(
        ColumnPtr write_column,
        const PaddedPODArray<UInt8> * null_bytemap,
        const String & format_name,
        arrow::ArrayBuilder* array_builder,
        size_t start,
        size_t end)
    {
        const auto & internal_column = assert_cast<const ColumnFixedString &>(*write_column);
        const auto & internal_data = internal_column.getChars();
        size_t fixed_length = internal_column.getN();
        arrow::FixedSizeBinaryBuilder & builder = assert_cast<arrow::FixedSizeBinaryBuilder &>(*array_builder);

        PaddedPODArray<UInt8> arrow_null_bytemap = revertNullByteMap(null_bytemap, start, end);
        const UInt8 * arrow_null_bytemap_raw_ptr = arrow_null_bytemap.empty() ? nullptr : arrow_null_bytemap.data();

        const uint8_t * data_start = reinterpret_cast<const uint8_t *>(internal_data.data() + start * fixed_length);
        arrow::Status status = builder.AppendValues(data_start, end - start, reinterpret_cast<const uint8_t *>(arrow_null_bytemap_raw_ptr));
        checkStatus(status, write_column->getName(), format_name);
    }

    static void fillArrowArrayWithIPv6ColumnData(
        ColumnPtr write_column,
        const PaddedPODArray<UInt8> * null_bytemap,
        const String & format_name,
        arrow::ArrayBuilder* array_builder,
        size_t start,
        size_t end)
    {
        const auto & internal_column = assert_cast<const ColumnIPv6 &>(*write_column);
        const auto & internal_data = internal_column.getData();
        size_t fixed_length = sizeof(IPv6);
        arrow::FixedSizeBinaryBuilder & builder = assert_cast<arrow::FixedSizeBinaryBuilder &>(*array_builder);

        PaddedPODArray<UInt8> arrow_null_bytemap = revertNullByteMap(null_bytemap, start, end);
        const UInt8 * arrow_null_bytemap_raw_ptr = arrow_null_bytemap.empty() ? nullptr : arrow_null_bytemap.data();

        const uint8_t * data_start = reinterpret_cast<const uint8_t *>(internal_data.data()) + start * fixed_length;
        arrow::Status status = builder.AppendValues(data_start, end - start, reinterpret_cast<const uint8_t *>(arrow_null_bytemap_raw_ptr));
        checkStatus(status, write_column->getName(), format_name);
    }

    static void fillArrowArrayWithIPv4ColumnData(
        ColumnPtr write_column,
        const PaddedPODArray<UInt8> * null_bytemap,
        const String & format_name,
        arrow::ArrayBuilder* array_builder,
        size_t start,
        size_t end)
    {
        const auto & internal_data = assert_cast<const ColumnIPv4 &>(*write_column).getData();
        auto & builder = assert_cast<arrow::UInt32Builder &>(*array_builder);

        PaddedPODArray<UInt8> arrow_null_bytemap = revertNullByteMap(null_bytemap, start, end);
        const UInt8 * arrow_null_bytemap_raw_ptr = arrow_null_bytemap.empty() ? nullptr : arrow_null_bytemap.data();
        arrow::Status status = builder.AppendValues(&(internal_data.data() + start)->toUnderType(), end - start, reinterpret_cast<const uint8_t *>(arrow_null_bytemap_raw_ptr));
        checkStatus(status, write_column->getName(), format_name);
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

    template <typename ColumnType>
    static void fillArrowArrayWithBigIntegerColumnData(
        ColumnPtr write_column,
        const PaddedPODArray<UInt8> * null_bytemap,
        const String & format_name,
        arrow::ArrayBuilder* array_builder,
        size_t start,
        size_t end)
    {
        const auto & internal_column = assert_cast<const ColumnType &>(*write_column);
        const auto & internal_data = internal_column.getData();
        size_t fixed_length = sizeof(typename ColumnType::ValueType);
        arrow::FixedSizeBinaryBuilder & builder = assert_cast<arrow::FixedSizeBinaryBuilder &>(*array_builder);

        PaddedPODArray<UInt8> arrow_null_bytemap = revertNullByteMap(null_bytemap, start, end);
        const UInt8 * arrow_null_bytemap_raw_ptr = arrow_null_bytemap.empty() ? nullptr : arrow_null_bytemap.data();

        const uint8_t * data_start = reinterpret_cast<const uint8_t *>(internal_data.data()) + start * fixed_length;
        arrow::Status status = builder.AppendValues(data_start, end - start, reinterpret_cast<const uint8_t *>(arrow_null_bytemap_raw_ptr));
        checkStatus(status, write_column->getName(), format_name);
    }

    static void fillArrowArray(
        const String & column_name,
        ColumnPtr & column,
        const DataTypePtr & column_type,
        const PaddedPODArray<UInt8> * null_bytemap,
        arrow::ArrayBuilder * array_builder,
        String format_name,
        size_t start,
        size_t end,
        const CHColumnToArrowColumn::Settings & settings,
        std::unordered_map<String, MutableColumnPtr> & dictionary_values)
    {
        switch (column_type->getTypeId())
        {
            case TypeIndex::Nullable:
            {
                const ColumnNullable * column_nullable = assert_cast<const ColumnNullable *>(column.get());
                ColumnPtr nested_column = column_nullable->getNestedColumnPtr();
                DataTypePtr nested_type = assert_cast<const DataTypeNullable *>(column_type.get())->getNestedType();
                const ColumnPtr & null_column = column_nullable->getNullMapColumnPtr();
                const PaddedPODArray<UInt8> & bytemap = assert_cast<const ColumnVector<UInt8> &>(*null_column).getData();
                fillArrowArray(column_name, nested_column, nested_type, &bytemap, array_builder, format_name, start, end, settings, dictionary_values);
                break;
            }
            case TypeIndex::String:
            {
                if (settings.output_string_as_string)
                    fillArrowArrayWithStringColumnData<ColumnString, arrow::StringBuilder>(column, null_bytemap, format_name, array_builder, start, end);
                else
                    fillArrowArrayWithStringColumnData<ColumnString, arrow::BinaryBuilder>(column, null_bytemap, format_name, array_builder, start, end);
                break;
            }
            case TypeIndex::FixedString:
            {
                if (settings.output_fixed_string_as_fixed_byte_array)
                    fillArrowArrayWithFixedStringColumnData(column, null_bytemap, format_name, array_builder, start, end);
                else if (settings.output_string_as_string)
                    fillArrowArrayWithStringColumnData<ColumnFixedString, arrow::StringBuilder>(column, null_bytemap, format_name, array_builder, start, end);
                else
                    fillArrowArrayWithStringColumnData<ColumnFixedString, arrow::BinaryBuilder>(column, null_bytemap, format_name, array_builder, start, end);
                break;
            }
            case TypeIndex::IPv6:
                fillArrowArrayWithIPv6ColumnData(column, null_bytemap, format_name, array_builder, start, end);
                break;
            case TypeIndex::IPv4:
                fillArrowArrayWithIPv4ColumnData(column, null_bytemap, format_name, array_builder, start, end);
                break;
            case TypeIndex::Date:
                fillArrowArrayWithDateColumnData(column, null_bytemap, format_name, array_builder, start, end);
                break;
            case TypeIndex::DateTime:
                fillArrowArrayWithDateTimeColumnData(column, null_bytemap, format_name, array_builder, start, end);
                break;
            case TypeIndex::Date32:
                fillArrowArrayWithDate32ColumnData(column, null_bytemap, format_name, array_builder, start, end);
                break;
            case TypeIndex::Array:
                fillArrowArrayWithArrayColumnData<arrow::ListBuilder>(column_name, column, column_type, null_bytemap, array_builder, format_name, start, end, settings, dictionary_values);
                break;
            case TypeIndex::Tuple:
                fillArrowArrayWithTupleColumnData(column_name, column, column_type, null_bytemap, array_builder, format_name, start, end, settings, dictionary_values);
                break;
            case TypeIndex::LowCardinality:
                fillArrowArrayWithLowCardinalityColumnData(column_name, column, column_type, null_bytemap, array_builder, format_name, start, end, settings, dictionary_values);
                break;
            case TypeIndex::Map:
            {
                ColumnPtr column_array = assert_cast<const ColumnMap *>(column.get())->getNestedColumnPtr();
                DataTypePtr array_type = assert_cast<const DataTypeMap *>(column_type.get())->getNestedType();
                fillArrowArrayWithArrayColumnData<arrow::MapBuilder>(column_name, column_array, array_type, null_bytemap, array_builder, format_name, start, end, settings, dictionary_values);
                break;
            }
            case TypeIndex::Decimal32:
                fillArrowArrayWithDecimalColumnData<DataTypeDecimal32, Int128, arrow::Decimal128, arrow::Decimal128Builder>(column, null_bytemap, array_builder, format_name, start, end);
                break;
            case TypeIndex::Decimal64:
                fillArrowArrayWithDecimalColumnData<DataTypeDecimal64, Int128, arrow::Decimal128, arrow::Decimal128Builder>(column, null_bytemap, array_builder, format_name, start, end);
                break;
            case TypeIndex::Decimal128:
                fillArrowArrayWithDecimalColumnData<DataTypeDecimal128, Int128, arrow::Decimal128, arrow::Decimal128Builder>(column, null_bytemap, array_builder, format_name, start, end);
                break;
            case TypeIndex::Decimal256:
                fillArrowArrayWithDecimalColumnData<DataTypeDecimal256, Int256, arrow::Decimal256, arrow::Decimal256Builder>(column, null_bytemap, array_builder, format_name, start, end);
                break;
            case TypeIndex::DateTime64:
                fillArrowArrayWithDateTime64ColumnData(column_type, column, null_bytemap, format_name, array_builder, start, end);
                break;
            case TypeIndex::UInt8:
            {
                if (isBool(column_type))
                    fillArrowArrayWithBoolColumnData(column, null_bytemap, format_name, array_builder, start, end);
                else
                    fillArrowArrayWithNumericColumnData<UInt8, arrow::UInt8Builder>(column, null_bytemap, format_name, array_builder, start, end);
                break;
            }
            case TypeIndex::Enum8:
                fillArrowArrayWithNumericColumnData<Int8, arrow::Int8Builder>(column, null_bytemap, format_name, array_builder, start, end);
                break;
            case TypeIndex::Enum16:
                fillArrowArrayWithNumericColumnData<Int16, arrow::Int16Builder>(column, null_bytemap, format_name, array_builder, start, end);
                break;
            case TypeIndex::Int128:
                fillArrowArrayWithBigIntegerColumnData<ColumnInt128>(column, null_bytemap, format_name, array_builder, start, end);
                break;
            case TypeIndex::UInt128:
                fillArrowArrayWithBigIntegerColumnData<ColumnUInt128>(column, null_bytemap, format_name, array_builder, start, end);
                break;
            case TypeIndex::Int256:
                fillArrowArrayWithBigIntegerColumnData<ColumnInt256>(column, null_bytemap, format_name, array_builder, start, end);
                break;
            case TypeIndex::UInt256:
                fillArrowArrayWithBigIntegerColumnData<ColumnUInt256>(column, null_bytemap, format_name, array_builder, start, end);
                break;
#define DISPATCH(CPP_NUMERIC_TYPE, ARROW_BUILDER_TYPE) \
            case TypeIndex::CPP_NUMERIC_TYPE: \
                fillArrowArrayWithNumericColumnData<CPP_NUMERIC_TYPE, ARROW_BUILDER_TYPE>(column, null_bytemap, format_name, array_builder, start, end); \
                break;
                FOR_INTERNAL_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
            default:
                throw Exception(ErrorCodes::UNKNOWN_TYPE, "Internal type '{}' of a column '{}' is not supported for conversion into {} data format.", column_type->getFamilyName(), column_name, format_name);
        }
    }

    static std::shared_ptr<arrow::DataType> getArrowTypeForLowCardinalityIndexes(ColumnPtr indexes_column, const CHColumnToArrowColumn::Settings & settings)
    {
        switch (indexes_column->getDataType())
        {
            /// In ClickHouse blocks with same header can contain LowCardinality columns with
            /// different dictionaries.
            /// Arrow supports only single dictionary for all batches, but it allows to extend
            /// dictionary if previous dictionary is a prefix of a new one.
            /// But it can happen that LowCardinality columns contains UInt8 indexes columns
            /// but resulting extended arrow dictionary will exceed UInt8 and we will need UInt16,
            /// but it's not possible to change the type of Arrow dictionary indexes as it's
            /// written in Arrow schema.
            /// We can just always use type UInt64, but it can be inefficient.
            /// In most cases UInt32 should be enough (with more unique values using dictionary is quite meaningless).
            /// So we use minimum UInt32 type here (actually maybe even UInt16 will be enough).
            /// In case if it's exceeded during dictionary extension, an exception will be thrown.
            /// There are also 2 settings that can change the indexes type that will be used for Dictionary indexes:
            /// use_64_bit_indexes_for_dictionary and use_signed_indexes_for_dictionary.
            case TypeIndex::UInt8:
            case TypeIndex::UInt16:
            case TypeIndex::UInt32:
                if (!settings.use_64_bit_indexes_for_dictionary)
                    return settings.use_signed_indexes_for_dictionary ? arrow::int32() : arrow::uint32();
                [[fallthrough]];
            case TypeIndex::UInt64:
                return settings.use_signed_indexes_for_dictionary ? arrow::int64() : arrow::uint64();
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Indexes column for getUniqueIndex must be ColumnUInt, got {}.", indexes_column->getName());
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
        DataTypePtr column_type, ColumnPtr column, const std::string & column_name, const std::string & format_name, const CHColumnToArrowColumn::Settings & settings, bool * out_is_column_nullable)
    {
        if (column_type->isNullable())
        {
            DataTypePtr nested_type = assert_cast<const DataTypeNullable *>(column_type.get())->getNestedType();
            ColumnPtr nested_column = assert_cast<const ColumnNullable *>(column.get())->getNestedColumnPtr();
            auto arrow_type = getArrowType(nested_type, nested_column, column_name, format_name, settings, out_is_column_nullable);
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
            auto nested_arrow_type = getArrowType(nested_type, nested_column, column_name, format_name, settings, out_is_column_nullable);
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
                auto nested_arrow_type = getArrowType(nested_types[i], tuple_column->getColumnPtr(i), nested_names[i], format_name, settings, out_is_column_nullable);
                nested_fields.push_back(std::make_shared<arrow::Field>(nested_names[i], nested_arrow_type, *out_is_column_nullable));
            }
            return arrow::struct_(nested_fields);
        }

        if (column_type->lowCardinality())
        {
            auto nested_type = assert_cast<const DataTypeLowCardinality *>(column_type.get())->getDictionaryType();
            const auto * lc_column = assert_cast<const ColumnLowCardinality *>(column.get());
            const auto & nested_column = lc_column->getDictionary().getNestedColumn();
            const auto & indexes_column = lc_column->getIndexesPtr();
            return arrow::dictionary(
                getArrowTypeForLowCardinalityIndexes(indexes_column, settings),
                getArrowType(nested_type, nested_column, column_name, format_name, settings, out_is_column_nullable));
        }

        if (isMap(column_type))
        {
            const auto * map_type = assert_cast<const DataTypeMap *>(column_type.get());
            const auto & key_type = map_type->getKeyType();
            const auto & val_type = map_type->getValueType();

            const auto & columns =  assert_cast<const ColumnMap *>(column.get())->getNestedData().getColumns();
            return arrow::map(
                getArrowType(key_type, columns[0], column_name, format_name, settings, out_is_column_nullable),
                getArrowType(val_type, columns[1], column_name, format_name, settings, out_is_column_nullable));
        }

        if (isDateTime64(column_type))
        {
            const auto * datetime64_type = assert_cast<const DataTypeDateTime64 *>(column_type.get());
            return arrow::timestamp(getArrowTimeUnit(datetime64_type), datetime64_type->getTimeZone().getTimeZone());
        }

        if (isFixedString(column_type) && settings.output_fixed_string_as_fixed_byte_array)
        {
            size_t fixed_length = assert_cast<const DataTypeFixedString *>(column_type.get())->getN();
            return arrow::fixed_size_binary(static_cast<int32_t>(fixed_length));
        }

        if (isStringOrFixedString(column_type) && settings.output_string_as_string)
            return arrow::utf8();

        if (isBool(column_type))
            return arrow::boolean();

        if (isIPv6(column_type))
            return arrow::fixed_size_binary(sizeof(IPv6));

        if (isIPv4(column_type))
            return arrow::uint32();

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

    CHColumnToArrowColumn::CHColumnToArrowColumn(const Block & header, const std::string & format_name_, const Settings & settings_)
        : format_name(format_name_), settings(settings_)
    {
        arrow_fields.reserve(header.columns());
        header_columns.reserve(header.columns());
        for (auto column : header.getColumnsWithTypeAndName())
        {
            if (!settings.low_cardinality_as_dictionary)
            {
                column.type = recursiveRemoveLowCardinality(column.type);
                column.column = recursiveRemoveLowCardinality(column.column);
            }

            header_columns.emplace_back(std::move(column));
        }
    }

    void CHColumnToArrowColumn::chChunkToArrowTable(
        std::shared_ptr<arrow::Table> & res,
        const std::vector<Chunk> & chunks,
        size_t columns_num)
    {
        std::shared_ptr<arrow::Schema> arrow_schema;
        std::vector<arrow::ArrayVector> table_data(columns_num);

        for (const auto & chunk : chunks)
        {
            /// For arrow::Schema and arrow::Table creation
            for (size_t column_i = 0; column_i < columns_num; ++column_i)
            {
                const ColumnWithTypeAndName & header_column = header_columns[column_i];
                auto column = chunk.getColumns()[column_i];

                if (!settings.low_cardinality_as_dictionary)
                    column = recursiveRemoveLowCardinality(column);

                if (!is_arrow_fields_initialized)
                {
                    bool is_column_nullable = false;
                    auto arrow_type = getArrowType(
                        header_column.type,
                        column,
                        header_column.name,
                        format_name,
                        settings,
                        &is_column_nullable);
                    arrow_fields.emplace_back(std::make_shared<arrow::Field>(header_column.name, arrow_type, is_column_nullable));
                }

                arrow::MemoryPool * pool = ArrowMemoryPool::instance();
                std::unique_ptr<arrow::ArrayBuilder> array_builder;
                arrow::Status status = MakeBuilder(pool, arrow_fields[column_i]->type(), &array_builder);
                checkStatus(status, column->getName(), format_name);

                fillArrowArray(
                    header_column.name,
                    column,
                    header_column.type,
                    nullptr,
                    array_builder.get(),
                    format_name,
                    0,
                    column->size(),
                    settings,
                    dictionary_values);

                std::shared_ptr<arrow::Array> arrow_array;
                status = array_builder->Finish(&arrow_array);
                checkStatus(status, column->getName(), format_name);

                table_data.at(column_i).emplace_back(std::move(arrow_array));
            }

            is_arrow_fields_initialized = true;
            if (!arrow_schema)
                arrow_schema = std::make_shared<arrow::Schema>(arrow_fields);
        }

        std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
        columns.reserve(columns_num);
        for (size_t column_i = 0; column_i < columns_num; ++column_i)
            columns.emplace_back(std::make_shared<arrow::ChunkedArray>(table_data.at(column_i)));

        res = arrow::Table::Make(arrow_schema, columns);
    }

}

#endif
