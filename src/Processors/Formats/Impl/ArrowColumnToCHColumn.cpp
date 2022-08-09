#include "ArrowColumnToCHColumn.h"

#if USE_ARROW || USE_ORC || USE_PARQUET

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNothing.h>
#include <Common/DateLUTImpl.h>
#include <base/types.h>
#include <Processors/Chunk.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnUnique.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNothing.h>
#include <Interpreters/castColumn.h>
#include <Common/quoteString.h>
#include <algorithm>
#include <arrow/builder.h>
#include <arrow/array.h>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/case_conv.hpp>

/// UINT16 and UINT32 are processed separately, see comments in readColumnFromArrowColumn.
#define FOR_ARROW_NUMERIC_TYPES(M) \
        M(arrow::Type::UINT8, DB::UInt8) \
        M(arrow::Type::INT8, DB::Int8) \
        M(arrow::Type::INT16, DB::Int16) \
        M(arrow::Type::INT32, DB::Int32) \
        M(arrow::Type::UINT64, DB::UInt64) \
        M(arrow::Type::INT64, DB::Int64) \
        M(arrow::Type::HALF_FLOAT, DB::Float32) \
        M(arrow::Type::FLOAT, DB::Float32) \
        M(arrow::Type::DOUBLE, DB::Float64)

#define FOR_ARROW_INDEXES_TYPES(M) \
        M(arrow::Type::UINT8, DB::UInt8) \
        M(arrow::Type::INT8, DB::UInt8) \
        M(arrow::Type::UINT16, DB::UInt16) \
        M(arrow::Type::INT16, DB::UInt16) \
        M(arrow::Type::UINT32, DB::UInt32) \
        M(arrow::Type::INT32, DB::UInt32) \
        M(arrow::Type::UINT64, DB::UInt64) \
        M(arrow::Type::INT64, DB::UInt64)

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
    extern const int BAD_ARGUMENTS;
    extern const int DUPLICATE_COLUMN;
    extern const int THERE_IS_NO_COLUMN;
    extern const int UNKNOWN_EXCEPTION;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
}

/// Inserts numeric data right into internal column data to reduce an overhead
template <typename NumericType, typename VectorType = ColumnVector<NumericType>>
static ColumnWithTypeAndName readColumnWithNumericData(std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name)
{
    auto internal_type = std::make_shared<DataTypeNumber<NumericType>>();
    auto internal_column = internal_type->createColumn();
    auto & column_data = static_cast<VectorType &>(*internal_column).getData();
    column_data.reserve(arrow_column->length());

    for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
    {
        std::shared_ptr<arrow::Array> chunk = arrow_column->chunk(chunk_i);
        if (chunk->length() == 0)
            continue;

        /// buffers[0] is a null bitmap and buffers[1] are actual values
        std::shared_ptr<arrow::Buffer> buffer = chunk->data()->buffers[1];
        const auto * raw_data = reinterpret_cast<const NumericType *>(buffer->data());
        column_data.insert_assume_reserved(raw_data, raw_data + chunk->length());
    }
    return {std::move(internal_column), std::move(internal_type), column_name};
}

/// Inserts chars and offsets right into internal column data to reduce an overhead.
/// Internal offsets are shifted by one to the right in comparison with Arrow ones. So the last offset should map to the end of all chars.
/// Also internal strings are null terminated.
static ColumnWithTypeAndName readColumnWithStringData(std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name)
{
    auto internal_type = std::make_shared<DataTypeString>();
    auto internal_column = internal_type->createColumn();
    PaddedPODArray<UInt8> & column_chars_t = assert_cast<ColumnString &>(*internal_column).getChars();
    PaddedPODArray<UInt64> & column_offsets = assert_cast<ColumnString &>(*internal_column).getOffsets();

    size_t chars_t_size = 0;
    for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
    {
        arrow::BinaryArray & chunk = dynamic_cast<arrow::BinaryArray &>(*(arrow_column->chunk(chunk_i)));
        const size_t chunk_length = chunk.length();

        if (chunk_length > 0)
        {
            chars_t_size += chunk.value_offset(chunk_length - 1) + chunk.value_length(chunk_length - 1);
            chars_t_size += chunk_length; /// additional space for null bytes
        }
    }

    column_chars_t.reserve(chars_t_size);
    column_offsets.reserve(arrow_column->length());

    for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
    {
        arrow::BinaryArray & chunk = dynamic_cast<arrow::BinaryArray &>(*(arrow_column->chunk(chunk_i)));
        std::shared_ptr<arrow::Buffer> buffer = chunk.value_data();
        const size_t chunk_length = chunk.length();

        for (size_t offset_i = 0; offset_i != chunk_length; ++offset_i)
        {
            if (!chunk.IsNull(offset_i) && buffer)
            {
                const auto * raw_data = buffer->data() + chunk.value_offset(offset_i);
                column_chars_t.insert_assume_reserved(raw_data, raw_data + chunk.value_length(offset_i));
            }
            column_chars_t.emplace_back('\0');

            column_offsets.emplace_back(column_chars_t.size());
        }
    }
    return {std::move(internal_column), std::move(internal_type), column_name};
}

static ColumnWithTypeAndName readColumnWithBooleanData(std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name)
{
    auto internal_type = std::make_shared<DataTypeUInt8>();
    auto internal_column = internal_type->createColumn();
    auto & column_data = assert_cast<ColumnVector<UInt8> &>(*internal_column).getData();
    column_data.reserve(arrow_column->length());

    for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
    {
        arrow::BooleanArray & chunk = dynamic_cast<arrow::BooleanArray &>(*(arrow_column->chunk(chunk_i)));
        if (chunk.length() == 0)
            continue;

        /// buffers[0] is a null bitmap and buffers[1] are actual values
        std::shared_ptr<arrow::Buffer> buffer = chunk.data()->buffers[1];

        for (size_t bool_i = 0; bool_i != static_cast<size_t>(chunk.length()); ++bool_i)
            column_data.emplace_back(chunk.Value(bool_i));
    }
    return {std::move(internal_column), std::move(internal_type), column_name};
}

static ColumnWithTypeAndName readColumnWithDate32Data(std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name)
{
    auto internal_type = std::make_shared<DataTypeDate32>();
    auto internal_column = internal_type->createColumn();
    PaddedPODArray<Int32> & column_data = assert_cast<ColumnVector<Int32> &>(*internal_column).getData();
    column_data.reserve(arrow_column->length());

    for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
    {
        arrow::Date32Array & chunk = dynamic_cast<arrow::Date32Array &>(*(arrow_column->chunk(chunk_i)));

        for (size_t value_i = 0, length = static_cast<size_t>(chunk.length()); value_i < length; ++value_i)
        {
            Int32 days_num = static_cast<Int32>(chunk.Value(value_i));
            if (days_num > DATE_LUT_MAX_EXTEND_DAY_NUM)
                throw Exception{ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                        "Input value {} of a column \"{}\" is greater than max allowed Date value, which is {}", days_num, column_name, DATE_LUT_MAX_DAY_NUM};

            column_data.emplace_back(days_num);
        }
    }
    return {std::move(internal_column), std::move(internal_type), column_name};
}

/// Arrow stores Parquet::DATETIME in Int64, while ClickHouse stores DateTime in UInt32. Therefore, it should be checked before saving
static ColumnWithTypeAndName readColumnWithDate64Data(std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name)
{
    auto internal_type = std::make_shared<DataTypeDateTime>();
    auto internal_column = internal_type->createColumn();
    auto & column_data = assert_cast<ColumnVector<UInt32> &>(*internal_column).getData();
    column_data.reserve(arrow_column->length());

    for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
    {
        auto & chunk = dynamic_cast<arrow::Date64Array &>(*(arrow_column->chunk(chunk_i)));
        for (size_t value_i = 0, length = static_cast<size_t>(chunk.length()); value_i < length; ++value_i)
        {
            auto timestamp = static_cast<UInt32>(chunk.Value(value_i) / 1000); // Always? in ms
            column_data.emplace_back(timestamp);
        }
    }
    return {std::move(internal_column), std::move(internal_type), column_name};
}

static ColumnWithTypeAndName readColumnWithTimestampData(std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name)
{
    const auto & arrow_type = static_cast<const arrow::TimestampType &>(*(arrow_column->type()));
    const UInt8 scale = arrow_type.unit() * 3;
    auto internal_type = std::make_shared<DataTypeDateTime64>(scale, arrow_type.timezone());
    auto internal_column = internal_type->createColumn();
    auto & column_data = assert_cast<ColumnDecimal<DateTime64> &>(*internal_column).getData();
    column_data.reserve(arrow_column->length());

    for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
    {
        const auto & chunk = dynamic_cast<const arrow::TimestampArray &>(*(arrow_column->chunk(chunk_i)));
        for (size_t value_i = 0, length = static_cast<size_t>(chunk.length()); value_i < length; ++value_i)
        {
            column_data.emplace_back(chunk.Value(value_i));
        }
    }
    return {std::move(internal_column), std::move(internal_type), column_name};
}

template <typename DecimalType, typename DecimalArray>
static ColumnWithTypeAndName readColumnWithDecimalDataImpl(std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name, DataTypePtr internal_type)
{
    auto internal_column = internal_type->createColumn();
    auto & column = assert_cast<ColumnDecimal<DecimalType> &>(*internal_column);
    auto & column_data = column.getData();
    column_data.reserve(arrow_column->length());

    for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
    {
        auto & chunk = dynamic_cast<DecimalArray &>(*(arrow_column->chunk(chunk_i)));
        for (size_t value_i = 0, length = static_cast<size_t>(chunk.length()); value_i < length; ++value_i)
        {
            column_data.emplace_back(chunk.IsNull(value_i) ? DecimalType(0) : *reinterpret_cast<const DecimalType *>(chunk.Value(value_i))); // TODO: copy column
        }
    }
    return {std::move(internal_column), internal_type, column_name};
}

template <typename DecimalArray>
static ColumnWithTypeAndName readColumnWithDecimalData(std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name)
{
    const auto * arrow_decimal_type = static_cast<arrow::DecimalType *>(arrow_column->type().get());
    size_t precision = arrow_decimal_type->precision();
    auto internal_type = createDecimal<DataTypeDecimal>(precision, arrow_decimal_type->scale());
    if (precision <= DecimalUtils::max_precision<Decimal32>)
        return readColumnWithDecimalDataImpl<Decimal32, DecimalArray>(arrow_column, column_name, internal_type);
    else if (precision <= DecimalUtils::max_precision<Decimal64>)
        return readColumnWithDecimalDataImpl<Decimal64, DecimalArray>(arrow_column, column_name, internal_type);
    else if (precision <= DecimalUtils::max_precision<Decimal128>)
        return readColumnWithDecimalDataImpl<Decimal128, DecimalArray>(arrow_column, column_name, internal_type);
    return readColumnWithDecimalDataImpl<Decimal256, DecimalArray>(arrow_column, column_name, internal_type);
}

/// Creates a null bytemap from arrow's null bitmap
static ColumnPtr readByteMapFromArrowColumn(std::shared_ptr<arrow::ChunkedArray> & arrow_column)
{
    auto nullmap_column = ColumnUInt8::create();
    PaddedPODArray<UInt8> & bytemap_data = assert_cast<ColumnVector<UInt8> &>(*nullmap_column).getData();
    bytemap_data.reserve(arrow_column->length());

    for (size_t chunk_i = 0; chunk_i != static_cast<size_t>(arrow_column->num_chunks()); ++chunk_i)
    {
        std::shared_ptr<arrow::Array> chunk = arrow_column->chunk(chunk_i);

        for (size_t value_i = 0; value_i != static_cast<size_t>(chunk->length()); ++value_i)
            bytemap_data.emplace_back(chunk->IsNull(value_i));
    }
    return nullmap_column;
}

static ColumnPtr readOffsetsFromArrowListColumn(std::shared_ptr<arrow::ChunkedArray> & arrow_column)
{
    auto offsets_column = ColumnUInt64::create();
    ColumnArray::Offsets & offsets_data = assert_cast<ColumnVector<UInt64> &>(*offsets_column).getData();
    offsets_data.reserve(arrow_column->length());

    for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
    {
        arrow::ListArray & list_chunk = dynamic_cast<arrow::ListArray &>(*(arrow_column->chunk(chunk_i)));
        auto arrow_offsets_array = list_chunk.offsets();
        auto & arrow_offsets = dynamic_cast<arrow::Int32Array &>(*arrow_offsets_array);
        auto start = offsets_data.back();
        for (int64_t i = 1; i < arrow_offsets.length(); ++i)
            offsets_data.emplace_back(start + arrow_offsets.Value(i));
    }
    return offsets_column;
}

static ColumnPtr readColumnWithIndexesData(std::shared_ptr<arrow::ChunkedArray> & arrow_column)
{
    switch (arrow_column->type()->id())
    {
#    define DISPATCH(ARROW_NUMERIC_TYPE, CPP_NUMERIC_TYPE) \
            case ARROW_NUMERIC_TYPE: \
            { \
                    return readColumnWithNumericData<CPP_NUMERIC_TYPE>(arrow_column, "").column; \
            }
        FOR_ARROW_INDEXES_TYPES(DISPATCH)
#    undef DISPATCH
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported type for indexes in LowCardinality: {}.", arrow_column->type()->name());
    }
}

static std::shared_ptr<arrow::ChunkedArray> getNestedArrowColumn(std::shared_ptr<arrow::ChunkedArray> & arrow_column)
{
    arrow::ArrayVector array_vector;
    array_vector.reserve(arrow_column->num_chunks());
    for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
    {
        arrow::ListArray & list_chunk = dynamic_cast<arrow::ListArray &>(*(arrow_column->chunk(chunk_i)));
        std::shared_ptr<arrow::Array> chunk = list_chunk.values();
        array_vector.emplace_back(std::move(chunk));
    }
    return std::make_shared<arrow::ChunkedArray>(array_vector);
}

static ColumnWithTypeAndName createLCColumnFromArrowDictionaryValues(
    const std::shared_ptr<ColumnWithTypeAndName> & dict_values,
    const ColumnPtr & indexes_column,
    const String & column_name
)
{
    auto lc_type = std::make_shared<DataTypeLowCardinality>(dict_values->type);

    auto lc_column = lc_type->createColumn();

    for (auto i = 0u; i < indexes_column->size(); i++)
    {
        Field f;
        dict_values->column->get(indexes_column->getUInt(i), f);
        lc_column->insert(f);
    }

    return {std::move(lc_column), std::move(lc_type), column_name};
}

static ColumnWithTypeAndName createLCOfNullableColumnFromArrowDictionaryValues(
    const std::shared_ptr<ColumnWithTypeAndName> & dict_values,
    const ColumnPtr & indexes_column,
    const ColumnPtr & nullmap_column,
    const String & column_name
)
{
    /*
     * ArrowColumn format handles nulls by maintaining a nullmap column, there is no nullable type.
     * Therefore, dict_values->type is the actual data type/ non-nullable. It needs to be transformed into nullable
     * so LC column is created from nullable type and a null value at the beginning of the collection
     * is automatically added.
     * */
    auto lc_type = std::make_shared<DataTypeLowCardinality>(makeNullable(dict_values->type));

    auto lc_column = lc_type->createColumn();

    for (auto i = 0u; i < indexes_column->size(); i++)
    {
        if (nullmap_column && nullmap_column->getBool(i))
        {
            lc_column->insertDefault();
        }
        else
        {
            Field f;
            dict_values->column->get(indexes_column->getUInt(i), f);
            lc_column->insert(f);
        }
    }

    return {std::move(lc_column), std::move(lc_type), column_name};
}

static ColumnWithTypeAndName readColumnFromArrowColumn(
    std::shared_ptr<arrow::ChunkedArray> & arrow_column,
    const std::string & column_name,
    const std::string & format_name,
    bool is_nullable,
    std::unordered_map<String, std::shared_ptr<ColumnWithTypeAndName>> & dictionary_values,
    bool read_ints_as_dates,
    bool allow_null_type,
    bool skip_columns_with_unsupported_types,
    bool & skipped)
{
    if (!is_nullable && arrow_column->null_count() && arrow_column->type()->id() != arrow::Type::LIST
        && arrow_column->type()->id() != arrow::Type::MAP && arrow_column->type()->id() != arrow::Type::STRUCT &&
        arrow_column->type()->id() != arrow::Type::DICTIONARY)
    {
        auto nested_column = readColumnFromArrowColumn(arrow_column, column_name, format_name, true, dictionary_values, read_ints_as_dates, allow_null_type, skip_columns_with_unsupported_types, skipped);
        if (skipped)
            return {};
        auto nullmap_column = readByteMapFromArrowColumn(arrow_column);
        auto nullable_type = std::make_shared<DataTypeNullable>(std::move(nested_column.type));
        auto nullable_column = ColumnNullable::create(nested_column.column, nullmap_column);
        return {std::move(nullable_column), std::move(nullable_type), column_name};
    }

    switch (arrow_column->type()->id())
    {
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
            //case arrow::Type::FIXED_SIZE_BINARY:
            return readColumnWithStringData(arrow_column, column_name);
        case arrow::Type::BOOL:
            return readColumnWithBooleanData(arrow_column, column_name);
        case arrow::Type::DATE32:
            return readColumnWithDate32Data(arrow_column, column_name);
        case arrow::Type::DATE64:
            return readColumnWithDate64Data(arrow_column, column_name);
        // ClickHouse writes Date as arrow UINT16 and DateTime as arrow UINT32,
        // so, read UINT16 as Date and UINT32 as DateTime to perform correct conversion
        // between Date and DateTime further.
        case arrow::Type::UINT16:
        {
            auto column = readColumnWithNumericData<UInt16>(arrow_column, column_name);
            if (read_ints_as_dates)
                column.type = std::make_shared<DataTypeDate>();
            return column;
        }
        case arrow::Type::UINT32:
        {
            auto column = readColumnWithNumericData<UInt32>(arrow_column, column_name);
            if (read_ints_as_dates)
                column.type = std::make_shared<DataTypeDateTime>();
            return column;
        }
        case arrow::Type::TIMESTAMP:
            return readColumnWithTimestampData(arrow_column, column_name);
        case arrow::Type::DECIMAL128:
            return readColumnWithDecimalData<arrow::Decimal128Array>(arrow_column, column_name);
        case arrow::Type::DECIMAL256:
            return readColumnWithDecimalData<arrow::Decimal256Array>(arrow_column, column_name);
        case arrow::Type::MAP:
        {
            auto arrow_nested_column = getNestedArrowColumn(arrow_column);
            auto nested_column = readColumnFromArrowColumn(arrow_nested_column, column_name, format_name, false, dictionary_values, read_ints_as_dates, allow_null_type, skip_columns_with_unsupported_types, skipped);
            if (skipped)
                return {};

            auto offsets_column = readOffsetsFromArrowListColumn(arrow_column);

            const auto * tuple_column = assert_cast<const ColumnTuple *>(nested_column.column.get());
            const auto * tuple_type = assert_cast<const DataTypeTuple *>(nested_column.type.get());
            auto map_column = ColumnMap::create(tuple_column->getColumnPtr(0), tuple_column->getColumnPtr(1), offsets_column);
            auto map_type = std::make_shared<DataTypeMap>(tuple_type->getElements()[0], tuple_type->getElements()[1]);
            return {std::move(map_column), std::move(map_type), column_name};
        }
        case arrow::Type::LIST:
        {
            auto arrow_nested_column = getNestedArrowColumn(arrow_column);
            auto nested_column = readColumnFromArrowColumn(arrow_nested_column, column_name, format_name, false, dictionary_values, read_ints_as_dates, allow_null_type, skip_columns_with_unsupported_types, skipped);
            if (skipped)
                return {};
            auto offsets_column = readOffsetsFromArrowListColumn(arrow_column);
            auto array_column = ColumnArray::create(nested_column.column, offsets_column);
            auto array_type = std::make_shared<DataTypeArray>(nested_column.type);
            return {std::move(array_column), std::move(array_type), column_name};
        }
        case arrow::Type::STRUCT:
        {
            auto arrow_type = arrow_column->type();
            auto * arrow_struct_type = assert_cast<arrow::StructType *>(arrow_type.get());
            std::vector<arrow::ArrayVector> nested_arrow_columns(arrow_struct_type->num_fields());
            for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
            {
                arrow::StructArray & struct_chunk = dynamic_cast<arrow::StructArray &>(*(arrow_column->chunk(chunk_i)));
                for (int i = 0; i < arrow_struct_type->num_fields(); ++i)
                    nested_arrow_columns[i].emplace_back(struct_chunk.field(i));
            }

            Columns tuple_elements;
            DataTypes tuple_types;
            std::vector<String> tuple_names;

            for (int i = 0; i != arrow_struct_type->num_fields(); ++i)
            {
                auto nested_arrow_column = std::make_shared<arrow::ChunkedArray>(nested_arrow_columns[i]);
                auto element = readColumnFromArrowColumn(nested_arrow_column, arrow_struct_type->field(i)->name(), format_name, false, dictionary_values, read_ints_as_dates, allow_null_type, skip_columns_with_unsupported_types, skipped);
                if (skipped)
                    return {};
                tuple_elements.emplace_back(std::move(element.column));
                tuple_types.emplace_back(std::move(element.type));
                tuple_names.emplace_back(std::move(element.name));
            }

            auto tuple_column = ColumnTuple::create(std::move(tuple_elements));
            auto tuple_type = std::make_shared<DataTypeTuple>(std::move(tuple_types), std::move(tuple_names));
            return {std::move(tuple_column), std::move(tuple_type), column_name};
        }
        case arrow::Type::DICTIONARY:
        {
            auto & dict_values = dictionary_values[column_name];
            /// Load dictionary values only once and reuse it.
            if (!dict_values)
            {
                arrow::ArrayVector dict_array;
                for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
                {
                    arrow::DictionaryArray & dict_chunk = dynamic_cast<arrow::DictionaryArray &>(*(arrow_column->chunk(chunk_i)));
                    dict_array.emplace_back(dict_chunk.dictionary());
                }
                auto arrow_dict_column = std::make_shared<arrow::ChunkedArray>(dict_array);
                auto dict_column = readColumnFromArrowColumn(arrow_dict_column, column_name, format_name, false, dictionary_values, read_ints_as_dates, allow_null_type, skip_columns_with_unsupported_types, skipped);
                dict_values = std::make_shared<ColumnWithTypeAndName>(std::move(dict_column));
            }

            arrow::ArrayVector indexes_array;
            for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
            {
                arrow::DictionaryArray & dict_chunk = dynamic_cast<arrow::DictionaryArray &>(*(arrow_column->chunk(chunk_i)));
                indexes_array.emplace_back(dict_chunk.indices());
            }

            auto arrow_indexes_column = std::make_shared<arrow::ChunkedArray>(indexes_array);
            auto indexes_column = readColumnWithIndexesData(arrow_indexes_column);

            const auto contains_null = arrow_column->null_count() > 0;

            if (contains_null)
            {
                auto nullmap_column = readByteMapFromArrowColumn(arrow_column);

                return createLCOfNullableColumnFromArrowDictionaryValues(dict_values, indexes_column, nullmap_column, column_name);
            }
            else
            {
                return createLCColumnFromArrowDictionaryValues(dict_values, indexes_column, column_name);
            }
        }
#    define DISPATCH(ARROW_NUMERIC_TYPE, CPP_NUMERIC_TYPE) \
        case ARROW_NUMERIC_TYPE: \
            return readColumnWithNumericData<CPP_NUMERIC_TYPE>(arrow_column, column_name);
        FOR_ARROW_NUMERIC_TYPES(DISPATCH)
#    undef DISPATCH
            // TODO: read JSON as a string?
            // TODO: read UUID as a string?
        case arrow::Type::NA:
        {
            if (allow_null_type)
            {
                auto type = std::make_shared<DataTypeNothing>();
                auto column = ColumnNothing::create(arrow_column->length());
                return {std::move(column), type, column_name};
            }
            [[fallthrough]];
        }
        default:
        {
            if (skip_columns_with_unsupported_types)
            {
                skipped = true;
                return {};
            }

            throw Exception(
                ErrorCodes::UNKNOWN_TYPE,
                "Unsupported {} type '{}' of an input column '{}'. If it happens during schema inference and you want to skip columns with "
                "unsupported types, you can enable setting input_format_{}_skip_columns_with_unsupported_types_in_schema_inference",
                format_name,
                arrow_column->type()->name(),
                column_name,
                boost::algorithm::to_lower_copy(format_name));
        }
    }
}


// Creating CH header by arrow schema. Will be useful in task about inserting
// data from file without knowing table structure.

static void checkStatus(const arrow::Status & status, const String & column_name, const String & format_name)
{
    if (!status.ok())
        throw Exception{ErrorCodes::UNKNOWN_EXCEPTION, "Error with a {} column '{}': {}.", format_name, column_name, status.ToString()};
}


Block ArrowColumnToCHColumn::arrowSchemaToCHHeader(
    const arrow::Schema & schema, const std::string & format_name, bool skip_columns_with_unsupported_types, const Block * hint_header, bool ignore_case)
{
    ColumnsWithTypeAndName sample_columns;
    std::unordered_set<String> nested_table_names;
    if (hint_header)
        nested_table_names = Nested::getAllTableNames(*hint_header, ignore_case);

    for (const auto & field : schema.fields())
    {
        if (hint_header && !hint_header->has(field->name(), ignore_case)
            && !nested_table_names.contains(ignore_case ? boost::to_lower_copy(field->name()) : field->name()))
            continue;

        /// Create empty arrow column by it's type and convert it to ClickHouse column.
        arrow::MemoryPool * pool = arrow::default_memory_pool();
        std::unique_ptr<arrow::ArrayBuilder> array_builder;
        arrow::Status status = MakeBuilder(pool, field->type(), &array_builder);
        checkStatus(status, field->name(), format_name);

        std::shared_ptr<arrow::Array> arrow_array;
        status = array_builder->Finish(&arrow_array);
        checkStatus(status, field->name(), format_name);

        arrow::ArrayVector array_vector = {arrow_array};
        auto arrow_column = std::make_shared<arrow::ChunkedArray>(array_vector);
        std::unordered_map<std::string, std::shared_ptr<ColumnWithTypeAndName>> dict_values;
        bool skipped = false;
        bool allow_null_type = false;
        if (hint_header && hint_header->has(field->name()) && hint_header->getByName(field->name()).type->isNullable())
            allow_null_type = true;
        ColumnWithTypeAndName sample_column = readColumnFromArrowColumn(
            arrow_column, field->name(), format_name, false, dict_values, false, allow_null_type, skip_columns_with_unsupported_types, skipped);
        if (!skipped)
            sample_columns.emplace_back(std::move(sample_column));
    }
    return Block(std::move(sample_columns));
}

ArrowColumnToCHColumn::ArrowColumnToCHColumn(
    const Block & header_,
    const std::string & format_name_,
    bool import_nested_,
    bool allow_missing_columns_,
    bool case_insensitive_matching_)
    : header(header_)
    , format_name(format_name_)
    , import_nested(import_nested_)
    , allow_missing_columns(allow_missing_columns_)
    , case_insensitive_matching(case_insensitive_matching_)
{
}

void ArrowColumnToCHColumn::arrowTableToCHChunk(Chunk & res, std::shared_ptr<arrow::Table> & table)
{
    NameToColumnPtr name_to_column_ptr;
    for (auto column_name : table->ColumnNames())
    {
        std::shared_ptr<arrow::ChunkedArray> arrow_column = table->GetColumnByName(column_name);
        if (!arrow_column)
            throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Column '{}' is duplicated", column_name);

        if (case_insensitive_matching)
            boost::to_lower(column_name);
        name_to_column_ptr[std::move(column_name)] = arrow_column;
    }

    arrowColumnsToCHChunk(res, name_to_column_ptr);
}

void ArrowColumnToCHColumn::arrowColumnsToCHChunk(Chunk & res, NameToColumnPtr & name_to_column_ptr)
{
    if (unlikely(name_to_column_ptr.empty()))
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Columns is empty");

    Columns columns_list;
    UInt64 num_rows = name_to_column_ptr.begin()->second->length();
    columns_list.reserve(header.columns());
    std::unordered_map<String, std::pair<BlockPtr, std::shared_ptr<NestedColumnExtractHelper>>> nested_tables;
    bool skipped = false;
    for (size_t column_i = 0, columns = header.columns(); column_i < columns; ++column_i)
    {
        const ColumnWithTypeAndName & header_column = header.getByPosition(column_i);

        auto search_column_name = header_column.name;
        if (case_insensitive_matching)
            boost::to_lower(search_column_name);

        ColumnWithTypeAndName column;
        if (!name_to_column_ptr.contains(search_column_name))
        {
            bool read_from_nested = false;
            /// Check if it's a column from nested table.
            if (import_nested)
            {
                String nested_table_name = Nested::extractTableName(header_column.name);
                String search_nested_table_name = nested_table_name;
                if (case_insensitive_matching)
                    boost::to_lower(search_nested_table_name);
                if (name_to_column_ptr.contains(search_nested_table_name))
                {
                    if (!nested_tables.contains(search_nested_table_name))
                    {
                        std::shared_ptr<arrow::ChunkedArray> arrow_column = name_to_column_ptr[search_nested_table_name];
                        ColumnsWithTypeAndName cols = {readColumnFromArrowColumn(
                            arrow_column, nested_table_name, format_name, false, dictionary_values, true, true, false, skipped)};
                        BlockPtr block_ptr = std::make_shared<Block>(cols);
                        auto column_extractor = std::make_shared<NestedColumnExtractHelper>(*block_ptr, case_insensitive_matching);
                        nested_tables[search_nested_table_name] = {block_ptr, column_extractor};
                    }
                    auto nested_column = nested_tables[search_nested_table_name].second->extractColumn(search_column_name);
                    if (nested_column)
                    {
                        column = *nested_column;
                        if (case_insensitive_matching)
                            column.name = header_column.name;
                        read_from_nested = true;
                    }
                }
            }
            if (!read_from_nested)
            {
                if (!allow_missing_columns)
                    throw Exception{ErrorCodes::THERE_IS_NO_COLUMN, "Column '{}' is not presented in input data.", header_column.name};
                else
                {
                    column.name = header_column.name;
                    column.type = header_column.type;
                    column.column = header_column.column->cloneResized(num_rows);
                    columns_list.push_back(std::move(column.column));
                    continue;
                }
            }
        }
        else
        {
            auto arrow_column = name_to_column_ptr[search_column_name];
            column = readColumnFromArrowColumn(
                arrow_column, header_column.name, format_name, false, dictionary_values, true, true, false, skipped);
        }

        try
        {
            column.column = castColumn(column, header_column.type);
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format(
                "while converting column {} from type {} to type {}",
                backQuote(header_column.name),
                column.type->getName(),
                header_column.type->getName()));
            throw;
        }

        column.type = header_column.type;
        columns_list.push_back(std::move(column.column));
    }

    res.setColumns(columns_list, num_rows);
}

std::vector<size_t> ArrowColumnToCHColumn::getMissingColumns(const arrow::Schema & schema) const
{
    std::vector<size_t> missing_columns;
    auto block_from_arrow = arrowSchemaToCHHeader(schema, format_name, false, &header, case_insensitive_matching);
    NestedColumnExtractHelper nested_columns_extractor(block_from_arrow, case_insensitive_matching);

    for (size_t i = 0, columns = header.columns(); i < columns; ++i)
    {
        const auto & header_column = header.getByPosition(i);
        if (!block_from_arrow.has(header_column.name, case_insensitive_matching))
        {
            if (!import_nested || !nested_columns_extractor.extractColumn(header_column.name))
            {
                if (!allow_missing_columns)
                    throw Exception{ErrorCodes::THERE_IS_NO_COLUMN, "Column '{}' is not presented in input data.", header_column.name};
                missing_columns.push_back(i);
            }
        }
    }
    return missing_columns;
}

}

#endif
