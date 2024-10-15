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
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <Common/DateLUTImpl.h>
#include <base/types.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
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
#include <Formats/insertNullAsDefaultIfNeeded.h>
#include <algorithm>
#include <arrow/builder.h>
#include <arrow/array.h>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/case_conv.hpp>

/// UINT16 and UINT32 are processed separately, see comments in readColumnFromArrowColumn.
#define FOR_ARROW_NUMERIC_TYPES(M) \
        M(arrow::Type::UINT8, UInt8) \
        M(arrow::Type::INT8, Int8) \
        M(arrow::Type::INT16, Int16) \
        M(arrow::Type::UINT64, UInt64) \
        M(arrow::Type::INT64, Int64) \
        M(arrow::Type::DURATION, Int64) \
        M(arrow::Type::HALF_FLOAT, Float32) \
        M(arrow::Type::FLOAT, Float32) \
        M(arrow::Type::DOUBLE, Float64)

#define FOR_ARROW_INDEXES_TYPES(M) \
        M(arrow::Type::UINT8, UInt8) \
        M(arrow::Type::INT8, UInt8) \
        M(arrow::Type::UINT16, UInt16) \
        M(arrow::Type::INT16, UInt16) \
        M(arrow::Type::UINT32, UInt32) \
        M(arrow::Type::INT32, UInt32) \
        M(arrow::Type::UINT64, UInt64) \
        M(arrow::Type::INT64, UInt64)

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
    extern const int INCORRECT_DATA;
}

/// Inserts numeric data right into internal column data to reduce an overhead
template <typename NumericType, typename VectorType = ColumnVector<NumericType>>
static ColumnWithTypeAndName readColumnWithNumericData(const std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name)
{
    auto internal_type = std::make_shared<DataTypeNumber<NumericType>>();
    auto internal_column = internal_type->createColumn();
    auto & column_data = static_cast<VectorType &>(*internal_column).getData();
    column_data.reserve(arrow_column->length());

    for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
    {
        std::shared_ptr<arrow::Array> chunk = arrow_column->chunk(chunk_i);
        if (chunk->length() == 0)
            continue;

        /// buffers[0] is a null bitmap and buffers[1] are actual values
        std::shared_ptr<arrow::Buffer> buffer = chunk->data()->buffers[1];
        const auto * raw_data = reinterpret_cast<const NumericType *>(buffer->data()) + chunk->offset();
        column_data.insert_assume_reserved(raw_data, raw_data + chunk->length());
    }
    return {std::move(internal_column), std::move(internal_type), column_name};
}

/// Inserts chars and offsets right into internal column data to reduce an overhead.
/// Internal offsets are shifted by one to the right in comparison with Arrow ones. So the last offset should map to the end of all chars.
/// Also internal strings are null terminated.
template <typename ArrowArray>
static ColumnWithTypeAndName readColumnWithStringData(const std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name)
{
    auto internal_type = std::make_shared<DataTypeString>();
    auto internal_column = internal_type->createColumn();
    PaddedPODArray<UInt8> & column_chars_t = assert_cast<ColumnString &>(*internal_column).getChars();
    PaddedPODArray<UInt64> & column_offsets = assert_cast<ColumnString &>(*internal_column).getOffsets();

    size_t chars_t_size = 0;
    for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
    {
        ArrowArray & chunk = dynamic_cast<ArrowArray &>(*(arrow_column->chunk(chunk_i)));
        const size_t chunk_length = chunk.length();

        if (chunk_length > 0)
        {
            chars_t_size += chunk.value_offset(chunk_length - 1) + chunk.value_length(chunk_length - 1);
            chars_t_size += chunk_length; /// additional space for null bytes
        }
    }

    column_chars_t.reserve(chars_t_size);
    column_offsets.reserve(arrow_column->length());

    for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
    {
        ArrowArray & chunk = dynamic_cast<ArrowArray &>(*(arrow_column->chunk(chunk_i)));
        std::shared_ptr<arrow::Buffer> buffer = chunk.value_data();
        const size_t chunk_length = chunk.length();

        const size_t null_count = chunk.null_count();
        if (null_count == 0)
        {
            for (size_t offset_i = 0; offset_i != chunk_length; ++offset_i)
            {
                const auto * raw_data = buffer->data() + chunk.value_offset(offset_i);
                column_chars_t.insert_assume_reserved(raw_data, raw_data + chunk.value_length(offset_i));
                column_chars_t.emplace_back('\0');

                column_offsets.emplace_back(column_chars_t.size());
            }
        }
        else
        {
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
    }
    return {std::move(internal_column), std::move(internal_type), column_name};
}

static ColumnWithTypeAndName readColumnWithFixedStringData(const std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name)
{
    const auto * fixed_type = assert_cast<arrow::FixedSizeBinaryType *>(arrow_column->type().get());
    size_t fixed_len = fixed_type->byte_width();
    auto internal_type = std::make_shared<DataTypeFixedString>(fixed_len);
    auto internal_column = internal_type->createColumn();
    PaddedPODArray<UInt8> & column_chars_t = assert_cast<ColumnFixedString &>(*internal_column).getChars();
    column_chars_t.reserve(arrow_column->length() * fixed_len);

    for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
    {
        arrow::FixedSizeBinaryArray & chunk = dynamic_cast<arrow::FixedSizeBinaryArray &>(*(arrow_column->chunk(chunk_i)));
        const uint8_t * raw_data = chunk.raw_values();
        column_chars_t.insert_assume_reserved(raw_data, raw_data + fixed_len * chunk.length());
    }
    return {std::move(internal_column), std::move(internal_type), column_name};
}

template <typename ValueType>
static ColumnWithTypeAndName readColumnWithBigIntegerFromFixedBinaryData(const std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name, const DataTypePtr & column_type)
{
    const auto * fixed_type = assert_cast<arrow::FixedSizeBinaryType *>(arrow_column->type().get());
    size_t fixed_len = fixed_type->byte_width();
    if (fixed_len != sizeof(ValueType))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot insert data into {} column from fixed size binary, expected data with size {}, got {}",
            column_type->getName(),
            sizeof(ValueType),
            fixed_len);

    auto internal_column = column_type->createColumn();
    auto & data = assert_cast<ColumnVector<ValueType> &>(*internal_column).getData();
    data.reserve(arrow_column->length());

    for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
    {
        arrow::FixedSizeBinaryArray & chunk = dynamic_cast<arrow::FixedSizeBinaryArray &>(*(arrow_column->chunk(chunk_i)));
        const auto * raw_data = reinterpret_cast<const ValueType *>(chunk.raw_values());
        data.insert_assume_reserved(raw_data, raw_data + chunk.length());
    }

    return {std::move(internal_column), column_type, column_name};
}

template <typename ColumnType, typename ValueType = typename ColumnType::ValueType>
static ColumnWithTypeAndName readColumnWithBigNumberFromBinaryData(const std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name, const DataTypePtr & column_type)
{
    size_t total_size = 0;
    for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
    {
        auto & chunk = dynamic_cast<arrow::BinaryArray &>(*(arrow_column->chunk(chunk_i)));
        const size_t chunk_length = chunk.length();

        for (size_t i = 0; i != chunk_length; ++i)
        {
            /// If at least one value size is not equal to the size if big integer, fallback to reading String column and further cast to result type.
            if (!chunk.IsNull(i) && chunk.value_length(i) != sizeof(ValueType))
                return readColumnWithStringData<arrow::BinaryArray>(arrow_column, column_name);

            total_size += chunk_length;
        }
    }

    auto internal_column = column_type->createColumn();
    auto & integer_column = assert_cast<ColumnType &>(*internal_column);
    integer_column.reserve(total_size);

    for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
    {
        auto & chunk = dynamic_cast<arrow::BinaryArray &>(*(arrow_column->chunk(chunk_i)));
        for (size_t value_i = 0, length = static_cast<size_t>(chunk.length()); value_i < length; ++value_i)
        {
            if (chunk.IsNull(value_i))
                integer_column.insertDefault();
            else
                integer_column.insertData(chunk.Value(value_i).data(), chunk.Value(value_i).size());
        }
    }
    return {std::move(internal_column), column_type, column_name};
}

static ColumnWithTypeAndName readColumnWithBooleanData(const std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name)
{
    auto internal_type = DataTypeFactory::instance().get("Bool");
    auto internal_column = internal_type->createColumn();
    auto & column_data = assert_cast<ColumnVector<UInt8> &>(*internal_column).getData();
    column_data.reserve(arrow_column->length());

    for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
    {
        arrow::BooleanArray & chunk = dynamic_cast<arrow::BooleanArray &>(*(arrow_column->chunk(chunk_i)));
        if (chunk.length() == 0)
            continue;

        for (size_t bool_i = 0; bool_i != static_cast<size_t>(chunk.length()); ++bool_i)
            column_data.emplace_back(chunk.Value(bool_i));
    }
    return {std::move(internal_column), internal_type, column_name};
}

static ColumnWithTypeAndName readColumnWithDate32Data(const std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name,
                                                      const DataTypePtr & type_hint, FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior)
{
    DataTypePtr internal_type;
    bool check_date_range = false;
    /// Make result type Date32 when requested type is actually Date32 or when we use schema inference

    if (!type_hint || (type_hint && isDate32(*type_hint)))
    {
        internal_type = std::make_shared<DataTypeDate32>();
        check_date_range = true;
    }
    else
    {
        internal_type = std::make_shared<DataTypeInt32>();
    }

    auto internal_column = internal_type->createColumn();
    PaddedPODArray<Int32> & column_data = assert_cast<ColumnVector<Int32> &>(*internal_column).getData();
    column_data.reserve(arrow_column->length());

    for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
    {
        arrow::Date32Array & chunk = dynamic_cast<arrow::Date32Array &>(*(arrow_column->chunk(chunk_i)));

        /// Check date range only when requested type is actually Date32
        if (check_date_range)
        {
            for (size_t value_i = 0, length = static_cast<size_t>(chunk.length()); value_i < length; ++value_i)
            {
                Int32 days_num = static_cast<Int32>(chunk.Value(value_i));
                if (days_num > DATE_LUT_MAX_EXTEND_DAY_NUM || days_num < -DAYNUM_OFFSET_EPOCH)
                {
                    switch (date_time_overflow_behavior)
                    {
                        case FormatSettings::DateTimeOverflowBehavior::Saturate:
                            days_num = (days_num < -DAYNUM_OFFSET_EPOCH) ? -DAYNUM_OFFSET_EPOCH : DATE_LUT_MAX_EXTEND_DAY_NUM;
                            break;
                        default:
                        /// Prior to introducing `date_time_overflow_behavior`, this function threw an error in case value was out of range.
                        /// In order to leave this behavior as default, we also throw when `date_time_overflow_mode == ignore`, as it is the setting's default value
                        /// (As we want to make this backwards compatible, not break any workflows.)
                            throw Exception{ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                                            "Input value {} of a column \"{}\" is out of allowed Date32 range, which is [{}, {}]",
                                            days_num,column_name, -DAYNUM_OFFSET_EPOCH, DATE_LUT_MAX_EXTEND_DAY_NUM};
                    }
                }

                column_data.emplace_back(days_num);
            }
        }
        else
        {
            std::shared_ptr<arrow::Buffer> buffer = chunk.data()->buffers[1];
            const auto * raw_data = reinterpret_cast<const Int32 *>(buffer->data()) + chunk.offset();
            column_data.insert_assume_reserved(raw_data, raw_data + chunk.length());
        }
    }
    return {std::move(internal_column), internal_type, column_name};
}

/// Arrow stores Parquet::DATETIME in Int64, while ClickHouse stores DateTime in UInt32. Therefore, it should be checked before saving
static ColumnWithTypeAndName readColumnWithDate64Data(const std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name)
{
    auto internal_type = std::make_shared<DataTypeDateTime>();
    auto internal_column = internal_type->createColumn();
    auto & column_data = assert_cast<ColumnVector<UInt32> &>(*internal_column).getData();
    column_data.reserve(arrow_column->length());

    for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
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

static ColumnWithTypeAndName readColumnWithTimestampData(const std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name)
{
    const auto & arrow_type = static_cast<const arrow::TimestampType &>(*(arrow_column->type()));
    const UInt8 scale = arrow_type.unit() * 3;
    auto internal_type = std::make_shared<DataTypeDateTime64>(scale, arrow_type.timezone());
    auto internal_column = internal_type->createColumn();
    auto & column_data = assert_cast<ColumnDecimal<DateTime64> &>(*internal_column).getData();
    column_data.reserve(arrow_column->length());

    for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
    {
        const auto & chunk = dynamic_cast<const arrow::TimestampArray &>(*(arrow_column->chunk(chunk_i)));
        for (size_t value_i = 0, length = static_cast<size_t>(chunk.length()); value_i < length; ++value_i)
        {
            column_data.emplace_back(chunk.Value(value_i));
        }
    }
    return {std::move(internal_column), std::move(internal_type), column_name};
}

template <typename TimeType, typename TimeArray>
static ColumnWithTypeAndName readColumnWithTimeData(const std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name)
{
    const auto & arrow_type = static_cast<const TimeType &>(*(arrow_column->type()));
    const UInt8 scale = arrow_type.unit() * 3;
    auto internal_type = std::make_shared<DataTypeDateTime64>(scale);
    auto internal_column = internal_type->createColumn();
    internal_column->reserve(arrow_column->length());

    for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
    {
        auto & chunk = dynamic_cast<TimeArray &>(*(arrow_column->chunk(chunk_i)));
        if (chunk.length() == 0)
            continue;

        for (size_t value_i = 0, length = static_cast<size_t>(chunk.length()); value_i < length; ++value_i)
        {
            assert_cast<DataTypeDateTime64::ColumnType &>(*internal_column).insertValue(chunk.Value(value_i));
        }
    }

    return {std::move(internal_column), std::move(internal_type), column_name};
}

static ColumnWithTypeAndName readColumnWithTime32Data(const std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name)
{
    return readColumnWithTimeData<arrow::Time32Type, arrow::Time32Array>(arrow_column, column_name);
}

static ColumnWithTypeAndName readColumnWithTime64Data(const std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name)
{
    return readColumnWithTimeData<arrow::Time64Type, arrow::Time64Array>(arrow_column, column_name);
}

template <typename DecimalType, typename DecimalArray>
static ColumnWithTypeAndName readColumnWithDecimalDataImpl(const std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name, DataTypePtr internal_type)
{
    auto internal_column = internal_type->createColumn();
    auto & column = assert_cast<ColumnDecimal<DecimalType> &>(*internal_column);
    auto & column_data = column.getData();
    column_data.reserve(arrow_column->length());

    for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
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
static ColumnWithTypeAndName readColumnWithDecimalData(const std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name)
{
    const auto * arrow_decimal_type = static_cast<arrow::DecimalType *>(arrow_column->type().get());
    size_t precision = arrow_decimal_type->precision();
    auto internal_type = createDecimal<DataTypeDecimal>(precision, arrow_decimal_type->scale());
    if (precision <= DecimalUtils::max_precision<Decimal32>)
        return readColumnWithDecimalDataImpl<Decimal32, DecimalArray>(arrow_column, column_name, internal_type);
    if (precision <= DecimalUtils::max_precision<Decimal64>)
        return readColumnWithDecimalDataImpl<Decimal64, DecimalArray>(arrow_column, column_name, internal_type);
    if (precision <= DecimalUtils::max_precision<Decimal128>)
        return readColumnWithDecimalDataImpl<Decimal128, DecimalArray>(arrow_column, column_name, internal_type);
    return readColumnWithDecimalDataImpl<Decimal256, DecimalArray>(arrow_column, column_name, internal_type);
}

/// Creates a null bytemap from arrow's null bitmap
static ColumnPtr readByteMapFromArrowColumn(const std::shared_ptr<arrow::ChunkedArray> & arrow_column)
{
    if (!arrow_column->null_count())
        return ColumnUInt8::create(arrow_column->length(), 0);

    auto nullmap_column = ColumnUInt8::create();
    PaddedPODArray<UInt8> & bytemap_data = assert_cast<ColumnVector<UInt8> &>(*nullmap_column).getData();
    bytemap_data.reserve(arrow_column->length());

    for (int chunk_i = 0; chunk_i != arrow_column->num_chunks(); ++chunk_i)
    {
        std::shared_ptr<arrow::Array> chunk = arrow_column->chunk(chunk_i);

        for (size_t value_i = 0; value_i != static_cast<size_t>(chunk->length()); ++value_i)
            bytemap_data.emplace_back(chunk->IsNull(value_i));
    }
    return nullmap_column;
}

template <typename T>
struct ArrowOffsetArray;

template <>
struct ArrowOffsetArray<arrow::ListArray>
{
    using type = arrow::Int32Array;
};

template <>
struct ArrowOffsetArray<arrow::LargeListArray>
{
    using type = arrow::Int64Array;
};

template <typename ArrowListArray>
static ColumnPtr readOffsetsFromArrowListColumn(const std::shared_ptr<arrow::ChunkedArray> & arrow_column)
{
    auto offsets_column = ColumnUInt64::create();
    ColumnArray::Offsets & offsets_data = assert_cast<ColumnVector<UInt64> &>(*offsets_column).getData();
    offsets_data.reserve(arrow_column->length());

    for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
    {
        ArrowListArray & list_chunk = dynamic_cast<ArrowListArray &>(*(arrow_column->chunk(chunk_i)));
        auto arrow_offsets_array = list_chunk.offsets();
        auto & arrow_offsets = dynamic_cast<typename ArrowOffsetArray<ArrowListArray>::type &>(*arrow_offsets_array);

        /*
         * CH uses element size as "offsets", while arrow uses actual offsets as offsets.
         * That's why CH usually starts reading offsets with i=1 and i=0 is ignored.
         * In case multiple batches are used to read a column, there is a chance the offsets are
         * monotonically increasing, which will cause inconsistencies with the batch data length on `DB::ColumnArray`.
         *
         * If the offsets are monotonically increasing, `arrow_offsets.Value(0)` will be non-zero for the nth batch, where n > 0.
         * If they are not monotonically increasing, it'll always be 0.
         * Therefore, we subtract the previous offset from the current offset to get the corresponding CH "offset".
         *
         * The same might happen for multiple chunks. In this case, we need to add the last offset of the previous chunk, hence
         * `offsets.back()`. More info can be found in https://lists.apache.org/thread/rrwfb9zo2dc58dhd9rblf20xd7wmy7jm,
         * https://github.com/ClickHouse/ClickHouse/pull/43297 and https://github.com/ClickHouse/ClickHouse/pull/54370
         * */
        uint64_t previous_offset = arrow_offsets.Value(0);

        for (int64_t i = 1; i < arrow_offsets.length(); ++i)
        {
            auto offset = arrow_offsets.Value(i);
            uint64_t elements = offset - previous_offset;
            previous_offset = offset;
            offsets_data.emplace_back(offsets_data.back() + elements);
        }
    }
    return offsets_column;
}

/*
 * Arrow Dictionary and ClickHouse LowCardinality types are a bit different.
 * Dictionary(Nullable(X)) in ArrowColumn format is composed of a nullmap, dictionary and an index.
 * It doesn't have the concept of null or default values.
 * An empty string is just a regular value appended at any position of the dictionary.
 * Null values have an index of 0, but it should be ignored since the nullmap will return null.
 * In ClickHouse LowCardinality, it's different. The dictionary contains null (if dictionary type is Nullable)
 * and default values at the beginning. [default, ...] when default values have index of 0 or [null, default, ...]
 * when null values have an index of 0 and default values have an index of 1.
 * So, we should remap indexes while converting Arrow Dictionary to ClickHouse LowCardinality
 * */
template <typename NumericType, typename VectorType = ColumnVector<NumericType>>
static ColumnWithTypeAndName readColumnWithIndexesDataImpl(std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name, Int64 default_value_index, NumericType dict_size, bool is_nullable)
{
    auto internal_type = std::make_shared<DataTypeNumber<NumericType>>();
    auto internal_column = internal_type->createColumn();
    auto & column_data = static_cast<VectorType &>(*internal_column).getData();
    column_data.reserve(arrow_column->length());
    NumericType shift = is_nullable ? 2 : 1;

    for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
    {
        std::shared_ptr<arrow::Array> chunk = arrow_column->chunk(chunk_i);
        if (chunk->length() == 0)
            continue;

        /// buffers[0] is a null bitmap and buffers[1] are actual values
        std::shared_ptr<arrow::Buffer> buffer = chunk->data()->buffers[1];
        const auto * data = reinterpret_cast<const NumericType *>(buffer->data()) + chunk->offset();

        /// Check that indexes are correct (protection against corrupted files)
        /// Note that on null values index can be arbitrary value.
        for (int64_t i = 0; i != chunk->length(); ++i)
        {
            if (!chunk->IsNull(i) && (data[i] < 0 || data[i] >= dict_size))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                                "Index {} in Dictionary column is out of bounds, dictionary size is {}",
                                Int64(data[i]), UInt64(dict_size));
        }

        /// If dictionary type is not nullable and arrow dictionary contains default type
        /// at 0 index, we don't need to remap anything (it's the case when this data
        /// was generated by ClickHouse)
        if (!is_nullable && default_value_index == 0)
        {
            column_data.insert_assume_reserved(data, data + chunk->length());
        }
        /// If dictionary don't contain default value, we should move all indexes
        /// to the right one or two (if dictionary is Nullable) positions
        /// Example:
        /// Dictionary:
        ///     dict: ["one", "two"]
        ///     indexes: [0, 1, 0]
        /// LowCardinality:
        ///     dict: ["", "one", "two"]
        ///     indexes: [1, 2, 1]
        /// LowCardinality(Nullable):
        ///     dict: [null, "", "one", "two"]
        ///     indexes: [2, 3, 2]
        else if (default_value_index == -1)
        {
            for (int64_t i = 0; i != chunk->length(); ++i)
            {
                if (chunk->IsNull(i))
                    column_data.push_back(0);
                else
                    column_data.push_back(data[i] + shift);
            }
        }
        /// If dictionary contains default value, we change all indexes of it to
        /// 0 or 1 (if dictionary type is Nullable) and move all indexes
        /// that are less then default value index to the right one or two
        /// (if dictionary is Nullable) position and all indexes that are
        /// greater then default value index zero or one (if dictionary is Nullable)
        /// positions.
        /// Example:
        /// Dictionary:
        ///     dict: ["one", "two", "", "three"]
        ///     indexes: [0, 1, 2, 3, 0]
        /// LowCardinality :
        ///     dict: ["", "one", "two", "three"]
        ///     indexes: [1, 2, 0, 3, 1]
        /// LowCardinality(Nullable):
        ///     dict: [null, "", "one", "two", "three"]
        ///     indexes: [2, 3, 1, 4, 2]
        else
        {
            NumericType new_default_index = is_nullable ? 1 : 0;
            NumericType default_index = NumericType(default_value_index);
            for (int64_t i = 0; i != chunk->length(); ++i)
            {
                if (chunk->IsNull(i))
                    column_data.push_back(0);
                else
                {
                    NumericType value = data[i];
                    if (value == default_index)
                        value = new_default_index;
                    else if (value < default_index)
                        value += shift;
                    else
                        value += shift - 1;
                    column_data.push_back(value);
                }
            }
        }
    }
    return {std::move(internal_column), std::move(internal_type), column_name};
}

static ColumnPtr readColumnWithIndexesData(std::shared_ptr<arrow::ChunkedArray> & arrow_column, Int64 default_value_index, UInt64 dict_size, bool is_nullable)
{
    switch (arrow_column->type()->id())
    {
#    define DISPATCH(ARROW_NUMERIC_TYPE, CPP_NUMERIC_TYPE) \
            case ARROW_NUMERIC_TYPE: \
            { \
                    return readColumnWithIndexesDataImpl<CPP_NUMERIC_TYPE>(\
                        arrow_column, "", default_value_index, static_cast<CPP_NUMERIC_TYPE>(dict_size), is_nullable).column; \
            }
        FOR_ARROW_INDEXES_TYPES(DISPATCH)
#    undef DISPATCH
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported type for indexes in LowCardinality: {}.", arrow_column->type()->name());
    }
}

template <typename ArrowListArray>
static std::shared_ptr<arrow::ChunkedArray> getNestedArrowColumn(const std::shared_ptr<arrow::ChunkedArray> & arrow_column)
{
    arrow::ArrayVector array_vector;
    array_vector.reserve(arrow_column->num_chunks());
    for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
    {
        ArrowListArray & list_chunk = dynamic_cast<ArrowListArray &>(*(arrow_column->chunk(chunk_i)));

        /*
         * It seems like arrow::ListArray::values() (nested column data) might or might not be shared across chunks.
         * Therefore, simply appending arrow::ListArray::values() could lead to duplicated data to be appended.
         * To properly handle this, arrow::ListArray::values() needs to be sliced based on the chunk offsets.
         * arrow::ListArray::Flatten does that. More info on: https://lists.apache.org/thread/rrwfb9zo2dc58dhd9rblf20xd7wmy7jm and
         * https://github.com/ClickHouse/ClickHouse/pull/43297
         * */
        auto flatten_result = list_chunk.Flatten();
        if (flatten_result.ok())
        {
            array_vector.emplace_back(flatten_result.ValueOrDie());
        }
        else
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to flatten chunk '{}' of column of type '{}' ", chunk_i, arrow_column->type()->id());
        }
    }
    return std::make_shared<arrow::ChunkedArray>(array_vector);
}

static ColumnWithTypeAndName readIPv6ColumnFromBinaryData(const std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name)
{
    size_t total_size = 0;
    for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
    {
        auto & chunk = dynamic_cast<arrow::BinaryArray &>(*(arrow_column->chunk(chunk_i)));
        const size_t chunk_length = chunk.length();

        for (size_t i = 0; i != chunk_length; ++i)
        {
            /// If at least one value size is not 16 bytes, fallback to reading String column and further cast to IPv6.
            if (!chunk.IsNull(i) && chunk.value_length(i) != sizeof(IPv6))
                return readColumnWithStringData<arrow::BinaryArray>(arrow_column, column_name);
        }
        total_size += chunk_length;
    }

    auto internal_type = std::make_shared<DataTypeIPv6>();
    auto internal_column = internal_type->createColumn();
    auto & ipv6_column = assert_cast<ColumnIPv6 &>(*internal_column);
    ipv6_column.reserve(total_size);

    for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
    {
        auto & chunk = dynamic_cast<arrow::BinaryArray &>(*(arrow_column->chunk(chunk_i)));
        for (size_t value_i = 0, length = static_cast<size_t>(chunk.length()); value_i < length; ++value_i)
        {
            if (chunk.IsNull(value_i))
                ipv6_column.insertDefault();
            else
                ipv6_column.insertData(chunk.Value(value_i).data(), chunk.Value(value_i).size());
        }
    }
    return {std::move(internal_column), std::move(internal_type), column_name};
}

static ColumnWithTypeAndName readIPv4ColumnWithInt32Data(const std::shared_ptr<arrow::ChunkedArray> & arrow_column, const String & column_name)
{
    auto internal_type = std::make_shared<DataTypeIPv4>();
    auto internal_column = internal_type->createColumn();
    auto & column_data = assert_cast<ColumnIPv4 &>(*internal_column).getData();
    column_data.reserve(arrow_column->length());

    for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
    {
        std::shared_ptr<arrow::Array> chunk = arrow_column->chunk(chunk_i);
        if (chunk->length() == 0)
            continue;

        /// buffers[0] is a null bitmap and buffers[1] are actual values
        std::shared_ptr<arrow::Buffer> buffer = chunk->data()->buffers[1];
        const auto * raw_data = reinterpret_cast<const IPv4 *>(buffer->data()) + chunk->offset();
        column_data.insert_assume_reserved(raw_data, raw_data + chunk->length());
    }
    return {std::move(internal_column), std::move(internal_type), column_name};
}

struct ReadColumnFromArrowColumnSettings
{
    std::string format_name;
    FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior;
    bool allow_arrow_null_type;
    bool skip_columns_with_unsupported_types;
    bool allow_inferring_nullable_columns;
};

static ColumnWithTypeAndName readColumnFromArrowColumn(
    const std::shared_ptr<arrow::ChunkedArray> & arrow_column,
    std::string column_name,
    std::unordered_map<String, ArrowColumnToCHColumn::DictionaryInfo> dictionary_infos,
    DataTypePtr type_hint,
    bool is_nullable_column,
    bool is_map_nested_column,
    const ReadColumnFromArrowColumnSettings & settings);

static ColumnWithTypeAndName readNonNullableColumnFromArrowColumn(
    const std::shared_ptr<arrow::ChunkedArray> & arrow_column,
    std::string column_name,
    std::unordered_map<String, ArrowColumnToCHColumn::DictionaryInfo> dictionary_infos,
    DataTypePtr type_hint,
    bool is_map_nested_column,
    const ReadColumnFromArrowColumnSettings & settings)
{
    switch (arrow_column->type()->id())
    {
        case arrow::Type::STRING:
        case arrow::Type::BINARY:
        {
            if (type_hint)
            {
                switch (type_hint->getTypeId())
                {
                    case TypeIndex::IPv6:
                        return readIPv6ColumnFromBinaryData(arrow_column, column_name);
                    /// ORC format outputs big integers as binary column, because there is no fixed binary in ORC.
                    ///
                    /// When ORC/Parquet file says the type is "byte array" or "fixed len byte array",
                    /// but the clickhouse query says to interpret the column as e.g. Int128, it
                    /// may mean one of two things:
                    ///  * The byte array is the 16 bytes of Int128, little-endian.
                    ///  * The byte array is an ASCII string containing the Int128 formatted in base 10.
                    /// There's no reliable way to distinguish these cases. We just guess: if the
                    /// byte array is variable-length, and the length is different from sizeof(type),
                    /// we parse as text, otherwise as binary.
                    case TypeIndex::Int128:
                        return readColumnWithBigNumberFromBinaryData<ColumnInt128>(arrow_column, column_name, type_hint);
                    case TypeIndex::UInt128:
                        return readColumnWithBigNumberFromBinaryData<ColumnUInt128>(arrow_column, column_name, type_hint);
                    case TypeIndex::Int256:
                        return readColumnWithBigNumberFromBinaryData<ColumnInt256>(arrow_column, column_name, type_hint);
                    case TypeIndex::UInt256:
                        return readColumnWithBigNumberFromBinaryData<ColumnUInt256>(arrow_column, column_name, type_hint);
                    /// ORC doesn't support Decimal256 as separate type. We read and write it as binary data.
                    case TypeIndex::Decimal256:
                        return readColumnWithBigNumberFromBinaryData<ColumnDecimal<Decimal256>>(arrow_column, column_name, type_hint);
                    default:
                        break;
                }
            }
            return readColumnWithStringData<arrow::BinaryArray>(arrow_column, column_name);
        }
        case arrow::Type::FIXED_SIZE_BINARY:
        {
            if (type_hint)
            {
                switch (type_hint->getTypeId())
                {
                    case TypeIndex::Int128:
                        return readColumnWithBigIntegerFromFixedBinaryData<Int128>(arrow_column, column_name, type_hint);
                    case TypeIndex::UInt128:
                        return readColumnWithBigIntegerFromFixedBinaryData<UInt128>(arrow_column, column_name, type_hint);
                    case TypeIndex::Int256:
                        return readColumnWithBigIntegerFromFixedBinaryData<Int256>(arrow_column, column_name, type_hint);
                    case TypeIndex::UInt256:
                        return readColumnWithBigIntegerFromFixedBinaryData<UInt256>(arrow_column, column_name, type_hint);
                    default:
                        break;
                }
            }

            return readColumnWithFixedStringData(arrow_column, column_name);
        }
        case arrow::Type::LARGE_BINARY:
        case arrow::Type::LARGE_STRING:
            return readColumnWithStringData<arrow::LargeBinaryArray>(arrow_column, column_name);
        case arrow::Type::BOOL:
            return readColumnWithBooleanData(arrow_column, column_name);
        case arrow::Type::DATE32:
            return readColumnWithDate32Data(arrow_column, column_name, type_hint, settings.date_time_overflow_behavior);
        case arrow::Type::DATE64:
            return readColumnWithDate64Data(arrow_column, column_name);
        // ClickHouse writes Date as arrow UINT16 and DateTime as arrow UINT32,
        // so, read UINT16 as Date and UINT32 as DateTime to perform correct conversion
        // between Date and DateTime further.
        case arrow::Type::UINT16:
        {
            auto column = readColumnWithNumericData<UInt16>(arrow_column, column_name);
            if (type_hint && (isDateOrDate32(type_hint) || isDateTime(type_hint) || isDateTime64(type_hint)))
                column.type = std::make_shared<DataTypeDate>();
            return column;
        }
        case arrow::Type::UINT32:
        {
            auto column = readColumnWithNumericData<UInt32>(arrow_column, column_name);
            if (type_hint && (isDateOrDate32(type_hint) || isDateTime(type_hint) || isDateTime64(type_hint)))
                column.type = std::make_shared<DataTypeDateTime>();
            return column;
        }
        case arrow::Type::INT32:
        {
            /// ORC format doesn't have unsigned integers and we output IPv4 as Int32.
            /// We should allow to read it back from Int32.
            if (type_hint && isIPv4(type_hint))
                return readIPv4ColumnWithInt32Data(arrow_column, column_name);
            return readColumnWithNumericData<Int32>(arrow_column, column_name);
        }
        case arrow::Type::TIMESTAMP:
            return readColumnWithTimestampData(arrow_column, column_name);
        case arrow::Type::DECIMAL128:
            return readColumnWithDecimalData<arrow::Decimal128Array>(arrow_column, column_name);
        case arrow::Type::DECIMAL256:
            return readColumnWithDecimalData<arrow::Decimal256Array>(arrow_column, column_name);
        case arrow::Type::MAP:
        {
            DataTypePtr nested_type_hint;
            DataTypePtr key_type_hint;
            if (type_hint)
            {
                const auto * map_type_hint = typeid_cast<const DataTypeMap *>(type_hint.get());
                if (map_type_hint)
                {
                    nested_type_hint = assert_cast<const DataTypeArray *>(map_type_hint->getNestedType().get())->getNestedType();
                    key_type_hint = map_type_hint->getKeyType();
                }
            }

            auto arrow_nested_column = getNestedArrowColumn<arrow::ListArray>(arrow_column);
            auto nested_column = readColumnFromArrowColumn(arrow_nested_column,
                column_name,
                dictionary_infos,
                nested_type_hint,
                false /*is_nullable_column*/,
                true /*is_map_nested_column*/,
                settings);
            if (!nested_column.column)
                return {};

            auto offsets_column = readOffsetsFromArrowListColumn<arrow::ListArray>(arrow_column);

            const auto * tuple_column = assert_cast<const ColumnTuple *>(nested_column.column.get());
            const auto * tuple_type = assert_cast<const DataTypeTuple *>(nested_column.type.get());
            auto key_column = tuple_column->getColumnPtr(0);
            auto key_type = tuple_type->getElements()[0];
            auto value_column = tuple_column->getColumnPtr(1);
            auto value_type = tuple_type->getElements()[1];

            if (key_type_hint && !key_type_hint->equals(*key_type))
            {
                /// Cast key column to target type, because it can happen
                /// that parsed type cannot be ClickHouse Map key type.
                key_column = castColumn({key_column, key_type, "key"}, key_type_hint);
                key_type = key_type_hint;
            }

            auto map_column = ColumnMap::create(key_column, value_column, offsets_column);
            auto map_type = std::make_shared<DataTypeMap>(key_type, value_type);
            return {std::move(map_column), std::move(map_type), column_name};
        }
        case arrow::Type::LIST:
        case arrow::Type::LARGE_LIST:
        {
            bool is_large_list = arrow_column->type()->id() == arrow::Type::LARGE_LIST;
            DataTypePtr nested_type_hint;
            if (type_hint)
            {
                const auto * array_type_hint = typeid_cast<const DataTypeArray *>(type_hint.get());
                if (array_type_hint)
                    nested_type_hint = array_type_hint->getNestedType();
            }

            bool is_nested_nullable_column = false;
            if (is_large_list)
            {
                auto * arrow_large_list_type = assert_cast<arrow::LargeListType *>(arrow_column->type().get());
                is_nested_nullable_column = arrow_large_list_type->value_field()->nullable();
            }
            else
            {
                auto * arrow_list_type = assert_cast<arrow::ListType *>(arrow_column->type().get());
                is_nested_nullable_column = arrow_list_type->value_field()->nullable();
            }

            auto arrow_nested_column = is_large_list ? getNestedArrowColumn<arrow::LargeListArray>(arrow_column) : getNestedArrowColumn<arrow::ListArray>(arrow_column);
            auto nested_column = readColumnFromArrowColumn(arrow_nested_column,
                column_name,
                dictionary_infos,
                nested_type_hint,
                is_nested_nullable_column,
                false /*is_map_nested_column*/,
                settings);
            if (!nested_column.column)
                return {};

            auto offsets_column = is_large_list ? readOffsetsFromArrowListColumn<arrow::LargeListArray>(arrow_column) : readOffsetsFromArrowListColumn<arrow::ListArray>(arrow_column);
            auto array_column = ColumnArray::create(nested_column.column, offsets_column);

            DataTypePtr array_type;
            /// If type hint is Nested, we should return Nested type,
            /// because we differentiate Nested and simple Array(Tuple)
            if (type_hint && isNested(type_hint))
            {
                const auto & tuple_type = assert_cast<const DataTypeTuple &>(*nested_column.type);
                array_type = createNested(tuple_type.getElements(), tuple_type.getElementNames());
            }
            else
            {
                array_type = std::make_shared<DataTypeArray>(nested_column.type);
            }
            return {std::move(array_column), array_type, column_name};
        }
        case arrow::Type::STRUCT:
        {
            auto arrow_type = arrow_column->type();
            auto * arrow_struct_type = assert_cast<arrow::StructType *>(arrow_type.get());
            std::vector<arrow::ArrayVector> nested_arrow_columns(arrow_struct_type->num_fields());
            for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
            {
                arrow::StructArray & struct_chunk = dynamic_cast<arrow::StructArray &>(*(arrow_column->chunk(chunk_i)));
                for (int i = 0; i < arrow_struct_type->num_fields(); ++i)
                    nested_arrow_columns[i].emplace_back(struct_chunk.field(i));
            }

            Columns tuple_elements;
            DataTypes tuple_types;
            std::vector<String> tuple_names;
            const auto * tuple_type_hint = type_hint ? typeid_cast<const DataTypeTuple *>(type_hint.get()) : nullptr;

            for (int i = 0; i != arrow_struct_type->num_fields(); ++i)
            {
                const auto & field = arrow_struct_type->field(i);
                const auto & field_name = field->name();

                DataTypePtr nested_type_hint;
                if (tuple_type_hint)
                {
                    if (tuple_type_hint->haveExplicitNames() && !is_map_nested_column)
                    {
                        auto pos = tuple_type_hint->tryGetPositionByName(field_name);
                        if (pos)
                            nested_type_hint = tuple_type_hint->getElement(*pos);
                    }
                    else if (size_t(i) < tuple_type_hint->getElements().size())
                        nested_type_hint = tuple_type_hint->getElement(i);
                }

                auto nested_arrow_column = std::make_shared<arrow::ChunkedArray>(nested_arrow_columns[i]);
                auto column_with_type_and_name = readColumnFromArrowColumn(nested_arrow_column,
                    field_name,
                    dictionary_infos,
                    nested_type_hint,
                    field->nullable(),
                    false /*is_map_nested_column*/,
                    settings);
                if (!column_with_type_and_name.column)
                    return {};

                tuple_elements.emplace_back(std::move(column_with_type_and_name.column));
                tuple_types.emplace_back(std::move(column_with_type_and_name.type));
                tuple_names.emplace_back(std::move(column_with_type_and_name.name));
            }

            auto tuple_column = ColumnTuple::create(std::move(tuple_elements));
            auto tuple_type = std::make_shared<DataTypeTuple>(std::move(tuple_types), std::move(tuple_names));
            return {std::move(tuple_column), std::move(tuple_type), column_name};
        }
        case arrow::Type::DICTIONARY:
        {
            auto & dict_info = dictionary_infos[column_name];
            const auto is_lc_nullable = arrow_column->null_count() > 0 || (type_hint && type_hint->isLowCardinalityNullable());

            /// Load dictionary values only once and reuse it.
            if (!dict_info.values)
            {
                arrow::ArrayVector dict_array;
                for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
                {
                    arrow::DictionaryArray & dict_chunk = dynamic_cast<arrow::DictionaryArray &>(*(arrow_column->chunk(chunk_i)));
                    dict_array.emplace_back(dict_chunk.dictionary());
                }

                auto arrow_dict_column = std::make_shared<arrow::ChunkedArray>(dict_array);
                auto dict_column = readColumnFromArrowColumn(arrow_dict_column,
                    column_name,
                    dictionary_infos,
                    nullptr /*nested_type_hint*/,
                    false /*is_nullable_column*/,
                    false /*is_map_nested_column*/,
                    settings);

                if (!dict_column.column)
                    return {};

                for (size_t i = 0; i != dict_column.column->size(); ++i)
                {
                    if (dict_column.column->isDefaultAt(i))
                    {
                        dict_info.default_value_index = i;
                        break;
                    }
                }

                auto lc_type = std::make_shared<DataTypeLowCardinality>(is_lc_nullable ? makeNullable(dict_column.type) : dict_column.type);
                auto tmp_lc_column = lc_type->createColumn();
                auto tmp_dict_column = IColumn::mutate(assert_cast<ColumnLowCardinality *>(tmp_lc_column.get())->getDictionaryPtr());
                dynamic_cast<IColumnUnique *>(tmp_dict_column.get())->uniqueInsertRangeFrom(*dict_column.column, 0, dict_column.column->size());
                dict_column.column = std::move(tmp_dict_column);
                dict_info.values = std::make_shared<ColumnWithTypeAndName>(std::move(dict_column));
                dict_info.dictionary_size = arrow_dict_column->length();
            }

            arrow::ArrayVector indexes_array;
            for (int chunk_i = 0, num_chunks = arrow_column->num_chunks(); chunk_i < num_chunks; ++chunk_i)
            {
                arrow::DictionaryArray & dict_chunk = dynamic_cast<arrow::DictionaryArray &>(*(arrow_column->chunk(chunk_i)));
                indexes_array.emplace_back(dict_chunk.indices());
            }

            auto arrow_indexes_column = std::make_shared<arrow::ChunkedArray>(indexes_array);
            auto indexes_column = readColumnWithIndexesData(arrow_indexes_column, dict_info.default_value_index, dict_info.dictionary_size, is_lc_nullable);
            auto lc_column = ColumnLowCardinality::create(dict_info.values->column, indexes_column);
            auto lc_type = std::make_shared<DataTypeLowCardinality>(is_lc_nullable ? makeNullable(dict_info.values->type) : dict_info.values->type);
            return {std::move(lc_column), std::move(lc_type), column_name};
        }
#    define DISPATCH(ARROW_NUMERIC_TYPE, CPP_NUMERIC_TYPE) \
        case ARROW_NUMERIC_TYPE: \
            return readColumnWithNumericData<CPP_NUMERIC_TYPE>(arrow_column, column_name);
        FOR_ARROW_NUMERIC_TYPES(DISPATCH)
#    undef DISPATCH
        case arrow::Type::TIME32:
        {
            return readColumnWithTime32Data(arrow_column, column_name);
        }
        case arrow::Type::TIME64:
        {
            return readColumnWithTime64Data(arrow_column, column_name);
        }
            // TODO: read JSON as a string?
            // TODO: read UUID as a string?
        case arrow::Type::NA:
        {
            if (settings.allow_arrow_null_type)
            {
                auto type = std::make_shared<DataTypeNothing>();
                auto column = ColumnNothing::create(arrow_column->length());
                return {std::move(column), type, column_name};
            }
            [[fallthrough]];
        }
        default:
        {
            if (settings.skip_columns_with_unsupported_types)
                return {};

            throw Exception(
                            ErrorCodes::UNKNOWN_TYPE,
                            "Unsupported {} type '{}' of an input column '{}'. "
                            "If it happens during schema inference and you want to skip columns with "
                            "unsupported types, you can enable setting input_format_{}"
                            "_skip_columns_with_unsupported_types_in_schema_inference",
                            settings.format_name,
                            arrow_column->type()->name(),
                            column_name,
                            boost::algorithm::to_lower_copy(settings.format_name));
        }
    }
}

static ColumnWithTypeAndName readColumnFromArrowColumn(
    const std::shared_ptr<arrow::ChunkedArray> & arrow_column,
    std::string column_name,
    std::unordered_map<String, ArrowColumnToCHColumn::DictionaryInfo> dictionary_infos,
    DataTypePtr type_hint,
    bool is_nullable_column,
    bool is_map_nested_column,
    const ReadColumnFromArrowColumnSettings & settings)
{
    bool read_as_nullable_column = (arrow_column->null_count() || is_nullable_column || (type_hint && type_hint->isNullable())) && settings.allow_inferring_nullable_columns;
    if (read_as_nullable_column &&
        arrow_column->type()->id() != arrow::Type::LIST &&
        arrow_column->type()->id() != arrow::Type::LARGE_LIST &&
        arrow_column->type()->id() != arrow::Type::MAP &&
        arrow_column->type()->id() != arrow::Type::STRUCT &&
        arrow_column->type()->id() != arrow::Type::DICTIONARY)
    {
        DataTypePtr nested_type_hint;
        if (type_hint)
            nested_type_hint = removeNullable(type_hint);

        auto nested_column = readNonNullableColumnFromArrowColumn(arrow_column,
            column_name,
            dictionary_infos,
            nested_type_hint,
            is_map_nested_column,
            settings);

        if (!nested_column.column)
            return {};

        auto nullmap_column = readByteMapFromArrowColumn(arrow_column);
        auto nullable_type = std::make_shared<DataTypeNullable>(std::move(nested_column.type));
        auto nullable_column = ColumnNullable::create(nested_column.column, nullmap_column);

        return {std::move(nullable_column), std::move(nullable_type), column_name};
    }

    return readNonNullableColumnFromArrowColumn(arrow_column,
        column_name,
        dictionary_infos,
        type_hint,
        is_map_nested_column,
        settings);
}

// Creating CH header by arrow schema. Will be useful in task about inserting
// data from file without knowing table structure.

static void checkStatus(const arrow::Status & status, const String & column_name, const String & format_name)
{
    if (!status.ok())
        throw Exception{ErrorCodes::UNKNOWN_EXCEPTION, "Error with a {} column '{}': {}.", format_name, column_name, status.ToString()};
}

/// Create empty arrow column using specified field
static std::shared_ptr<arrow::ChunkedArray> createArrowColumn(const std::shared_ptr<arrow::Field> & field, const String & format_name)
{
    arrow::MemoryPool * pool = ArrowMemoryPool::instance();
    std::unique_ptr<arrow::ArrayBuilder> array_builder;
    arrow::Status status = MakeBuilder(pool, field->type(), &array_builder);
    checkStatus(status, field->name(), format_name);

    std::shared_ptr<arrow::Array> arrow_array;
    status = array_builder->Finish(&arrow_array);
    checkStatus(status, field->name(), format_name);

    return std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{arrow_array});
}

Block ArrowColumnToCHColumn::arrowSchemaToCHHeader(
    const arrow::Schema & schema,
    const std::string & format_name,
    bool skip_columns_with_unsupported_types,
    bool allow_inferring_nullable_columns)
{
    ReadColumnFromArrowColumnSettings settings
    {
        .format_name = format_name,
        .date_time_overflow_behavior = FormatSettings::DateTimeOverflowBehavior::Ignore,
        .allow_arrow_null_type = false,
        .skip_columns_with_unsupported_types = skip_columns_with_unsupported_types,
        .allow_inferring_nullable_columns = allow_inferring_nullable_columns,
    };

    ColumnsWithTypeAndName sample_columns;

    for (const auto & field : schema.fields())
    {
        /// Create empty arrow column by it's type and convert it to ClickHouse column.
        auto arrow_column = createArrowColumn(field, format_name);

        std::unordered_map<std::string, DictionaryInfo> dict_infos;

        auto sample_column = readColumnFromArrowColumn(
            arrow_column,
            field->name(),
            dict_infos,
            nullptr /*nested_type_hint*/,
            field->nullable() /*is_nullable_column*/,
            false /*is_map_nested_column*/,
            settings);

        if (sample_column.column)
            sample_columns.emplace_back(std::move(sample_column));
    }

    return Block(std::move(sample_columns));
}

ArrowColumnToCHColumn::ArrowColumnToCHColumn(
    const Block & header_,
    const std::string & format_name_,
    bool allow_missing_columns_,
    bool null_as_default_,
    FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior_,
    bool case_insensitive_matching_,
    bool is_stream_)
    : header(header_)
    , format_name(format_name_)
    , allow_missing_columns(allow_missing_columns_)
    , null_as_default(null_as_default_)
    , date_time_overflow_behavior(date_time_overflow_behavior_)
    , case_insensitive_matching(case_insensitive_matching_)
    , is_stream(is_stream_)
{
}

Chunk ArrowColumnToCHColumn::arrowTableToCHChunk(const std::shared_ptr<arrow::Table> & table, size_t num_rows, BlockMissingValues * block_missing_values)
{
    NameToArrowColumn name_to_arrow_column;

    for (auto column_name : table->ColumnNames())
    {
        auto arrow_column = table->GetColumnByName(column_name);
        if (!arrow_column)
            throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Column '{}' is duplicated", column_name);

        auto arrow_field = table->schema()->GetFieldByName(column_name);

        if (case_insensitive_matching)
            boost::to_lower(column_name);

        name_to_arrow_column[std::move(column_name)] = {std::move(arrow_column), std::move(arrow_field)};
    }

    return arrowColumnsToCHChunk(name_to_arrow_column, num_rows, block_missing_values);
}

Chunk ArrowColumnToCHColumn::arrowColumnsToCHChunk(const NameToArrowColumn & name_to_arrow_column, size_t num_rows, BlockMissingValues * block_missing_values)
{
    ReadColumnFromArrowColumnSettings settings
    {
        .format_name = format_name,
        .date_time_overflow_behavior = date_time_overflow_behavior,
        .allow_arrow_null_type = true,
        .skip_columns_with_unsupported_types = false,
        .allow_inferring_nullable_columns = true
    };

    Columns columns;
    columns.reserve(header.columns());

    std::unordered_map<String, std::pair<BlockPtr, std::shared_ptr<NestedColumnExtractHelper>>> nested_tables;

    for (size_t column_i = 0, header_columns = header.columns(); column_i < header_columns; ++column_i)
    {
        const ColumnWithTypeAndName & header_column = header.getByPosition(column_i);

        auto search_column_name = header_column.name;
        if (case_insensitive_matching)
            boost::to_lower(search_column_name);

        ColumnWithTypeAndName column;
        if (!name_to_arrow_column.contains(search_column_name))
        {
            bool read_from_nested = false;

            /// Check if it's a subcolumn from some struct.
            String nested_table_name = Nested::extractTableName(header_column.name);
            String search_nested_table_name = nested_table_name;
            if (case_insensitive_matching)
                boost::to_lower(search_nested_table_name);

            if (name_to_arrow_column.contains(search_nested_table_name))
            {
                if (!nested_tables.contains(search_nested_table_name))
                {
                    NamesAndTypesList nested_columns;
                    for (const auto & name_and_type : header.getNamesAndTypesList())
                    {
                        if (name_and_type.name.starts_with(nested_table_name + "."))
                            nested_columns.push_back(name_and_type);
                    }
                    auto nested_table_type = Nested::collect(nested_columns).front().type;

                    const auto & arrow_column = name_to_arrow_column.find(search_nested_table_name)->second;

                    ColumnsWithTypeAndName cols =
                    {
                        readColumnFromArrowColumn(arrow_column.column,
                            nested_table_name,
                            dictionary_infos,
                            nested_table_type,
                            arrow_column.field->nullable() /*is_nullable_column*/,
                            false /*is_map_nested_column*/,
                            settings)
                    };

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

            if (!read_from_nested)
            {
                if (!allow_missing_columns)
                    throw Exception{ErrorCodes::THERE_IS_NO_COLUMN, "Column '{}' is not presented in input data.", header_column.name};

                column.name = header_column.name;
                column.type = header_column.type;
                column.column = header_column.column->cloneResized(num_rows);
                columns.push_back(std::move(column.column));
                if (block_missing_values)
                    block_missing_values->setBits(column_i, num_rows);
                continue;
            }
        }
        else
        {
            const auto & arrow_column = name_to_arrow_column.find(search_column_name)->second;
            column = readColumnFromArrowColumn(arrow_column.column,
                header_column.name,
                dictionary_infos,
                header_column.type,
                arrow_column.field->nullable(),
                false /*is_map_nested_column*/,
                settings);
        }

        if (null_as_default)
            insertNullAsDefaultIfNeeded(column, header_column, column_i, block_missing_values);

        /// In ArrowStream batches can have different dictionaries.
        if (is_stream)
            dictionary_infos.clear();

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
        columns.push_back(std::move(column.column));
    }

    return Chunk(std::move(columns), num_rows);
}

}

#endif
