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
#include <Interpreters/castColumn.h>
#include <Common/quoteString.h>
#include <algorithm>
#include <arrow/builder.h>
#include <arrow/array.h>

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

static ColumnWithTypeAndName readColumnFromArrowColumn(
    std::shared_ptr<arrow::ChunkedArray> & arrow_column,
    const std::string & column_name,
    const std::string & format_name,
    bool is_nullable,
    std::unordered_map<String, std::shared_ptr<ColumnWithTypeAndName>> & dictionary_values,
    bool read_ints_as_dates)
{
    if (!is_nullable && arrow_column->null_count() && arrow_column->type()->id() != arrow::Type::LIST
        && arrow_column->type()->id() != arrow::Type::MAP && arrow_column->type()->id() != arrow::Type::STRUCT)
    {
        auto nested_column = readColumnFromArrowColumn(arrow_column, column_name, format_name, true, dictionary_values, read_ints_as_dates);
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
            auto nested_column = readColumnFromArrowColumn(arrow_nested_column, column_name, format_name, false, dictionary_values, read_ints_as_dates);
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
            auto nested_column = readColumnFromArrowColumn(arrow_nested_column, column_name, format_name, false, dictionary_values, read_ints_as_dates);
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
                auto element = readColumnFromArrowColumn(nested_arrow_column, arrow_struct_type->field(i)->name(), format_name, false, dictionary_values, read_ints_as_dates);
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
                auto dict_column = readColumnFromArrowColumn(arrow_dict_column, column_name, format_name, false, dictionary_values, read_ints_as_dates);

                /// We should convert read column to ColumnUnique.
                auto tmp_lc_column = DataTypeLowCardinality(dict_column.type).createColumn();
                auto tmp_dict_column = IColumn::mutate(assert_cast<ColumnLowCardinality *>(tmp_lc_column.get())->getDictionaryPtr());
                static_cast<IColumnUnique *>(tmp_dict_column.get())->uniqueInsertRangeFrom(*dict_column.column, 0, dict_column.column->size());
                dict_column.column = std::move(tmp_dict_column);
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
            auto lc_column = ColumnLowCardinality::create(dict_values->column, indexes_column);
            auto lc_type = std::make_shared<DataTypeLowCardinality>(dict_values->type);
            return {std::move(lc_column), std::move(lc_type), column_name};
        }
#    define DISPATCH(ARROW_NUMERIC_TYPE, CPP_NUMERIC_TYPE) \
        case ARROW_NUMERIC_TYPE: \
            return readColumnWithNumericData<CPP_NUMERIC_TYPE>(arrow_column, column_name);
        FOR_ARROW_NUMERIC_TYPES(DISPATCH)
#    undef DISPATCH
            // TODO: read JSON as a string?
            // TODO: read UUID as a string?
        default:
            throw Exception(ErrorCodes::UNKNOWN_TYPE,
                    "Unsupported {} type '{}' of an input column '{}'.", format_name, arrow_column->type()->name(), column_name);
    }
}


// Creating CH header by arrow schema. Will be useful in task about inserting
// data from file without knowing table structure.

static void checkStatus(const arrow::Status & status, const String & column_name, const String & format_name)
{
    if (!status.ok())
        throw Exception{ErrorCodes::UNKNOWN_EXCEPTION, "Error with a {} column '{}': {}.", format_name, column_name, status.ToString()};
}

Block ArrowColumnToCHColumn::arrowSchemaToCHHeader(const arrow::Schema & schema, const std::string & format_name)
{
    ColumnsWithTypeAndName sample_columns;
    for (const auto & field : schema.fields())
    {
        /// Create empty arrow column by it's type and convert it to ClickHouse column.
        arrow::MemoryPool* pool = arrow::default_memory_pool();
        std::unique_ptr<arrow::ArrayBuilder> array_builder;
        arrow::Status status = MakeBuilder(pool, field->type(), &array_builder);
        checkStatus(status, field->name(), format_name);

        std::shared_ptr<arrow::Array> arrow_array;
        status = array_builder->Finish(&arrow_array);
        checkStatus(status, field->name(), format_name);

        arrow::ArrayVector array_vector = {arrow_array};
        auto arrow_column = std::make_shared<arrow::ChunkedArray>(array_vector);
        std::unordered_map<std::string, std::shared_ptr<ColumnWithTypeAndName>> dict_values;
        ColumnWithTypeAndName sample_column = readColumnFromArrowColumn(arrow_column, field->name(), format_name, false, dict_values, false);

        sample_columns.emplace_back(std::move(sample_column));
    }
    return Block(std::move(sample_columns));
}

ArrowColumnToCHColumn::ArrowColumnToCHColumn(
    const Block & header_, const std::string & format_name_, bool import_nested_, bool allow_missing_columns_)
    : header(header_), format_name(format_name_), import_nested(import_nested_), allow_missing_columns(allow_missing_columns_)
{
}

void ArrowColumnToCHColumn::arrowTableToCHChunk(Chunk & res, std::shared_ptr<arrow::Table> & table)
{
    NameToColumnPtr name_to_column_ptr;
    for (const auto & column_name : table->ColumnNames())
    {
        std::shared_ptr<arrow::ChunkedArray> arrow_column = table->GetColumnByName(column_name);
        if (!arrow_column)
            throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Column '{}' is duplicated", column_name);
        name_to_column_ptr[column_name] = arrow_column;
    }

    arrowColumnsToCHChunk(res, name_to_column_ptr);
}

void ArrowColumnToCHColumn::arrowColumnsToCHChunk(Chunk & res, NameToColumnPtr & name_to_column_ptr)
{
    if (unlikely(name_to_column_ptr.empty()))
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Columns is empty");

    Columns columns_list;
    UInt64 num_rows = name_to_column_ptr.begin()->second->length();
    columns_list.reserve(header.rows());
    std::unordered_map<String, BlockPtr> nested_tables;
    for (size_t column_i = 0, columns = header.columns(); column_i < columns; ++column_i)
    {
        const ColumnWithTypeAndName & header_column = header.getByPosition(column_i);

        bool read_from_nested = false;
        String nested_table_name = Nested::extractTableName(header_column.name);
        if (!name_to_column_ptr.contains(header_column.name))
        {
            /// Check if it's a column from nested table.
            if (import_nested && name_to_column_ptr.contains(nested_table_name))
            {
                if (!nested_tables.contains(nested_table_name))
                {
                    std::shared_ptr<arrow::ChunkedArray> arrow_column = name_to_column_ptr[nested_table_name];
                    ColumnsWithTypeAndName cols = {readColumnFromArrowColumn(arrow_column, nested_table_name, format_name, false, dictionary_values, true)};
                    Block block(cols);
                    nested_tables[nested_table_name] = std::make_shared<Block>(Nested::flatten(block));
                }

                read_from_nested = nested_tables[nested_table_name]->has(header_column.name);
            }

            if (!read_from_nested)
            {
                if (!allow_missing_columns)
                    throw Exception{ErrorCodes::THERE_IS_NO_COLUMN, "Column '{}' is not presented in input data.", header_column.name};

                ColumnWithTypeAndName column;
                column.name = header_column.name;
                column.type = header_column.type;
                column.column = header_column.column->cloneResized(num_rows);
                columns_list.push_back(std::move(column.column));
                continue;
            }
        }

        std::shared_ptr<arrow::ChunkedArray> arrow_column = name_to_column_ptr[header_column.name];

        ColumnWithTypeAndName column;
        if (read_from_nested)
            column = nested_tables[nested_table_name]->getByName(header_column.name);
        else
            column = readColumnFromArrowColumn(arrow_column, header_column.name, format_name, false, dictionary_values, true);

        try
        {
            column.column = castColumn(column, header_column.type);
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format("while converting column {} from type {} to type {}",
                backQuote(header_column.name), column.type->getName(), header_column.type->getName()));
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
    auto block_from_arrow = arrowSchemaToCHHeader(schema, format_name);
    auto flatten_block_from_arrow = Nested::flatten(block_from_arrow);
    for (size_t i = 0, columns = header.columns(); i < columns; ++i)
    {
        const auto & column = header.getByPosition(i);
        bool read_from_nested = false;
        String nested_table_name = Nested::extractTableName(column.name);
        if (!block_from_arrow.has(column.name))
        {
            if (import_nested && block_from_arrow.has(nested_table_name))
                read_from_nested = flatten_block_from_arrow.has(column.name);

            if (!read_from_nested)
            {
                if (!allow_missing_columns)
                    throw Exception{ErrorCodes::THERE_IS_NO_COLUMN, "Column '{}' is not presented in input data.", column.name};

                missing_columns.push_back(i);
            }
        }
    }
    return missing_columns;
}

}

#endif
