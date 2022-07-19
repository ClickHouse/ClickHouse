#include "config_formats.h"
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
#include <common/DateLUTImpl.h>
#include <common/types.h>
#include <Core/Block.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnUnique.h>
#include <Columns/ColumnMap.h>
#include <Interpreters/castColumn.h>
#include <algorithm>
#include <fmt/format.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int UNKNOWN_TYPE;
        extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
        extern const int CANNOT_CONVERT_TYPE;
        extern const int CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN;
        extern const int THERE_IS_NO_COLUMN;
        extern const int BAD_ARGUMENTS;
    }

    static const std::initializer_list<std::pair<arrow::Type::type, const char *>> arrow_type_to_internal_type =
    {
            {arrow::Type::UINT8, "UInt8"},
            {arrow::Type::INT8, "Int8"},
            {arrow::Type::UINT16, "UInt16"},
            {arrow::Type::INT16, "Int16"},
            {arrow::Type::UINT32, "UInt32"},
            {arrow::Type::INT32, "Int32"},
            {arrow::Type::UINT64, "UInt64"},
            {arrow::Type::INT64, "Int64"},
            {arrow::Type::HALF_FLOAT, "Float32"},
            {arrow::Type::FLOAT, "Float32"},
            {arrow::Type::DOUBLE, "Float64"},

            {arrow::Type::BOOL, "UInt8"},
            {arrow::Type::DATE32, "Date"},
            {arrow::Type::DATE64, "DateTime"},
            {arrow::Type::TIMESTAMP, "DateTime"},

            {arrow::Type::STRING, "String"},
            {arrow::Type::BINARY, "String"},

            // TODO: add other types that are convertible to internal ones:
            // 0. ENUM?
            // 1. UUID -> String
            // 2. JSON -> String
            // Full list of types: contrib/arrow/cpp/src/arrow/type.h
    };

/// Inserts numeric data right into internal column data to reduce an overhead
    template <typename NumericType, typename VectorType = ColumnVector<NumericType>>
    static void fillColumnWithNumericData(std::shared_ptr<arrow::ChunkedArray> & arrow_column, IColumn & internal_column)
    {
        auto & column_data = static_cast<VectorType &>(internal_column).getData();
        column_data.reserve(arrow_column->length());

        for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
        {
            std::shared_ptr<arrow::Array> chunk = arrow_column->chunk(chunk_i);
            /// buffers[0] is a null bitmap and buffers[1] are actual values
            std::shared_ptr<arrow::Buffer> buffer = chunk->data()->buffers[1];

            const auto * raw_data = reinterpret_cast<const NumericType *>(buffer->data());
            column_data.insert_assume_reserved(raw_data, raw_data + chunk->length());
        }
    }

/// Inserts chars and offsets right into internal column data to reduce an overhead.
/// Internal offsets are shifted by one to the right in comparison with Arrow ones. So the last offset should map to the end of all chars.
/// Also internal strings are null terminated.
    static void fillColumnWithStringData(std::shared_ptr<arrow::ChunkedArray> & arrow_column, IColumn & internal_column)
    {
        PaddedPODArray<UInt8> & column_chars_t = assert_cast<ColumnString &>(internal_column).getChars();
        PaddedPODArray<UInt64> & column_offsets = assert_cast<ColumnString &>(internal_column).getOffsets();

        size_t chars_t_size = 0;
        for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
        {
            arrow::BinaryArray & chunk = static_cast<arrow::BinaryArray &>(*(arrow_column->chunk(chunk_i)));
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
            arrow::BinaryArray & chunk = static_cast<arrow::BinaryArray &>(*(arrow_column->chunk(chunk_i)));
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
    }

    static void fillColumnWithBooleanData(std::shared_ptr<arrow::ChunkedArray> & arrow_column, IColumn & internal_column)
    {
        auto & column_data = assert_cast<ColumnVector<UInt8> &>(internal_column).getData();
        column_data.reserve(arrow_column->length());

        for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
        {
            arrow::BooleanArray & chunk = static_cast<arrow::BooleanArray &>(*(arrow_column->chunk(chunk_i)));
            /// buffers[0] is a null bitmap and buffers[1] are actual values
            std::shared_ptr<arrow::Buffer> buffer = chunk.data()->buffers[1];

            for (size_t bool_i = 0; bool_i != static_cast<size_t>(chunk.length()); ++bool_i)
                column_data.emplace_back(chunk.Value(bool_i));
        }
    }

/// Arrow stores Parquet::DATE in Int32, while ClickHouse stores Date in UInt16. Therefore, it should be checked before saving
    static void fillColumnWithDate32Data(std::shared_ptr<arrow::ChunkedArray> & arrow_column, IColumn & internal_column)
    {
        PaddedPODArray<UInt16> & column_data = assert_cast<ColumnVector<UInt16> &>(internal_column).getData();
        column_data.reserve(arrow_column->length());

        for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
        {
            arrow::Date32Array & chunk = static_cast<arrow::Date32Array &>(*(arrow_column->chunk(chunk_i)));

            for (size_t value_i = 0, length = static_cast<size_t>(chunk.length()); value_i < length; ++value_i)
            {
                UInt32 days_num = static_cast<UInt32>(chunk.Value(value_i));
                if (days_num > DATE_LUT_MAX_DAY_NUM)
                {
                    // TODO: will it rollback correctly?
                    throw Exception
                        {
                            fmt::format("Input value {} of a column \"{}\" is greater than max allowed Date value, which is {}", days_num, internal_column.getName(), DATE_LUT_MAX_DAY_NUM),
                            ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE
                        };
                }

                column_data.emplace_back(days_num);
            }
        }
    }

/// Arrow stores Parquet::DATETIME in Int64, while ClickHouse stores DateTime in UInt32. Therefore, it should be checked before saving
    static void fillColumnWithDate64Data(std::shared_ptr<arrow::ChunkedArray> & arrow_column, IColumn & internal_column)
    {
        auto & column_data = assert_cast<ColumnVector<UInt32> &>(internal_column).getData();
        column_data.reserve(arrow_column->length());

        for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
        {
            auto & chunk = static_cast<arrow::Date64Array &>(*(arrow_column->chunk(chunk_i)));
            for (size_t value_i = 0, length = static_cast<size_t>(chunk.length()); value_i < length; ++value_i)
            {
                auto timestamp = static_cast<UInt32>(chunk.Value(value_i) / 1000); // Always? in ms
                column_data.emplace_back(timestamp);
            }
        }
    }

    static void fillColumnWithTimestampData(std::shared_ptr<arrow::ChunkedArray> & arrow_column, IColumn & internal_column)
    {
        auto & column_data = assert_cast<ColumnVector<UInt32> &>(internal_column).getData();
        column_data.reserve(arrow_column->length());

        for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
        {
            auto & chunk = static_cast<arrow::TimestampArray &>(*(arrow_column->chunk(chunk_i)));
            const auto & type = static_cast<const ::arrow::TimestampType &>(*chunk.type());

            UInt32 divide = 1;
            const auto unit = type.unit();
            switch (unit)
            {
                case arrow::TimeUnit::SECOND:
                    divide = 1;
                    break;
                case arrow::TimeUnit::MILLI:
                    divide = 1000;
                    break;
                case arrow::TimeUnit::MICRO:
                    divide = 1000000;
                    break;
                case arrow::TimeUnit::NANO:
                    divide = 1000000000;
                    break;
            }

            for (size_t value_i = 0, length = static_cast<size_t>(chunk.length()); value_i < length; ++value_i)
            {
                auto timestamp = static_cast<UInt32>(chunk.Value(value_i) / divide); // ms! TODO: check other 's' 'ns' ...
                column_data.emplace_back(timestamp);
            }
        }
    }

    template <typename DecimalType, typename DecimalArray>
    static void fillColumnWithDecimalData(std::shared_ptr<arrow::ChunkedArray> & arrow_column, IColumn & internal_column)
    {
        auto & column = assert_cast<ColumnDecimal<DecimalType> &>(internal_column);
        auto & column_data = column.getData();
        column_data.reserve(arrow_column->length());

        for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
        {
            auto & chunk = static_cast<DecimalArray &>(*(arrow_column->chunk(chunk_i)));
            for (size_t value_i = 0, length = static_cast<size_t>(chunk.length()); value_i < length; ++value_i)
            {
                column_data.emplace_back(chunk.IsNull(value_i) ? DecimalType(0) : *reinterpret_cast<const DecimalType *>(chunk.Value(value_i))); // TODO: copy column
            }
        }
    }

/// Creates a null bytemap from arrow's null bitmap
    static void fillByteMapFromArrowColumn(std::shared_ptr<arrow::ChunkedArray> & arrow_column, IColumn & bytemap)
    {
        PaddedPODArray<UInt8> & bytemap_data = assert_cast<ColumnVector<UInt8> &>(bytemap).getData();
        bytemap_data.reserve(arrow_column->length());

        for (size_t chunk_i = 0; chunk_i != static_cast<size_t>(arrow_column->num_chunks()); ++chunk_i)
        {
            std::shared_ptr<arrow::Array> chunk = arrow_column->chunk(chunk_i);

            for (size_t value_i = 0; value_i != static_cast<size_t>(chunk->length()); ++value_i)
                bytemap_data.emplace_back(chunk->IsNull(value_i));
        }
    }

    static void fillOffsetsFromArrowListColumn(std::shared_ptr<arrow::ChunkedArray> & arrow_column, IColumn & offsets)
    {
        ColumnArray::Offsets & offsets_data = assert_cast<ColumnVector<UInt64> &>(offsets).getData();
        offsets_data.reserve(arrow_column->length());

        for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
        {
            arrow::ListArray & list_chunk = static_cast<arrow::ListArray &>(*(arrow_column->chunk(chunk_i)));
            auto arrow_offsets_array = list_chunk.offsets();
            auto & arrow_offsets = static_cast<arrow::Int32Array &>(*arrow_offsets_array);
            auto start = offsets_data.back();
            for (int64_t i = 1; i < arrow_offsets.length(); ++i)
                offsets_data.emplace_back(start + arrow_offsets.Value(i));
        }
    }
    static ColumnPtr createAndFillColumnWithIndexesData(std::shared_ptr<arrow::ChunkedArray> & arrow_column)
    {
        switch (arrow_column->type()->id())
        {
#    define DISPATCH(ARROW_NUMERIC_TYPE, CPP_NUMERIC_TYPE) \
            case ARROW_NUMERIC_TYPE: \
            { \
                    auto column = DataTypeNumber<CPP_NUMERIC_TYPE>().createColumn(); \
                    fillColumnWithNumericData<CPP_NUMERIC_TYPE>(arrow_column, *column); \
                    return column; \
            }
            FOR_ARROW_INDEXES_TYPES(DISPATCH)
#    undef DISPATCH
            default:
                throw Exception(fmt::format("Unsupported type for indexes in LowCardinality: {}.", arrow_column->type()->name()), ErrorCodes::BAD_ARGUMENTS);
        }
    }

    static void readColumnFromArrowColumn(
        std::shared_ptr<arrow::ChunkedArray> & arrow_column,
        IColumn & internal_column,
        const std::string & column_name,
        const std::string & format_name,
        bool is_nullable,
        std::unordered_map<String, ColumnPtr> dictionary_values)
    {
        if (internal_column.isNullable())
        {
            ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(internal_column);
            readColumnFromArrowColumn(arrow_column, column_nullable.getNestedColumn(), column_name, format_name, true, dictionary_values);
            fillByteMapFromArrowColumn(arrow_column, column_nullable.getNullMapColumn());
            return;
        }

        /// TODO: check if a column is const?
        if (!is_nullable && arrow_column->null_count() && arrow_column->type()->id() != arrow::Type::LIST
            && arrow_column->type()->id() != arrow::Type::MAP && arrow_column->type()->id() != arrow::Type::STRUCT)
        {
            throw Exception
                {
                    fmt::format("Can not insert NULL data into non-nullable column \"{}\".", column_name),
                    ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN
                };
        }

        switch (arrow_column->type()->id())
        {
            case arrow::Type::STRING:
            case arrow::Type::BINARY:
                //case arrow::Type::FIXED_SIZE_BINARY:
                fillColumnWithStringData(arrow_column, internal_column);
                break;
            case arrow::Type::BOOL:
                fillColumnWithBooleanData(arrow_column, internal_column);
                break;
            case arrow::Type::DATE32:
                fillColumnWithDate32Data(arrow_column, internal_column);
                break;
            case arrow::Type::DATE64:
                fillColumnWithDate64Data(arrow_column, internal_column);
                break;
            case arrow::Type::TIMESTAMP:
                fillColumnWithTimestampData(arrow_column, internal_column);
                break;
            case arrow::Type::DECIMAL128:
                fillColumnWithDecimalData<Decimal128, arrow::Decimal128Array>(arrow_column, internal_column /*, internal_nested_type*/);
                break;
            case arrow::Type::DECIMAL256:
                fillColumnWithDecimalData<Decimal256, arrow::Decimal256Array>(arrow_column, internal_column /*, internal_nested_type*/);
                break;
            case arrow::Type::MAP: [[fallthrough]];
            case arrow::Type::LIST:
            {
                arrow::ArrayVector array_vector;
                array_vector.reserve(arrow_column->num_chunks());
                for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
                {
                    arrow::ListArray & list_chunk = static_cast<arrow::ListArray &>(*(arrow_column->chunk(chunk_i)));
                    std::shared_ptr<arrow::Array> chunk = list_chunk.values();
                    array_vector.emplace_back(std::move(chunk));
                }
                auto arrow_nested_column = std::make_shared<arrow::ChunkedArray>(array_vector);

                ColumnArray & column_array = arrow_column->type()->id() == arrow::Type::MAP
                    ? assert_cast<ColumnMap &>(internal_column).getNestedColumn()
                    : assert_cast<ColumnArray &>(internal_column);

                readColumnFromArrowColumn(arrow_nested_column, column_array.getData(), column_name, format_name, false, dictionary_values);
                fillOffsetsFromArrowListColumn(arrow_column, column_array.getOffsetsColumn());
                break;
            }
            case arrow::Type::STRUCT:
            {
                ColumnTuple & column_tuple = assert_cast<ColumnTuple &>(internal_column);
                int fields_count = column_tuple.tupleSize();
                std::vector<arrow::ArrayVector> nested_arrow_columns(fields_count);
                for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
                {
                    arrow::StructArray & struct_chunk = static_cast<arrow::StructArray &>(*(arrow_column->chunk(chunk_i)));
                    for (int i = 0; i < fields_count; ++i)
                        nested_arrow_columns[i].emplace_back(struct_chunk.field(i));
                }

                for (int i = 0; i != fields_count; ++i)
                {
                    auto nested_arrow_column = std::make_shared<arrow::ChunkedArray>(nested_arrow_columns[i]);
                    readColumnFromArrowColumn(nested_arrow_column, column_tuple.getColumn(i), column_name, format_name, false, dictionary_values);
                }
                break;
            }
            case arrow::Type::DICTIONARY:
            {
                ColumnLowCardinality & column_lc = assert_cast<ColumnLowCardinality &>(internal_column);
                auto & dict_values = dictionary_values[column_name];
                /// Load dictionary values only once and reuse it.
                if (!dict_values)
                {
                    arrow::ArrayVector dict_array;
                    for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
                    {
                        arrow::DictionaryArray & dict_chunk = static_cast<arrow::DictionaryArray &>(*(arrow_column->chunk(chunk_i)));
                        dict_array.emplace_back(dict_chunk.dictionary());
                    }
                    auto arrow_dict_column = std::make_shared<arrow::ChunkedArray>(dict_array);

                    auto dict_column = IColumn::mutate(column_lc.getDictionaryPtr());
                    auto * uniq_column = static_cast<IColumnUnique *>(dict_column.get());
                    auto values_column = uniq_column->getNestedColumn()->cloneEmpty();
                    readColumnFromArrowColumn(arrow_dict_column, *values_column, column_name, format_name, false, dictionary_values);
                    uniq_column->uniqueInsertRangeFrom(*values_column, 0, values_column->size());
                    dict_values = std::move(dict_column);
                }

                arrow::ArrayVector indexes_array;
                for (size_t chunk_i = 0, num_chunks = static_cast<size_t>(arrow_column->num_chunks()); chunk_i < num_chunks; ++chunk_i)
                {
                    arrow::DictionaryArray & dict_chunk = static_cast<arrow::DictionaryArray &>(*(arrow_column->chunk(chunk_i)));
                    indexes_array.emplace_back(dict_chunk.indices());
                }

                auto arrow_indexes_column = std::make_shared<arrow::ChunkedArray>(indexes_array);
                auto indexes_column = createAndFillColumnWithIndexesData(arrow_indexes_column);

                auto new_column_lc = ColumnLowCardinality::create(dict_values, std::move(indexes_column));
                column_lc = std::move(*new_column_lc);
                break;
            }
#    define DISPATCH(ARROW_NUMERIC_TYPE, CPP_NUMERIC_TYPE) \
        case ARROW_NUMERIC_TYPE: \
            fillColumnWithNumericData<CPP_NUMERIC_TYPE>(arrow_column, internal_column); \
            break;

            FOR_ARROW_NUMERIC_TYPES(DISPATCH)
#    undef DISPATCH
                // TODO: support TIMESTAMP_MICROS and TIMESTAMP_MILLIS with truncated micro- and milliseconds?
                // TODO: read JSON as a string?
                // TODO: read UUID as a string?
            default:
                throw Exception
                    {
                        fmt::format(R"(Unsupported {} type "{}" of an input column "{}".)", format_name, arrow_column->type()->name(), column_name),
                        ErrorCodes::UNKNOWN_TYPE
                    };
        }
    }

    static DataTypePtr getInternalType(std::shared_ptr<arrow::DataType> arrow_type, const DataTypePtr & column_type, const std::string & column_name, const std::string & format_name)
    {
        if (column_type->isNullable())
        {
            DataTypePtr nested_type = assert_cast<const DataTypeNullable *>(column_type.get())->getNestedType();
            return makeNullable(getInternalType(arrow_type, nested_type, column_name, format_name));
        }

        if (arrow_type->id() == arrow::Type::DECIMAL128)
        {
            const auto * decimal_type = static_cast<arrow::DecimalType *>(arrow_type.get());
            return std::make_shared<DataTypeDecimal<Decimal128>>(decimal_type->precision(), decimal_type->scale());
        }

        if (arrow_type->id() == arrow::Type::DECIMAL256)
        {
            const auto * decimal_type = static_cast<arrow::DecimalType *>(arrow_type.get());
            return std::make_shared<DataTypeDecimal<Decimal256>>(decimal_type->precision(), decimal_type->scale());
        }

        if (arrow_type->id() == arrow::Type::LIST)
        {
            const auto * list_type = static_cast<arrow::ListType *>(arrow_type.get());
            auto list_nested_type = list_type->value_type();

            const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(column_type.get());
            if (!array_type)
                throw Exception{fmt::format("Cannot convert arrow LIST type to a not Array ClickHouse type {}.", column_type->getName()), ErrorCodes::CANNOT_CONVERT_TYPE};

            return std::make_shared<DataTypeArray>(getInternalType(list_nested_type, array_type->getNestedType(), column_name, format_name));
        }

        if (arrow_type->id() == arrow::Type::STRUCT)
        {
            const auto * struct_type = static_cast<arrow::StructType *>(arrow_type.get());
            const DataTypeTuple * tuple_type = typeid_cast<const DataTypeTuple *>(column_type.get());
            if (!tuple_type)
                throw Exception{fmt::format("Cannot convert arrow STRUCT type to a not Tuple ClickHouse type {}.", column_type->getName()), ErrorCodes::CANNOT_CONVERT_TYPE};

            const DataTypes & tuple_nested_types = tuple_type->getElements();
            int internal_fields_num = tuple_nested_types.size();
            /// If internal column has less elements then arrow struct, we will select only first internal_fields_num columns.
            if (internal_fields_num > struct_type->num_fields())
                throw Exception
                    {
                        fmt::format(
                            "Cannot convert arrow STRUCT with {} fields to a ClickHouse Tuple with {} elements: {}.",
                            struct_type->num_fields(),
                            internal_fields_num,
                            column_type->getName()),
                        ErrorCodes::CANNOT_CONVERT_TYPE
                    };

            DataTypes nested_types;
            for (int i = 0; i < internal_fields_num; ++i)
                nested_types.push_back(getInternalType(struct_type->field(i)->type(), tuple_nested_types[i], column_name, format_name));

            return std::make_shared<DataTypeTuple>(std::move(nested_types));
        }

        if (arrow_type->id() == arrow::Type::DICTIONARY)
        {
            const auto * arrow_dict_type = static_cast<arrow::DictionaryType *>(arrow_type.get());
            const auto * lc_type = typeid_cast<const DataTypeLowCardinality *>(column_type.get());
            /// We allow to insert arrow dictionary into a non-LowCardinality column.
            const auto & dict_type = lc_type ? lc_type->getDictionaryType() : column_type;
            return std::make_shared<DataTypeLowCardinality>(getInternalType(arrow_dict_type->value_type(), dict_type, column_name, format_name));
        }

        if (arrow_type->id() == arrow::Type::MAP)
        {
            const auto * arrow_map_type = typeid_cast<arrow::MapType *>(arrow_type.get());
            const auto * map_type = typeid_cast<const DataTypeMap *>(column_type.get());
            if (!map_type)
                throw Exception{fmt::format("Cannot convert arrow MAP type to a not Map ClickHouse type {}.", column_type->getName()), ErrorCodes::CANNOT_CONVERT_TYPE};

            return std::make_shared<DataTypeMap>(
                getInternalType(arrow_map_type->key_type(), map_type->getKeyType(), column_name, format_name),
                getInternalType(arrow_map_type->item_type(), map_type->getValueType(), column_name, format_name)
                );
        }

        if (const auto * internal_type_it = std::find_if(arrow_type_to_internal_type.begin(), arrow_type_to_internal_type.end(),
                                                              [=](auto && elem) { return elem.first == arrow_type->id(); });
            internal_type_it != arrow_type_to_internal_type.end())
        {
            return DataTypeFactory::instance().get(internal_type_it->second);
        }
        throw Exception
            {
                fmt::format(R"(The type "{}" of an input column "{}" is not supported for conversion from a {} data format.)", arrow_type->name(), column_name, format_name),
                ErrorCodes::CANNOT_CONVERT_TYPE
            };
    }

    ArrowColumnToCHColumn::ArrowColumnToCHColumn(const Block & header_, std::shared_ptr<arrow::Schema> schema_, const std::string & format_name_) : header(header_), format_name(format_name_)
    {
        for (const auto & field : schema_->fields())
        {
            if (header.has(field->name()))
            {
                const auto column_type = recursiveRemoveLowCardinality(header.getByName(field->name()).type);
                name_to_internal_type[field->name()] = getInternalType(field->type(), column_type, field->name(), format_name);
            }
        }
    }

    void ArrowColumnToCHColumn::arrowTableToCHChunk(Chunk & res, std::shared_ptr<arrow::Table> & table)
    {
        Columns columns_list;
        UInt64 num_rows = 0;

        columns_list.reserve(header.rows());

        using NameToColumnPtr = std::unordered_map<std::string, std::shared_ptr<arrow::ChunkedArray>>;

        NameToColumnPtr name_to_column_ptr;
        for (const auto& column_name : table->ColumnNames())
        {
            std::shared_ptr<arrow::ChunkedArray> arrow_column = table->GetColumnByName(column_name);
            name_to_column_ptr[column_name] = arrow_column;
        }

        for (size_t column_i = 0, columns = header.columns(); column_i < columns; ++column_i)
        {
            const ColumnWithTypeAndName & header_column = header.getByPosition(column_i);

            if (name_to_column_ptr.find(header_column.name) == name_to_column_ptr.end())
                // TODO: What if some columns were not presented? Insert NULLs? What if a column is not nullable?
                throw Exception{fmt::format("Column \"{}\" is not presented in input data.", header_column.name),
                                ErrorCodes::THERE_IS_NO_COLUMN};

            std::shared_ptr<arrow::ChunkedArray> arrow_column = name_to_column_ptr[header_column.name];

            DataTypePtr & internal_type = name_to_internal_type[header_column.name];
            MutableColumnPtr read_column = internal_type->createColumn();
            readColumnFromArrowColumn(arrow_column, *read_column, header_column.name, format_name, false, dictionary_values);

            ColumnWithTypeAndName column;
            column.name = header_column.name;
            column.type = internal_type;
            column.column = std::move(read_column);

            column.column = castColumn(column, header_column.type);
            column.type = header_column.type;
            num_rows = column.column->size();
            columns_list.push_back(std::move(column.column));
        }

        res.setColumns(columns_list, num_rows);
    }
}
#endif
