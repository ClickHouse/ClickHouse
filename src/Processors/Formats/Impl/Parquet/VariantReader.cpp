#include <Processors/Formats/Impl/Parquet/VariantReader.h>
#include <Processors/Formats/Impl/Parquet/VariantUtils.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsCommon.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeVariant.h>
#include <Interpreters/castColumn.h>
#include <IO/ReadBufferFromString.h>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace DB::Parquet::VariantReader
{

namespace
{

enum class OutputMode
{
    NullableString,
    Generic,
    Dynamic,
    Object,
};

std::optional<std::string_view> getNullableStringValue(const IColumn & column, size_t row_num);

class SequentialNullableStringAccessor
{
public:
    explicit SequentialNullableStringAccessor(const Reader::ColumnSubchunk & subchunk_)
        : subchunk(subchunk_)
    {
    }

    std::optional<std::string_view> get(size_t logical_row)
    {
        if (!subchunk.column)
            return std::nullopt;

        if (!subchunk.null_map)
            return getNullableStringValue(*subchunk.column, logical_row);

        const auto & null_map = assert_cast<const ColumnUInt8 &>(*subchunk.null_map).getData();
        if (null_map[logical_row])
            return std::nullopt;

        const auto & string_column = assert_cast<const ColumnString &>(*subchunk.column);
        auto data = string_column.getDataAt(physical_row);
        ++physical_row;
        return std::string_view(data.data(), data.size());
    }

private:
    const Reader::ColumnSubchunk & subchunk;
    size_t physical_row = 0;
};

bool nestedColumnHasDefaultsAtNulls(const ColumnNullable & nullable_column, const DataTypePtr & output_type)
{
    const auto & null_map = nullable_column.getNullMapData();
    if (memoryIsZero(null_map.data(), 0, null_map.size()))
        return true;

    auto default_column = output_type->createColumn();
    default_column->insertDefault();

    const auto & nested = nullable_column.getNestedColumn();
    for (size_t row = 0; row < null_map.size(); ++row)
    {
        if (null_map[row] && nested.compareAt(row, 0, *default_column, 1) != 0)
            return false;
    }

    return true;
}

MutableColumnPtr materializeTypedValueFastPath(
    const ColumnPtr & typed_value_column,
    const DataTypePtr & typed_value_output_type,
    const DataTypePtr & output_type)
{
    if (typed_value_output_type->equals(*output_type))
        return IColumn::mutate(typed_value_column);

    const auto * nullable_type = typeid_cast<const DataTypeNullable *>(typed_value_output_type.get());
    const auto * nullable_column = typeid_cast<const ColumnNullable *>(typed_value_column.get());
    if (!nullable_type || !nullable_column || !nullable_type->getNestedType()->equals(*output_type))
        return {};

    if (nestedColumnHasDefaultsAtNulls(*nullable_column, output_type))
        return IColumn::mutate(nullable_column->getNestedColumnPtr());

    MutableColumnPtr result = output_type->createColumn();
    const auto & nested = nullable_column->getNestedColumn();
    for (size_t row = 0; row < typed_value_column->size(); ++row)
    {
        if (nullable_column->isNullAt(row))
            result->insertDefault();
        else
            result->insertFrom(nested, row);
    }

    return result;
}

MutableColumnPtr materializeNullableTypedValueAsLowCardinality(
    const ColumnPtr & typed_value_column,
    const DataTypePtr & typed_value_input_type,
    const DataTypePtr & output_type)
{
    const auto * output_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(output_type.get());
    const auto * input_low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(typed_value_input_type.get());
    const auto * input_low_cardinality_column = typeid_cast<const ColumnLowCardinality *>(typed_value_column.get());
    if (output_low_cardinality && input_low_cardinality_type && input_low_cardinality_column)
    {
        const auto * input_nullable_dictionary = typeid_cast<const DataTypeNullable *>(input_low_cardinality_type->getDictionaryType().get());
        if (input_nullable_dictionary && input_nullable_dictionary->getNestedType()->equals(*output_low_cardinality->getDictionaryType()))
        {
            auto result = input_low_cardinality_column->cloneWithDefaultOnNull();
            return IColumn::mutate(std::move(result));
        }
    }

    const auto * input_nullable_type = typeid_cast<const DataTypeNullable *>(typed_value_input_type.get());
    const auto * nullable_column = typeid_cast<const ColumnNullable *>(typed_value_column.get());
    if (!output_low_cardinality || !input_nullable_type || !nullable_column)
        return {};

    if (!input_nullable_type->getNestedType()->equals(*output_low_cardinality->getDictionaryType()))
        return {};

    if (!nestedColumnHasDefaultsAtNulls(*nullable_column, output_low_cardinality->getDictionaryType()))
        return {};

    auto result = output_type->createColumn();
    auto & low_cardinality_column = assert_cast<ColumnLowCardinality &>(*result);
    low_cardinality_column.insertRangeFromFullColumn(nullable_column->getNestedColumn(), 0, nullable_column->size());
    return result;
}

MutableColumnPtr materializeDecodedNullableTypedValueFastPath(
    const ColumnPtr & typed_value_column,
    const DataTypePtr & typed_value_input_type,
    const DataTypePtr & output_type)
{
    const auto * nullable_type = typeid_cast<const DataTypeNullable *>(typed_value_input_type.get());
    const auto * nullable_column = typeid_cast<const ColumnNullable *>(typed_value_column.get());
    if (!nullable_type || !nullable_column || !nullable_type->getNestedType()->equals(*output_type))
        return {};

    /// The `Parquet` decoder expanded nullable primitives with defaults in nested null positions.
    return IColumn::mutate(nullable_column->getNestedColumnPtr());
}

std::optional<std::string_view> getNullableStringValue(const IColumn & column, size_t row_num)
{
    if (const auto * column_const = typeid_cast<const ColumnConst *>(&column))
        return getNullableStringValue(column_const->getDataColumn(), column_const->getDataColumn().size() == 1 ? 0 : row_num);

    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        if (nullable->isNullAt(row_num))
            return std::nullopt;

        const auto & nested = assert_cast<const ColumnString &>(nullable->getNestedColumn());
        auto data = nested.getDataAt(row_num);
        return std::string_view(data.data(), data.size());
    }

    const auto & string_column = assert_cast<const ColumnString &>(column);
    auto data = string_column.getDataAt(row_num);
    return std::string_view(data.data(), data.size());
}

size_t preserveModeIndex(bool preserve_empty_containers)
{
    return preserve_empty_containers ? 1 : 0;
}

template <typename T>
T loadScalarPrimitiveValue(const ScalarExactValue & value)
{
    T result {};
    static_assert(sizeof(T) <= sizeof(value.primitive_bits));
    memcpy(&result, &value.primitive_bits, sizeof(T));
    return result;
}

size_t typedOutputCacheKey(const Reader::VariantSourceInfo & source_info)
{
    return source_info.typed_value_output_idx;
}

template <typename T>
void filterVectorInPlace(std::vector<T> & values, const IColumnFilter & filter, size_t result_size_hint)
{
    if (values.empty())
        return;

    chassert(values.size() == filter.size());

    std::vector<T> filtered;
    filtered.reserve(result_size_hint);
    for (size_t row = 0; row < filter.size(); ++row)
    {
        if (filter[row])
            filtered.push_back(std::move(values[row]));
    }

    values = std::move(filtered);
}

void rebuildMetadataByRow(MetadataState & state, const FormatSettings & format_settings)
{
    state.shared_metadata_storage.reset();
    state.metadata_by_row.assign(state.metadata_values.size(), nullptr);
    state.metadata_is_shared_across_rows = true;

    std::optional<std::string_view> first_metadata;
    bool first_row = true;
    for (size_t row = 0; row < state.metadata_values.size(); ++row)
    {
        const auto & metadata = state.metadata_values[row];

        if (first_row)
        {
            first_metadata = metadata;
            first_row = false;
            continue;
        }

        if (metadata != first_metadata)
            state.metadata_is_shared_across_rows = false;
    }

    if (state.metadata_is_shared_across_rows)
    {
        if (first_metadata.has_value())
        {
            state.shared_metadata_storage.emplace(decodeMetadata(*first_metadata, format_settings));
            const VariantMetadata * shared_metadata = &*state.shared_metadata_storage;
            std::fill(state.metadata_by_row.begin(), state.metadata_by_row.end(), shared_metadata);
        }

        return;
    }

    for (size_t row = 0; row < state.metadata_values.size(); ++row)
    {
        if (state.metadata_values[row].has_value())
            state.metadata_by_row[row] = &state.metadata_cache.get(*state.metadata_values[row], format_settings);
    }
}

const Reader::OutputColumnInfo * getTypedValueOutputInfo(const Reader & reader, const Reader::VariantSourceInfo & source_info)
{
    if (source_info.typed_value_output_idx == UINT64_MAX)
        return nullptr;

    return &reader.output_columns.at(source_info.typed_value_output_idx);
}

ColumnPtr getTypedValueColumn(
    Reader & reader,
    Reader::RowSubgroup & row_subgroup,
    const Reader::VariantSourceInfo & source_info,
    size_t num_rows)
{
    if (source_info.typed_value_output_idx == UINT64_MAX)
        return {};

    return reader.formOutputColumn(
        row_subgroup,
        source_info.typed_value_output_idx,
        num_rows,
        /*skip_cast=*/ true);
}

bool shouldPreserveEmptyContainers(
    const MetadataState & metadata_state,
    const Reader::OutputColumnInfo & output_info,
    const Reader::OutputColumnInfo * typed_value_output_info)
{
    return (metadata_state.metadata_is_shared_across_rows && metadata_state.metadata_by_row.size() == 1)
        || output_info.variant_preserve_empty_typed_fields
        || (typed_value_output_info && typed_value_output_info->variant_preserve_empty_typed_fields);
}

struct PreparedRowValue
{
    const VariantValue * borrowed_value = nullptr;
    std::optional<VariantValue> storage;
    std::optional<VariantValue> projected_storage;

    const VariantValue * getValue() const
    {
        if (projected_storage)
            return &*projected_storage;

        if (storage)
            return &*storage;

        return borrowed_value;
    }
};

PreparedRowValue prepareRowValue(
    const SourceState & state,
    const std::vector<ConvertedTypedValue> * typed_value_rows,
    size_t row,
    const FormatSettings & format_settings)
{
    PreparedRowValue prepared;
    const auto & row_value = state.value_values[row];
    chassert(state.metadata_state);
    const VariantMetadata * row_metadata = state.metadata_state->metadata_by_row[row];
    const ConvertedTypedValue * prepared_typed_row = typed_value_rows ? &typed_value_rows->at(row) : nullptr;
    const bool typed_value_present = prepared_typed_row && prepared_typed_row->present;

    if (!row_value.has_value() && !typed_value_present)
        return prepared;

    if (!row_metadata)
    {
        if (!row_value.has_value() && typed_value_present && prepared_typed_row->is_empty_container)
            return prepared;

        throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed `Parquet` `VARIANT`: row {} has `NULL` `metadata` but non-`NULL` payload", row);
    }

    if (row_value.has_value())
    {
        auto residual_value = decodeValue(*row_metadata, *row_value, format_settings);
        if (typed_value_present)
            prepared.storage = mergeResidualValueWithTypedValue(std::move(residual_value), *prepared_typed_row, format_settings);
        else
            prepared.storage = std::move(residual_value.value);
        return prepared;
    }

    if (typed_value_present)
        prepared.borrowed_value = &prepared_typed_row->value;

    return prepared;
}

std::vector<ConvertedTypedValue> & getOrPrepareTypedValueRows(
    SourceState & state,
    Reader & reader,
    Reader::RowSubgroup & row_subgroup,
    const Reader::OutputColumnInfo & output_info,
    const Reader::VariantSourceInfo & source_info,
    size_t num_rows)
{
    const auto * typed_value_output_info = getTypedValueOutputInfo(reader, source_info);
    if (!typed_value_output_info)
    {
        static std::vector<ConvertedTypedValue> empty;
        return empty;
    }

    chassert(state.metadata_state);
    const bool preserve_empty_containers = shouldPreserveEmptyContainers(*state.metadata_state, output_info, typed_value_output_info);
    const size_t preserve_idx = preserveModeIndex(preserve_empty_containers);
    auto & per_output_state = state.per_typed_output_state[typedOutputCacheKey(source_info)];
    if (per_output_state.typed_value_rows_prepared[preserve_idx])
        return per_output_state.typed_value_rows_by_preserve[preserve_idx];

    auto & typed_value_rows = per_output_state.typed_value_rows_by_preserve[preserve_idx];
    ColumnPtr typed_value_column = getTypedValueColumn(reader, row_subgroup, source_info, num_rows);
    if (typed_value_column)
    {
        typed_value_rows = convertTypedColumnRange(
            *typed_value_column,
            typed_value_output_info->input_type,
            state.metadata_state->metadata_by_row.data(),
            num_rows,
            0,
            reader.options.format,
            state.metadata_state->metadata_is_shared_across_rows,
            preserve_empty_containers,
            std::nullopt,
            1);
    }

    per_output_state.typed_value_rows_prepared[preserve_idx] = true;
    return typed_value_rows;
}

MutableColumnPtr tryFormTypedValueFastPath(
    SourceState & state,
    Reader & reader,
    Reader::RowSubgroup & row_subgroup,
    const Reader::OutputColumnInfo & output_info,
    const Reader::VariantSourceInfo & source_info,
    size_t num_rows)
{
    const bool use_exact_typed_value_output
        = !source_info.string_output_uses_json && !isComplexVariantExactOutputType(output_info.output_type);
    if (!use_exact_typed_value_output)
        return {};

    const auto * typed_value_output_info = getTypedValueOutputInfo(reader, source_info);
    if (!typed_value_output_info)
        return {};

    ColumnPtr typed_value_column = getTypedValueColumn(reader, row_subgroup, source_info, num_rows);
    if (!typed_value_column)
        return {};

    DataTypePtr typed_value_input_type = typed_value_output_info->input_type;
    DataTypePtr typed_value_output_type = typed_value_input_type;

    if (state.value_column_is_all_null && typed_value_input_type)
    {
        chassert(state.metadata_state);
        if (!state.metadata_state || state.metadata_state->metadata_by_row.size() < num_rows)
            return {};

        for (size_t row = 0; row < num_rows; ++row)
        {
            if (!state.metadata_state->metadata_by_row[row])
                return {};
        }

        if (auto result = materializeNullableTypedValueAsLowCardinality(typed_value_column, typed_value_input_type, output_info.output_type))
            return result;

        if (auto result = materializeDecodedNullableTypedValueFastPath(typed_value_column, typed_value_input_type, output_info.output_type))
            return result;
    }

    if (typed_value_input_type)
    {
        DataTypePtr exact_typed_value_type = output_info.output_type;
        if (typeid_cast<const ColumnNullable *>(typed_value_column.get())
            && !typeid_cast<const DataTypeNullable *>(exact_typed_value_type.get()))
        {
            exact_typed_value_type = makeVariantExactOutputTypeNullable(exact_typed_value_type);
        }

        if (!typed_value_input_type->equals(*exact_typed_value_type))
        {
            auto casted_typed_value_column = castColumn(
                {typed_value_column, typed_value_input_type, output_info.name + ".__typed_value"},
                exact_typed_value_type);
            typed_value_column = std::move(casted_typed_value_column);
            typed_value_output_type = exact_typed_value_type;
        }
    }

    if (state.value_column_is_all_null && typed_value_output_type)
        return materializeTypedValueFastPath(typed_value_column, typed_value_output_type, output_info.output_type);

    return {};
}

}

static MetadataState & getOrPrepareMetadataState(
    Reader & reader,
    Reader::RowSubgroup & row_subgroup,
    const Reader::OutputColumnInfo & output_info,
    const Reader::VariantSourceInfo & source_info,
    size_t num_rows)
{
    const size_t metadata_state_idx = source_info.metadata_state_slot_idx;
    if (metadata_state_idx >= row_subgroup.variant_metadata_states.size())
    {
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Invalid shared `Parquet` `VARIANT` metadata state slot {} for output {}",
            metadata_state_idx,
            output_info.name);
    }

    auto & metadata_state_ptr = row_subgroup.variant_metadata_states[metadata_state_idx];
    if (metadata_state_ptr)
        return *metadata_state_ptr;

    const auto & metadata_subchunk = row_subgroup.columns.at(source_info.metadata_primitive_idx);
    if (!metadata_subchunk.column)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "`Parquet` `VARIANT` metadata primitive {} for output {} was unavailable while preparing shared metadata state "
            "(value primitive: {}, typed output: {}, source idx: {}, idx_in_output_block: {}, primitive dependency count: {})",
            source_info.metadata_primitive_idx,
            output_info.name,
            source_info.value_primitive_idx,
            source_info.typed_value_output_idx,
            output_info.source_idx,
            output_info.idx_in_output_block.has_value() ? std::to_string(*output_info.idx_in_output_block) : String("none"),
            output_info.primitive_dependencies.size());
    }

    auto metadata_state = std::make_shared<MetadataState>();
    metadata_state->metadata_values.resize(num_rows);
    metadata_state->metadata_column_holder = metadata_subchunk.column->getPtr();

    SequentialNullableStringAccessor metadata_accessor(metadata_subchunk);
    for (size_t row = 0; row < num_rows; ++row)
    {
        if (auto metadata = metadata_accessor.get(row); metadata.has_value())
            metadata_state->metadata_values[row] = *metadata;
    }

    rebuildMetadataByRow(*metadata_state, reader.options.format);

    row_subgroup.columns.at(source_info.metadata_primitive_idx).column.reset();
    row_subgroup.columns.at(source_info.metadata_primitive_idx).null_map.reset();

    metadata_state_ptr = std::move(metadata_state);
    return *metadata_state_ptr;
}

SourceState & getOrPrepareSourceState(
    Reader & reader,
    Reader::RowSubgroup & row_subgroup,
    const Reader::OutputColumnInfo & output_info,
    const Reader::VariantSourceInfo & source_info,
    size_t num_rows)
{
    const size_t state_idx = source_info.state_slot_idx;
    if (state_idx >= row_subgroup.variant_source_states.size())
    {
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Invalid shared `Parquet` `VARIANT` source state slot {} for output {}",
            state_idx,
            output_info.name);
    }

    auto & state_ptr = row_subgroup.variant_source_states[state_idx];
    if (state_ptr)
        return *state_ptr;

    auto state = std::make_shared<SourceState>();
    state->metadata_state = row_subgroup.variant_metadata_states[source_info.metadata_state_slot_idx];
    if (!state->metadata_state)
    {
        getOrPrepareMetadataState(reader, row_subgroup, output_info, source_info, num_rows);
        state->metadata_state = row_subgroup.variant_metadata_states[source_info.metadata_state_slot_idx];
    }
    state->value_values.resize(num_rows);

    if (source_info.value_primitive_idx != UINT64_MAX)
    {
        Reader::ColumnSubchunk & value_subchunk = row_subgroup.columns.at(source_info.value_primitive_idx);
        if (value_subchunk.column)
        {
            state->value_column_holder = value_subchunk.column->getPtr();
            state->value_column_is_all_null = true;
            SequentialNullableStringAccessor value_accessor(value_subchunk);
            for (size_t row = 0; row < num_rows; ++row)
            {
                if (auto value = value_accessor.get(row); value.has_value())
                {
                    state->value_values[row] = *value;
                    state->value_column_is_all_null = false;
                }
            }
        }
    }
    if (source_info.value_primitive_idx != UINT64_MAX)
        row_subgroup.columns.at(source_info.value_primitive_idx).column.reset();

    state_ptr = std::move(state);
    return *state_ptr;
}

void filterMetadataState(MetadataState & state, const IColumnFilter & filter, size_t result_size_hint, const FormatSettings & format_settings)
{
    filterVectorInPlace(state.metadata_values, filter, result_size_hint);
    rebuildMetadataByRow(state, format_settings);
}

void filterSourceState(SourceState & state, const IColumnFilter & filter, size_t result_size_hint, const FormatSettings & /*format_settings*/)
{
    filterVectorInPlace(state.value_values, filter, result_size_hint);

    state.value_column_is_all_null = true;
    for (const auto & value : state.value_values)
    {
        if (value.has_value())
        {
            state.value_column_is_all_null = false;
            break;
        }
    }

    state.per_typed_output_state.clear();
}

static const VariantValue * getOutputValue(
    PreparedRowValue & prepared_row_value,
    const ParsedVariantPath * parsed_subcolumn_path,
    const FormatSettings & format_settings)
{
    const VariantValue * value = prepared_row_value.getValue();
    if (!value)
        return nullptr;

    if (parsed_subcolumn_path)
    {
        prepared_row_value.projected_storage = extractSubcolumnValue(*value, *parsed_subcolumn_path, format_settings);
        value = prepared_row_value.getValue();
    }

    return value;
}

static void insertExactValueIntoColumn(
    IColumn & column,
    const VariantValue & value,
    const FormatSettings & format_settings)
{
    if (!value.exact_type)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot insert `Parquet` `VARIANT` value without exact type directly");

    if (value.exact_string_view.has_value())
    {
        assert_cast<ColumnString &>(column).insertData(value.exact_string_view->data(), value.exact_string_view->size());
        return;
    }

    if (!value.exact_column)
    {
        auto source_column = materializeExactValueColumn(value, format_settings);
        column.insertFrom(*source_column, 0);
        return;
    }

    column.insertFrom(*value.exact_column, value.exact_row_num);
}

static void insertScalarExactValueIntoColumn(IColumn & column, const ScalarExactValue & value)
{
    if (!value.exact_type)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot insert `Parquet` `VARIANT` scalar value without exact type directly");

    if (value.exact_string_view.has_value())
    {
        assert_cast<ColumnString &>(column).insertData(value.exact_string_view->data(), value.exact_string_view->size());
        return;
    }

    switch (value.primitive_kind)
    {
        case ScalarExactValue::PrimitiveKind::UInt8:
            assert_cast<ColumnUInt8 &>(column).insertValue(loadScalarPrimitiveValue<UInt8>(value));
            return;
        case ScalarExactValue::PrimitiveKind::Int8:
            assert_cast<ColumnInt8 &>(column).insertValue(loadScalarPrimitiveValue<Int8>(value));
            return;
        case ScalarExactValue::PrimitiveKind::Int16:
            assert_cast<ColumnInt16 &>(column).insertValue(loadScalarPrimitiveValue<Int16>(value));
            return;
        case ScalarExactValue::PrimitiveKind::Int32:
            assert_cast<ColumnInt32 &>(column).insertValue(loadScalarPrimitiveValue<Int32>(value));
            return;
        case ScalarExactValue::PrimitiveKind::Int64:
            assert_cast<ColumnInt64 &>(column).insertValue(loadScalarPrimitiveValue<Int64>(value));
            return;
        case ScalarExactValue::PrimitiveKind::Float32:
            assert_cast<ColumnFloat32 &>(column).insertValue(loadScalarPrimitiveValue<Float32>(value));
            return;
        case ScalarExactValue::PrimitiveKind::Float64:
            assert_cast<ColumnFloat64 &>(column).insertValue(loadScalarPrimitiveValue<Float64>(value));
            return;
        case ScalarExactValue::PrimitiveKind::Date32:
            assert_cast<ColumnDate32 &>(column).insertValue(loadScalarPrimitiveValue<Int32>(value));
            return;
        case ScalarExactValue::PrimitiveKind::DateTime64:
            assert_cast<ColumnDateTime64 &>(column).insertValue(static_cast<DateTime64>(loadScalarPrimitiveValue<Int64>(value)));
            return;
        case ScalarExactValue::PrimitiveKind::Time64:
            assert_cast<ColumnTime64 &>(column).insertValue(static_cast<Time64>(loadScalarPrimitiveValue<Int64>(value)));
            return;
        case ScalarExactValue::PrimitiveKind::Decimal32:
            assert_cast<ColumnDecimal<Decimal32> &>(column).insertValue(Decimal32(loadScalarPrimitiveValue<Int32>(value)));
            return;
        case ScalarExactValue::PrimitiveKind::Decimal64:
            assert_cast<ColumnDecimal<Decimal64> &>(column).insertValue(Decimal64(loadScalarPrimitiveValue<Int64>(value)));
            return;
        case ScalarExactValue::PrimitiveKind::None:
            break;
    }

    if (!value.exact_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot insert `Parquet` `VARIANT` scalar value without payload directly");

    column.insertFrom(*value.exact_column, value.exact_row_num);
}

static MutableColumnPtr materializeDynamicExactColumn(
    MutableColumnPtr materialized_column,
    const DataTypePtr & common_exact_type,
    const PaddedPODArray<UInt8> & null_rows,
    size_t num_rows,
    const DataTypeDynamic & dynamic_type)
{
    DataTypes variant_types{common_exact_type, ColumnDynamic::getSharedVariantDataType()};
    auto variant_type = std::make_shared<DataTypeVariant>(variant_types);
    auto variant_discr_opt = variant_type->tryGetVariantDiscriminator(common_exact_type->getName());
    if (!variant_discr_opt)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find `Parquet` `VARIANT` exact type {} inside `Dynamic` helper variant {}", common_exact_type->getName(), variant_type->getName());

    auto variant_discr = *variant_discr_opt;
    auto discriminators = ColumnVariant::ColumnDiscriminators::create();
    auto & discriminators_data = discriminators->getData();
    discriminators_data.resize_fill(num_rows, variant_discr);
    if (!memoryIsZero(null_rows.data(), 0, null_rows.size()))
    {
        for (size_t row = 0; row < num_rows; ++row)
        {
            if (null_rows[row])
                discriminators_data[row] = ColumnVariant::NULL_DISCRIMINATOR;
        }
    }

    Columns variant_columns;
    variant_columns.reserve(variant_type->getVariants().size());
    for (const auto & type : variant_type->getVariants())
        variant_columns.emplace_back(type->createColumn());

    variant_columns[variant_discr] = std::move(materialized_column);

    auto variant_column = ColumnVariant::create(std::move(discriminators), variant_columns);
    return ColumnDynamic::create(
        IColumn::mutate(std::move(variant_column)),
        variant_type,
        dynamic_type.getMaxDynamicTypes(),
        dynamic_type.getMaxDynamicTypes());
}

static MutableColumnPtr materializeNullableExactColumn(
    MutableColumnPtr materialized_column,
    const PaddedPODArray<UInt8> & null_rows)
{
    materialized_column->expand(null_rows, /* inverted = */ true);

    auto null_map = ColumnUInt8::create();
    null_map->getData().assign(null_rows.begin(), null_rows.end());
    return ColumnNullable::create(std::move(materialized_column), std::move(null_map));
}

static MutableColumnPtr finalizeExactOutputColumn(
    MutableColumnPtr materialized_column,
    const DataTypePtr & common_exact_type,
    const PaddedPODArray<UInt8> & null_rows,
    size_t num_rows,
    const Reader::OutputColumnInfo & output_info)
{
    if (!common_exact_type)
    {
        auto result = output_info.output_type->createColumn();
        result->insertManyDefaults(num_rows);
        return result;
    }

    const auto * dynamic_type = typeid_cast<const DataTypeDynamic *>(output_info.output_type.get());
    if (dynamic_type)
        return materializeDynamicExactColumn(std::move(materialized_column), common_exact_type, null_rows, num_rows, *dynamic_type);

    const bool has_null_rows = !memoryIsZero(null_rows.data(), 0, null_rows.size());
    if (has_null_rows && isNullableOrLowCardinalityNullable(output_info.output_type))
    {
        auto nullable_column = materializeNullableExactColumn(std::move(materialized_column), null_rows);
        auto nullable_exact_type = makeNullable(common_exact_type);
        if (nullable_exact_type->equals(*output_info.output_type))
            return nullable_column;

        auto casted = castColumn({std::move(nullable_column), nullable_exact_type, output_info.name}, output_info.output_type);
        return IColumn::mutate(casted->convertToFullColumnIfConst());
    }

    if (has_null_rows)
        materialized_column->expand(null_rows, /* inverted = */ true);

    if (common_exact_type->equals(*output_info.output_type))
        return materialized_column;

    auto casted = castColumn({std::move(materialized_column), common_exact_type, output_info.name}, output_info.output_type);
    return IColumn::mutate(casted->convertToFullColumnIfConst());
}

struct DirectExactOutputCandidate
{
    size_t output_idx = UINT64_MAX;
    const Reader::OutputColumnInfo * output_info = nullptr;
    ParsedVariantPath parsed_path;
    std::optional<ResolvedVariantPath> resolved_path;
    std::optional<size_t> exact_root_field_slice_idx;
    std::optional<size_t> nested_root_field_slice_idx;
    std::optional<ResolvedVariantPath> nested_root_tail_path;
    std::optional<size_t> nested_root_batch_group_idx;
    std::optional<size_t> nested_root_batch_field_slice_idx;
    DataTypePtr common_exact_type;
    MutableColumnPtr materialized_column;
    PaddedPODArray<UInt8> null_rows;
};

static bool isDirectExactOutputCandidate(const Reader::OutputColumnInfo & output_info, const Reader::VariantSourceInfo & source_info)
{
    return !output_info.source_subcolumn_name.empty()
        && source_info.typed_value_output_idx == UINT64_MAX;
}

static std::vector<DirectExactOutputCandidate> collectDirectExactOutputCandidates(
    Reader & reader,
    Reader::RowSubgroup & row_subgroup,
    const Reader::OutputColumnInfo & output_info)
{
    if (output_info.source_idx >= reader.variant_sources.size())
        return {};

    const size_t shared_state_idx = reader.variant_sources[output_info.source_idx].state_slot_idx;
    if (shared_state_idx >= reader.variant_output_indices_by_state_slot.size())
        return {};

    std::vector<DirectExactOutputCandidate> candidates;
    for (size_t output_idx : reader.variant_output_indices_by_state_slot[shared_state_idx])
    {
        if (output_idx >= reader.output_columns.size() || output_idx >= row_subgroup.formed_output_columns.size())
            continue;
        if (row_subgroup.formed_output_columns[output_idx])
            continue;

        const auto & sibling_output_info = reader.output_columns[output_idx];
        if (sibling_output_info.source_kind != Reader::OutputColumnInfo::SourceKind::Variant)
            continue;
        if (sibling_output_info.source_idx >= reader.variant_sources.size())
            continue;
        if (sibling_output_info.step_idx != output_info.step_idx)
            continue;

        const auto & sibling_source_info = reader.variant_sources[sibling_output_info.source_idx];
        if (!isDirectExactOutputCandidate(sibling_output_info, sibling_source_info))
            continue;
        DirectExactOutputCandidate & candidate = candidates.emplace_back();
        candidate.output_idx = output_idx;
        candidate.output_info = &sibling_output_info;
        candidate.parsed_path = parseVariantPath(sibling_output_info.source_subcolumn_name);
    }

    return candidates;
}

static MutableColumnPtr tryFormDirectSubcolumnExactFastPaths(
    Reader & reader,
    Reader::RowSubgroup & row_subgroup,
    const Reader::OutputColumnInfo & output_info,
    const FormatSettings & format_settings,
    size_t num_rows)
{
    if (output_info.source_idx >= reader.variant_sources.size())
        return {};

    const auto & source_info = reader.variant_sources[output_info.source_idx];
    if (!isDirectExactOutputCandidate(output_info, source_info))
        return {};
    if (source_info.value_primitive_idx == UINT64_MAX)
        return {};

    MetadataState & metadata_state = getOrPrepareMetadataState(reader, row_subgroup, output_info, source_info, num_rows);
    const Reader::ColumnSubchunk & value_subchunk = row_subgroup.columns.at(source_info.value_primitive_idx);
    if (!value_subchunk.column)
        return {};

    auto candidates = collectDirectExactOutputCandidates(reader, row_subgroup, output_info);
    if (candidates.empty())
        return {};

    const size_t requested_output_idx = static_cast<size_t>(&output_info - reader.output_columns.data());

    const VariantMetadata * shared_metadata = metadata_state.shared_metadata_storage ? &*metadata_state.shared_metadata_storage : nullptr;
    std::vector<UInt64> shared_root_field_ids;
    std::vector<std::optional<std::string_view>> shared_root_field_slices;
    struct NestedRootSliceBatchGroup
    {
        size_t root_field_slice_idx = UINT64_MAX;
        std::vector<UInt64> field_ids;
        std::vector<std::optional<std::string_view>> field_slices;
    };
    std::vector<NestedRootSliceBatchGroup> nested_root_slice_batch_groups;
    if (shared_metadata)
    {
        auto get_or_add_shared_root_field_idx = [&](UInt64 field_id)
        {
            auto it = std::find(shared_root_field_ids.begin(), shared_root_field_ids.end(), field_id);
            if (it != shared_root_field_ids.end())
                return static_cast<size_t>(it - shared_root_field_ids.begin());

            shared_root_field_ids.push_back(field_id);
            return shared_root_field_ids.size() - 1;
        };

        for (auto & candidate : candidates)
        {
            candidate.resolved_path = resolveVariantPath(*shared_metadata, candidate.parsed_path);

            if (candidate.resolved_path->steps.empty())
                continue;

            const auto & root_step = candidate.resolved_path->steps.front();
            if (root_step.exact_match_field_id.has_value())
                candidate.exact_root_field_slice_idx = get_or_add_shared_root_field_idx(*root_step.exact_match_field_id);

            if (candidate.parsed_path.steps.size() > 1 && root_step.nested_key_field_id.has_value())
            {
                candidate.nested_root_field_slice_idx = get_or_add_shared_root_field_idx(*root_step.nested_key_field_id);

                ResolvedVariantPath tail_path;
                tail_path.parsed_path.steps.assign(candidate.resolved_path->parsed_path.steps.begin() + 1, candidate.resolved_path->parsed_path.steps.end());
                tail_path.steps.assign(candidate.resolved_path->steps.begin() + 1, candidate.resolved_path->steps.end());
                candidate.nested_root_tail_path = std::move(tail_path);

                if (candidate.nested_root_tail_path->steps.size() == 1
                    && candidate.nested_root_tail_path->steps.front().exact_match_field_id.has_value())
                {
                    const size_t root_field_slice_idx = *candidate.nested_root_field_slice_idx;
                    auto group_it = std::find_if(
                        nested_root_slice_batch_groups.begin(),
                        nested_root_slice_batch_groups.end(),
                        [&](const NestedRootSliceBatchGroup & group)
                        {
                            return group.root_field_slice_idx == root_field_slice_idx;
                        });

                    if (group_it == nested_root_slice_batch_groups.end())
                    {
                        NestedRootSliceBatchGroup group;
                        group.root_field_slice_idx = root_field_slice_idx;
                        group_it = nested_root_slice_batch_groups.emplace(nested_root_slice_batch_groups.end(), std::move(group));
                    }

                    candidate.nested_root_batch_group_idx = static_cast<size_t>(group_it - nested_root_slice_batch_groups.begin());

                    const UInt64 field_id = *candidate.nested_root_tail_path->steps.front().exact_match_field_id;
                    auto field_it = std::find(group_it->field_ids.begin(), group_it->field_ids.end(), field_id);
                    if (field_it == group_it->field_ids.end())
                    {
                        group_it->field_ids.push_back(field_id);
                        group_it->field_slices.emplace_back();
                        field_it = std::prev(group_it->field_ids.end());
                    }

                    candidate.nested_root_batch_field_slice_idx = static_cast<size_t>(field_it - group_it->field_ids.begin());
                }
            }
        }

        shared_root_field_slices.resize(shared_root_field_ids.size());
    }

    for (auto & candidate : candidates)
        candidate.null_rows.reserve(num_rows);

    SequentialNullableStringAccessor value_accessor(value_subchunk);
    for (size_t row = 0; row < num_rows; ++row)
    {
        auto value_blob = value_accessor.get(row);
        const VariantMetadata * row_metadata = metadata_state.metadata_by_row[row];
        if (!row_metadata || !value_blob.has_value())
        {
            if (!row_metadata && value_blob.has_value())
                return {};

            for (auto & candidate : candidates)
                candidate.null_rows.push_back(UInt8(1));
            continue;
        }

        const VariantMetadata & metadata = *row_metadata;

        if (shared_metadata && !shared_root_field_ids.empty())
        {
            collectObjectFieldSlicesByResolvedIds(metadata, *value_blob, shared_root_field_ids, shared_root_field_slices, format_settings, 1);

            for (auto & group : nested_root_slice_batch_groups)
            {
                if (group.root_field_slice_idx >= shared_root_field_slices.size())
                    continue;

                const auto & nested_root_slice = shared_root_field_slices[group.root_field_slice_idx];
                if (!nested_root_slice.has_value())
                {
                    group.field_slices.assign(group.field_ids.size(), std::nullopt);
                    continue;
                }

                collectObjectFieldSlicesByResolvedIds(metadata, *nested_root_slice, group.field_ids, group.field_slices, format_settings, 2);
            }
        }

        for (auto & candidate : candidates)
        {
            if (shared_metadata)
            {
                ScalarExactValue scalar_value;
                std::optional<ScalarExactPathStatus> batched_scalar_status;

                if (candidate.exact_root_field_slice_idx.has_value())
                {
                    const auto & root_slice = shared_root_field_slices[*candidate.exact_root_field_slice_idx];
                    if (root_slice.has_value())
                        batched_scalar_status = tryDecodeScalarExactValue(*root_slice, scalar_value, format_settings, 2);
                }

                if (!batched_scalar_status.has_value() && candidate.nested_root_batch_group_idx.has_value() && candidate.nested_root_batch_field_slice_idx.has_value())
                {
                    const auto & group = nested_root_slice_batch_groups[*candidate.nested_root_batch_group_idx];
                    if (*candidate.nested_root_batch_field_slice_idx < group.field_slices.size())
                    {
                        const auto & nested_field_slice = group.field_slices[*candidate.nested_root_batch_field_slice_idx];
                        batched_scalar_status = nested_field_slice.has_value()
                            ? tryDecodeScalarExactValue(*nested_field_slice, scalar_value, format_settings, 3)
                            : ScalarExactPathStatus::Missing;
                    }
                }

                if (!batched_scalar_status.has_value() && candidate.nested_root_field_slice_idx.has_value() && candidate.nested_root_tail_path.has_value())
                {
                    const auto & nested_root_slice = shared_root_field_slices[*candidate.nested_root_field_slice_idx];
                    if (nested_root_slice.has_value())
                        batched_scalar_status = tryDecodeScalarExactValueByPath(metadata, *nested_root_slice, *candidate.nested_root_tail_path, scalar_value, format_settings, 2);
                }

                if (!batched_scalar_status.has_value() && (candidate.exact_root_field_slice_idx.has_value() || candidate.nested_root_field_slice_idx.has_value()))
                    batched_scalar_status = ScalarExactPathStatus::Missing;

                if (batched_scalar_status == ScalarExactPathStatus::Exact)
                {
                    if (!candidate.common_exact_type)
                    {
                        candidate.common_exact_type = scalar_value.exact_type;
                        candidate.materialized_column = candidate.common_exact_type->createColumn();
                        candidate.materialized_column->reserve(num_rows);
                    }
                    else if (!candidate.common_exact_type->equals(*scalar_value.exact_type))
                    {
                        return {};
                    }

                    insertScalarExactValueIntoColumn(*candidate.materialized_column, scalar_value);
                    candidate.null_rows.push_back(UInt8(0));
                    continue;
                }

                if (batched_scalar_status == ScalarExactPathStatus::Missing || batched_scalar_status == ScalarExactPathStatus::Null)
                {
                    candidate.null_rows.push_back(UInt8(1));
                    continue;
                }
            }

            if (candidate.resolved_path)
            {
                ScalarExactValue scalar_value;
                auto scalar_status = tryDecodeScalarExactValueByPath(metadata, *value_blob, *candidate.resolved_path, scalar_value, format_settings);
                if (scalar_status == ScalarExactPathStatus::Exact)
                {
                    if (!candidate.common_exact_type)
                    {
                        candidate.common_exact_type = scalar_value.exact_type;
                        candidate.materialized_column = candidate.common_exact_type->createColumn();
                        candidate.materialized_column->reserve(num_rows);
                    }
                    else if (!candidate.common_exact_type->equals(*scalar_value.exact_type))
                    {
                        return {};
                    }

                    insertScalarExactValueIntoColumn(*candidate.materialized_column, scalar_value);
                    candidate.null_rows.push_back(UInt8(0));
                    continue;
                }
                if (scalar_status == ScalarExactPathStatus::Missing || scalar_status == ScalarExactPathStatus::Null)
                {
                    candidate.null_rows.push_back(UInt8(1));
                    continue;
                }
            }

            VariantValue value;
            bool decoded = candidate.resolved_path
                ? tryDecodeValueByPath(metadata, *value_blob, *candidate.resolved_path, value, format_settings)
                : tryDecodeValueByPath(metadata, *value_blob, candidate.parsed_path, value, format_settings);
            if (!decoded || value.isNull())
            {
                candidate.null_rows.push_back(UInt8(1));
                continue;
            }

            if (!value.exact_type)
                return {};

            if (!candidate.common_exact_type)
            {
                candidate.common_exact_type = value.exact_type;
                candidate.materialized_column = candidate.common_exact_type->createColumn();
                candidate.materialized_column->reserve(num_rows);
            }
            else if (!candidate.common_exact_type->equals(*value.exact_type))
            {
                return {};
            }

            insertExactValueIntoColumn(*candidate.materialized_column, value, format_settings);
            candidate.null_rows.push_back(UInt8(0));
        }
    }

    MutableColumnPtr requested_output;
    for (auto & candidate : candidates)
    {
        auto finalized_column = finalizeExactOutputColumn(
            std::move(candidate.materialized_column),
            candidate.common_exact_type,
            candidate.null_rows,
            num_rows,
            *candidate.output_info);

        ColumnPtr finalized_column_ptr = finalized_column->getPtr();
        reader.cacheOutputColumn(row_subgroup, candidate.output_idx, finalized_column_ptr);

        if (candidate.output_idx == requested_output_idx)
            requested_output = std::move(finalized_column);
    }

    return requested_output;
}

MutableColumnPtr formOutputColumn(
    Reader & reader,
    Reader::RowSubgroup & row_subgroup,
    const Reader::OutputColumnInfo & output_info,
    const Reader::VariantSourceInfo & source_info,
    size_t num_rows)
{
    std::optional<ParsedVariantPath> parsed_subcolumn_path;

    if (!output_info.source_subcolumn_name.empty())
        parsed_subcolumn_path = parseVariantPath(output_info.source_subcolumn_name);

    if (auto result = tryFormDirectSubcolumnExactFastPaths(
            reader,
            row_subgroup,
            output_info,
            reader.options.format,
            num_rows))
        return result;

    SourceState & state = getOrPrepareSourceState(reader, row_subgroup, output_info, source_info, num_rows);

    if (auto result = tryFormTypedValueFastPath(state, reader, row_subgroup, output_info, source_info, num_rows))
        return result;
    const auto & typed_value_rows = getOrPrepareTypedValueRows(state, reader, row_subgroup, output_info, source_info, num_rows);
    const std::vector<ConvertedTypedValue> * typed_value_rows_ptr = typed_value_rows.empty() ? nullptr : &typed_value_rows;

    MutableColumnPtr result = output_info.output_type->createColumn();
    auto serialization = output_info.output_type->getDefaultSerialization();
    const bool use_json_string_output = isNullableStringType(output_info.output_type.get())
        && (source_info.string_output_uses_json || output_info.source_subcolumn_name.empty());
    const OutputMode output_mode
        = use_json_string_output ? OutputMode::NullableString
        : isDynamicLikeVariantOutputType(output_info.output_type.get()) ? OutputMode::Dynamic
        : isObjectLikeVariantOutputType(output_info.output_type.get()) ? OutputMode::Object
        : OutputMode::Generic;
    const bool is_nullable_output = typeid_cast<const DataTypeNullable *>(output_info.output_type.get());
    constexpr std::string_view null_json = "null";
    String json_buffer;
    ColumnString * string_output = nullptr;
    ColumnNullable * nullable_string_output = nullptr;

    if (output_mode == OutputMode::NullableString)
    {
        if (is_nullable_output)
        {
            nullable_string_output = &assert_cast<ColumnNullable &>(*result);
            string_output = &assert_cast<ColumnString &>(nullable_string_output->getNestedColumn());
            nullable_string_output->getNullMapData().reserve(num_rows);
        }
        else
        {
            string_output = &assert_cast<ColumnString &>(*result);
        }

        string_output->reserve(num_rows);
    }
    else
    {
        result->reserve(num_rows);
    }

    for (size_t row = 0; row < num_rows; ++row)
    {
        PreparedRowValue prepared_row_value = prepareRowValue(state, typed_value_rows_ptr, row, reader.options.format);
        const VariantValue * output_value = getOutputValue(
            prepared_row_value,
            parsed_subcolumn_path ? &*parsed_subcolumn_path : nullptr,
            reader.options.format);

        if (output_mode == OutputMode::NullableString)
        {
            if (nullable_string_output && (!output_value || output_value->isNull()))
            {
                string_output->insertDefault();
                nullable_string_output->getNullMapData().push_back(UInt8(1));
                continue;
            }

            if (!output_value)
                json_buffer = "null";
            else
                fillVariantValueJSON(*output_value, json_buffer, reader.options.format);
            string_output->insertData(json_buffer.data(), json_buffer.size());
            if (nullable_string_output)
                nullable_string_output->getNullMapData().push_back(UInt8(0));
            continue;
        }

        if (!output_value || output_value->isNull())
        {
            if (!source_info.string_output_uses_json || output_mode != OutputMode::Generic || !output_info.source_subcolumn_name.empty())
            {
                result->insertDefault();
                continue;
            }

            ReadBufferFromString buf(null_json);
            serialization->deserializeTextJSON(*result, buf, reader.options.format);
            continue;
        }

        if (output_mode == OutputMode::Dynamic || output_mode == OutputMode::Object || output_value->exact_type)
        {
            insertValueIntoOutput(*result, output_info.output_type, *output_value, reader.options.format);
            continue;
        }

        fillVariantValueJSON(*output_value, json_buffer, reader.options.format);
        if ((output_mode == OutputMode::Dynamic || is_nullable_output) && json_buffer == "null")
        {
            result->insertDefault();
            continue;
        }

        ReadBufferFromString buf(json_buffer);
        serialization->deserializeTextJSON(*result, buf, reader.options.format);
    }

    return result;
}

}
