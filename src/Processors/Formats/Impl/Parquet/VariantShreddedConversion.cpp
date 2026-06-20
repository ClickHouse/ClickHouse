#include <Processors/Formats/Impl/Parquet/VariantReader.h>
#include <Processors/Formats/Impl/Parquet/VariantUtils.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>

#include <algorithm>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace DB::Parquet::VariantReader
{

namespace
{

bool metadataContainsKey(const VariantMetadata & metadata, std::string_view key)
{
    if (metadata.strings_sorted)
        return std::binary_search(metadata.strings.begin(), metadata.strings.end(), key);

    return std::find(metadata.strings.begin(), metadata.strings.end(), key) != metadata.strings.end();
}

VariantValue makeExactValue(ColumnPtr exact_column, DataTypePtr exact_type, size_t exact_row_num = 0)
{
    VariantValue value;
    value.kind = VariantValue::Kind::ExactValue;
    value.exact_type = std::move(exact_type);
    value.exact_column = std::move(exact_column);
    value.exact_row_num = exact_row_num;
    return value;
}

VariantValue makeArrayValue(std::vector<VariantValue> elements, DataTypePtr exact_type)
{
    VariantValue value;
    value.kind = VariantValue::Kind::Array;
    value.exact_type = std::move(exact_type);
    value.array_elements = std::move(elements);
    return value;
}

VariantValue makeObjectValue(std::map<String, VariantValue> fields, DataTypePtr exact_type = {})
{
    VariantValue value;
    value.kind = VariantValue::Kind::Object;
    value.exact_type = std::move(exact_type);
    value.object_fields = std::move(fields);
    return value;
}

VariantValue makeTupleValue(std::vector<std::pair<String, VariantValue>> elements, DataTypePtr exact_type)
{
    VariantValue value;
    value.kind = VariantValue::Kind::Tuple;
    value.exact_type = std::move(exact_type);
    value.tuple_elements = std::move(elements);
    return value;
}

DataTypePtr tryBuildExactArrayType(const std::vector<VariantValue> & elements)
{
    DataTypePtr element_type;
    bool has_nulls = false;

    for (const auto & element : elements)
    {
        if (element.isNull())
        {
            has_nulls = true;
            continue;
        }

        if (!element.exact_type)
            return {};

        if (!element_type)
        {
            element_type = element.exact_type;
            continue;
        }

        if (!element_type->equals(*element.exact_type))
            return {};
    }

    if (!element_type)
        return {};

    if (has_nulls)
        element_type = makeVariantExactOutputTypeNullable(element_type);

    return std::make_shared<DataTypeArray>(element_type);
}

DataTypePtr getLogicalTypedValueType(const DataTypePtr & type)
{
    if (!type)
        return {};

    if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type.get()))
    {
        auto dictionary_type = getLogicalTypedValueType(low_cardinality->getDictionaryType());
        if (!dictionary_type)
            return {};
        return std::make_shared<DataTypeLowCardinality>(dictionary_type);
    }

    if (auto wrapper = tryGetVariantWrapperLayout(type))
        return getLogicalTypedValueType(wrapper->typed_value_type);

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        auto nested_type = getLogicalTypedValueType(array_type->getNestedType());
        if (!nested_type)
            return {};
        return std::make_shared<DataTypeArray>(nested_type);
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        if (!tuple_type->hasExplicitNames())
            return {};

        DataTypes field_types;
        Names field_names;
        field_types.reserve(tuple_type->getElements().size());
        field_names.reserve(tuple_type->getElements().size());

        for (size_t i = 0; i < tuple_type->getElements().size(); ++i)
        {
            auto child_type = getLogicalTypedValueType(tuple_type->getElement(i));
            if (!child_type)
                return {};

            field_types.emplace_back(makeNullableSafe(child_type));
            field_names.emplace_back(tuple_type->getNameByPosition(i + 1));
        }

        return std::make_shared<DataTypeTuple>(field_types, field_names);
    }

    return type;
}

bool hasVariantWrapperResidualValue(const IColumn & column, size_t row, const VariantWrapperLayout &)
{
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        if (nullable->isNullAt(row))
            return false;

        const auto & nested = assert_cast<const ColumnString &>(nullable->getNestedColumn());
        return !nested.getDataAt(row).empty();
    }

    const auto & string_column = assert_cast<const ColumnString &>(column);
    return !string_column.getDataAt(row).empty();
}

std::optional<std::string_view> getVariantWrapperResidualValue(const IColumn & column, size_t row)
{
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        if (nullable->isNullAt(row))
            return std::nullopt;

        const auto & nested = assert_cast<const ColumnString &>(nullable->getNestedColumn());
        auto data = nested.getDataAt(row);
        return std::string_view(data.data(), data.size());
    }

    const auto & string_column = assert_cast<const ColumnString &>(column);
    auto data = string_column.getDataAt(row);
    return std::string_view(data.data(), data.size());
}

bool isJSONEmptyContainer(std::string_view json_text)
{
    return json_text == "{}" || json_text == "[]";
}

bool isStructurallyEmptyContainerRow(const IColumn & column, size_t row, const DataTypePtr & type)
{
    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        if (const auto * array_column = typeid_cast<const ColumnArray *>(&column))
        {
            const auto & offsets = array_column->getOffsets();
            size_t begin = row == 0 ? 0 : offsets[row - 1];
            size_t end = offsets[row];
            return begin == end;
        }

        if (const auto * map_column = typeid_cast<const ColumnMap *>(&column))
        {
            const auto & offsets = map_column->getNestedColumn().getOffsets();
            size_t begin = row == 0 ? 0 : offsets[row - 1];
            size_t end = offsets[row];
            return begin == end;
        }

        (void) array_type;
        return false;
    }

    if (typeid_cast<const DataTypeMap *>(type.get()))
    {
        if (const auto * map_column = typeid_cast<const ColumnMap *>(&column))
        {
            const auto & offsets = map_column->getNestedColumn().getOffsets();
            size_t begin = row == 0 ? 0 : offsets[row - 1];
            size_t end = offsets[row];
            return begin == end;
        }

        return false;
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
        return tuple_type->getElements().empty() && typeid_cast<const ColumnTuple *>(&column);

    return false;
}

bool isRowExactEmptyContainer(const IColumn & column, size_t row, const DataTypePtr & type, const FormatSettings & format_settings)
{
    const IDataType * normalized = type.get();
    while (normalized)
    {
        if (const auto * nullable = typeid_cast<const DataTypeNullable *>(normalized))
        {
            normalized = nullable->getNestedType().get();
            continue;
        }

        if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(normalized))
        {
            normalized = low_cardinality->getDictionaryType().get();
            continue;
        }

        break;
    }

    if (!normalized)
        return false;

    if (typeid_cast<const DataTypeObject *>(normalized)
        || typeid_cast<const DataTypeDynamic *>(normalized)
        || typeid_cast<const DataTypeVariant *>(normalized))
    {
        WriteBufferFromOwnString out;
        type->getDefaultSerialization()->serializeTextJSON(column, row, out, format_settings);
        return isJSONEmptyContainer(out.str());
    }

    return isStructurallyEmptyContainerRow(column, row, type);
}

ConvertedTypedValue finalizeConvertedValue(
    ConvertedTypedValue result,
    std::optional<std::string_view> field_name,
    const VariantMetadata & metadata,
    bool metadata_is_shared_across_rows,
    bool preserve_empty_containers_in_shared_metadata)
{
    if (!result.present || !result.is_empty_container || !field_name.has_value())
        return result;

    if (preserve_empty_containers_in_shared_metadata)
        return result;

    if (!metadataContainsKey(metadata, *field_name))
        return {};

    if (metadata_is_shared_across_rows)
        return {};

    return result;
}

ConvertedTypedValue finalizeConvertedContainer(
    VariantValue value,
    bool is_empty_container,
    std::optional<std::string_view> field_name,
    const VariantMetadata & metadata,
    bool metadata_is_shared_across_rows,
    bool preserve_empty_containers_in_shared_metadata)
{
    ConvertedTypedValue result;
    result.present = true;
    result.is_empty_container = is_empty_container;
    result.value = std::move(value);
    return finalizeConvertedValue(std::move(result), field_name, metadata, metadata_is_shared_across_rows, preserve_empty_containers_in_shared_metadata);
}

std::vector<ConvertedTypedValue> convertDynamicColumnRange(
    const ColumnDynamic & dynamic_column,
    const VariantMetadata * const * metadata_by_row,
    size_t num_rows,
    size_t row_offset,
    const FormatSettings & format_settings,
    bool metadata_is_shared_across_rows,
    bool preserve_empty_containers_in_shared_metadata,
    std::optional<std::string_view> field_name)
{
    const auto & variant_column = dynamic_column.getVariantColumn();
    const auto shared_variant_discriminator = dynamic_column.getSharedVariantDiscriminator();

    std::vector<ConvertedTypedValue> result(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
    {
        if (!metadata_by_row[i])
            continue;

        const size_t row = row_offset + i;
        const auto discriminator = variant_column.globalDiscriminatorAt(row);
        if (discriminator == ColumnVariant::NULL_DISCRIMINATOR)
            continue;

        DataTypePtr exact_type;
        ColumnPtr exact_column;
        size_t exact_row = 0;

        if (discriminator == shared_variant_discriminator)
        {
            auto value = dynamic_column.getSharedVariant().getDataAt(variant_column.offsetAt(row));
            ReadBufferFromMemory buf(value);
            exact_type = decodeDataType(buf);
            auto column = exact_type->createColumn();
            exact_type->getDefaultSerialization()->deserializeBinary(*column, buf, format_settings);
            exact_column = std::move(column);
        }
        else
        {
            exact_type = dynamic_column.getTypeAt(row);
            if (!exact_type)
                continue;

            exact_column = variant_column.getVariantPtrByGlobalDiscriminator(discriminator);
            exact_row = variant_column.offsetAt(row);
        }

        result[i].present = true;
        if (isComplexVariantExactOutputType(exact_type))
            result[i].is_empty_container = isRowExactEmptyContainer(*exact_column, exact_row, exact_type, format_settings);
        result[i].value = makeExactValue(std::move(exact_column), std::move(exact_type), exact_row);
        result[i] = finalizeConvertedValue(
            std::move(result[i]),
            field_name,
            *metadata_by_row[i],
            metadata_is_shared_across_rows,
            preserve_empty_containers_in_shared_metadata);
    }

    return result;
}

std::map<String, VariantValue> toObjectMap(const VariantValue & value)
{
    if (value.isObject())
        return value.object_fields;

    std::map<String, VariantValue> result;
    for (const auto & [name, child] : value.tuple_elements)
        result.emplace(name, child);
    return result;
}

ConvertedTypedValue combineConvertedValues(
    std::optional<DecodedVariantValue> residual_value,
    ConvertedTypedValue typed_result,
    const FormatSettings & format_settings,
    std::optional<std::string_view> field_name,
    const VariantMetadata & metadata,
    bool metadata_is_shared_across_rows,
    bool preserve_empty_containers_in_shared_metadata)
{
    ConvertedTypedValue result;
    if (residual_value.has_value())
    {
        result.present = true;
        result.value = !typed_result.present
            ? std::move(residual_value->value)
            : mergeResidualValueWithTypedValue(std::move(*residual_value), typed_result, format_settings);
    }
    else if (typed_result.present)
    {
        result.present = true;
        result.is_empty_container = typed_result.is_empty_container;
        result.value = std::move(typed_result.value);
    }

    return finalizeConvertedValue(std::move(result), field_name, metadata, metadata_is_shared_across_rows, preserve_empty_containers_in_shared_metadata);
}

std::vector<ConvertedTypedValue> convertTypedColumnRangeImpl(
    const IColumn & column,
    const DataTypePtr & type,
    const VariantMetadata * const * metadata_by_row,
    size_t num_rows,
    size_t row_offset,
    const FormatSettings & format_settings,
    bool metadata_is_shared_across_rows,
    bool preserve_empty_containers_in_shared_metadata,
    std::optional<std::string_view> field_name,
    size_t depth);

std::vector<ConvertedTypedValue> convertRowwiseAsExactValue(
    const IColumn & column,
    const DataTypePtr & type,
    const VariantMetadata * const * metadata_by_row,
    size_t num_rows,
    size_t row_offset,
    const FormatSettings & format_settings,
    bool metadata_is_shared_across_rows,
    bool preserve_empty_containers_in_shared_metadata,
    std::optional<std::string_view> field_name)
{
    std::vector<ConvertedTypedValue> result(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
    {
        if (!metadata_by_row[i])
            continue;

        result[i].present = true;
        if (isComplexVariantExactOutputType(type))
            result[i].is_empty_container = isRowExactEmptyContainer(column, row_offset + i, type, format_settings);
        result[i].value = makeExactValue(column.getPtr(), type, row_offset + i);
        result[i] = finalizeConvertedValue(
            std::move(result[i]),
            field_name,
            *metadata_by_row[i],
            metadata_is_shared_across_rows,
            preserve_empty_containers_in_shared_metadata);
    }

    return result;
}

std::vector<ConvertedTypedValue> convertTypedColumnRangeImpl(
    const IColumn & column,
    const DataTypePtr & type,
    const VariantMetadata * const * metadata_by_row,
    size_t num_rows,
    size_t row_offset,
    const FormatSettings & format_settings,
    bool metadata_is_shared_across_rows,
    bool preserve_empty_containers_in_shared_metadata,
    std::optional<std::string_view> field_name,
    size_t depth)
{
    checkVariantReadDepth(format_settings, depth);

    if (num_rows == 0)
        return {};

    bool has_any_metadata = false;
    for (size_t i = 0; i < num_rows; ++i)
    {
        if (metadata_by_row[i])
        {
            has_any_metadata = true;
            break;
        }
    }

    if (!has_any_metadata)
        return std::vector<ConvertedTypedValue>(num_rows);

    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        const auto * nullable_column = typeid_cast<const ColumnNullable *>(&column);
        if (!nullable_column)
            return convertRowwiseAsExactValue(column, type, metadata_by_row, num_rows, row_offset, format_settings, metadata_is_shared_across_rows, preserve_empty_containers_in_shared_metadata, field_name);

        auto nested_results = convertTypedColumnRangeImpl(
            nullable_column->getNestedColumn(),
            nullable_type->getNestedType(),
            metadata_by_row,
            num_rows,
            row_offset,
            format_settings,
            metadata_is_shared_across_rows,
            preserve_empty_containers_in_shared_metadata,
            field_name,
            depth);

        std::vector<ConvertedTypedValue> result(num_rows);
        for (size_t i = 0; i < num_rows; ++i)
        {
            if (!nullable_column->isNullAt(row_offset + i))
                result[i] = std::move(nested_results[i]);
        }
        return result;
    }

    if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type.get()))
    {
        auto full_column = column.convertToFullColumnIfLowCardinality();
        if (row_offset != 0 || full_column->size() != num_rows)
            full_column = full_column->cut(row_offset, num_rows);
        return convertTypedColumnRangeImpl(
            *full_column,
            low_cardinality->getDictionaryType(),
            metadata_by_row,
            num_rows,
            0,
            format_settings,
            metadata_is_shared_across_rows,
            preserve_empty_containers_in_shared_metadata,
            field_name,
            depth);
    }

    if (typeid_cast<const DataTypeDynamic *>(type.get()))
    {
        const auto * dynamic_column = typeid_cast<const ColumnDynamic *>(&column);
        if (!dynamic_column)
            return convertRowwiseAsExactValue(column, type, metadata_by_row, num_rows, row_offset, format_settings, metadata_is_shared_across_rows, preserve_empty_containers_in_shared_metadata, field_name);

        return convertDynamicColumnRange(
            *dynamic_column,
            metadata_by_row,
            num_rows,
            row_offset,
            format_settings,
            metadata_is_shared_across_rows,
            preserve_empty_containers_in_shared_metadata,
            field_name);
    }

    if (auto wrapper = tryGetVariantWrapperLayout(type))
    {
        const auto * tuple_column = typeid_cast<const ColumnTuple *>(&column);
        if (!tuple_column)
            return convertRowwiseAsExactValue(column, type, metadata_by_row, num_rows, row_offset, format_settings, metadata_is_shared_across_rows, preserve_empty_containers_in_shared_metadata, field_name);

        std::vector<ConvertedTypedValue> typed_results;
        if (wrapper->typed_value_pos.has_value())
        {
            typed_results = convertTypedColumnRangeImpl(
                tuple_column->getColumn(*wrapper->typed_value_pos),
                wrapper->typed_value_type,
                metadata_by_row,
                num_rows,
                row_offset,
                format_settings,
                metadata_is_shared_across_rows,
                preserve_empty_containers_in_shared_metadata,
                std::nullopt,
                depth);
        }

        const auto & value_column = tuple_column->getColumn(wrapper->value_pos);
        std::vector<ConvertedTypedValue> result(num_rows);
        for (size_t i = 0; i < num_rows; ++i)
        {
            if (!metadata_by_row[i])
                continue;

            std::optional<DecodedVariantValue> residual_value;
            if (hasVariantWrapperResidualValue(value_column, row_offset + i, *wrapper))
            {
                auto value_blob = getVariantWrapperResidualValue(value_column, row_offset + i);
                if (value_blob.has_value())
                    residual_value = decodeValue(*metadata_by_row[i], *value_blob, format_settings);
            }

            ConvertedTypedValue typed_result;
            if (wrapper->typed_value_pos.has_value())
                typed_result = std::move(typed_results[i]);

            result[i] = combineConvertedValues(
                std::move(residual_value),
                std::move(typed_result),
                format_settings,
                field_name,
                *metadata_by_row[i],
                metadata_is_shared_across_rows,
                preserve_empty_containers_in_shared_metadata);
        }

        return result;
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        const auto * tuple_column = typeid_cast<const ColumnTuple *>(&column);
        if (!tuple_column || !tuple_type->hasExplicitNames())
            return convertRowwiseAsExactValue(column, type, metadata_by_row, num_rows, row_offset, format_settings, metadata_is_shared_across_rows, preserve_empty_containers_in_shared_metadata, field_name);

        std::vector<std::vector<std::pair<String, VariantValue>>> tuple_rows(num_rows);
        std::vector<bool> has_values(num_rows, false);
        std::vector<std::vector<DataTypePtr>> tuple_field_exact_types(num_rows, std::vector<DataTypePtr>(tuple_type->getElements().size()));
        for (size_t i = 0; i < tuple_type->getElements().size(); ++i)
        {
            const auto & child_name = tuple_type->getNameByPosition(i + 1);
            auto child_results = convertTypedColumnRangeImpl(
                tuple_column->getColumn(i),
                tuple_type->getElement(i),
                metadata_by_row,
                num_rows,
                row_offset,
                format_settings,
                metadata_is_shared_across_rows,
                preserve_empty_containers_in_shared_metadata,
                std::optional<std::string_view>(child_name),
                depth + 1);

            for (size_t row = 0; row < num_rows; ++row)
            {
                if (!child_results[row].present)
                    continue;

                tuple_field_exact_types[row][i] = child_results[row].value.exact_type;
                tuple_rows[row].emplace_back(child_name, std::move(child_results[row].value));
                has_values[row] = true;
            }
        }

        std::vector<ConvertedTypedValue> result(num_rows);
        for (size_t row = 0; row < num_rows; ++row)
        {
            if (!metadata_by_row[row])
                continue;

            if (!has_values[row] && !tuple_type->getElements().empty())
                continue;

            DataTypes exact_field_types;
            Names exact_field_names;
            exact_field_types.reserve(tuple_type->getElements().size());
            exact_field_names.reserve(tuple_type->getElements().size());
            bool has_exact_type = true;
            for (size_t i = 0; i < tuple_type->getElements().size(); ++i)
            {
                DataTypePtr field_exact_type = tuple_field_exact_types[row][i];
                if (!field_exact_type)
                {
                    if (auto logical_type = getLogicalTypedValueType(tuple_type->getElement(i)))
                    {
                        field_exact_type = makeNullableSafe(logical_type);
                    }
                    else
                    {
                        has_exact_type = false;
                        break;
                    }
                }

                exact_field_types.emplace_back(std::move(field_exact_type));
                exact_field_names.emplace_back(tuple_type->getNameByPosition(i + 1));
            }

            bool tuple_is_empty = tuple_rows[row].empty();
            result[row] = finalizeConvertedContainer(
                makeTupleValue(
                    std::move(tuple_rows[row]),
                    has_exact_type ? std::make_shared<DataTypeTuple>(std::move(exact_field_types), std::move(exact_field_names)) : DataTypePtr {}),
                tuple_is_empty,
                field_name,
                *metadata_by_row[row],
                metadata_is_shared_across_rows,
                preserve_empty_containers_in_shared_metadata);
        }

        return result;
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        const auto * array_column = typeid_cast<const ColumnArray *>(&column);
        if (!array_column)
            return convertRowwiseAsExactValue(column, type, metadata_by_row, num_rows, row_offset, format_settings, metadata_is_shared_across_rows, preserve_empty_containers_in_shared_metadata, field_name);

        const auto & offsets = array_column->getOffsets();
        size_t begin = row_offset == 0 ? 0 : offsets[row_offset - 1];
        size_t end = offsets[row_offset + num_rows - 1];

        std::vector<const VariantMetadata *> child_metadata;
        child_metadata.reserve(end - begin);
        for (size_t row = 0; row < num_rows; ++row)
        {
            size_t element_begin = row == 0 ? begin : offsets[row_offset + row - 1];
            size_t element_end = offsets[row_offset + row];
            child_metadata.insert(child_metadata.end(), element_end - element_begin, metadata_by_row[row]);
        }

        auto child_results = convertTypedColumnRangeImpl(
            array_column->getData(),
            array_type->getNestedType(),
            child_metadata.data(),
            child_metadata.size(),
            begin,
            format_settings,
            metadata_is_shared_across_rows,
            preserve_empty_containers_in_shared_metadata,
            std::nullopt,
            depth + 1);

        std::vector<ConvertedTypedValue> result(num_rows);
        for (size_t row = 0; row < num_rows; ++row)
        {
            if (!metadata_by_row[row])
                continue;

            size_t element_begin = row == 0 ? begin : offsets[row_offset + row - 1];
            size_t element_end = offsets[row_offset + row];

            std::vector<VariantValue> values;
            values.reserve(element_end - element_begin);
            for (size_t pos = element_begin; pos < element_end; ++pos)
            {
                auto & child_result = child_results[pos - begin];
                values.emplace_back(child_result.present ? std::move(child_result.value) : VariantValue {});
            }

            bool array_is_empty = values.empty();
            DataTypePtr exact_array_type = tryBuildExactArrayType(values);
            result[row] = finalizeConvertedContainer(
                makeArrayValue(std::move(values), std::move(exact_array_type)),
                array_is_empty,
                field_name,
                *metadata_by_row[row],
                metadata_is_shared_across_rows,
                preserve_empty_containers_in_shared_metadata);
        }

        return result;
    }

    return convertRowwiseAsExactValue(column, type, metadata_by_row, num_rows, row_offset, format_settings, metadata_is_shared_across_rows, preserve_empty_containers_in_shared_metadata, field_name);
}

}

bool isTypedArrayDefaultFiller(const ConvertedTypedValue & typed_value)
{
    return typed_value.present
        && typed_value.is_empty_container
        && typed_value.value.isArray();
}

VariantValue mergeResidualValueWithTypedValue(
    DecodedVariantValue residual_value,
    const ConvertedTypedValue & typed_value,
    const FormatSettings & format_settings)
{
    if (isTypedArrayDefaultFiller(typed_value))
        return std::move(residual_value.value);

    return mergeValues(std::move(residual_value.value), typed_value.value, format_settings);
}

VariantValue mergeValues(VariantValue base, const VariantValue & patch, const FormatSettings & format_settings)
{
    auto merge_impl = [&](auto && self, VariantValue current, const VariantValue & delta, size_t depth) -> VariantValue
    {
        checkVariantReadDepth(format_settings, depth);

        if (current.isNull())
            return delta;

        if (delta.isNull())
            return current;

        if (current.isArray() && delta.isArray())
        {
            auto & current_array = current.array_elements;
            const auto & delta_array = delta.array_elements;
            if (delta_array.size() > current_array.size())
                current_array.resize(delta_array.size());

            for (size_t i = 0; i < delta_array.size(); ++i)
                current_array[i] = self(self, std::move(current_array[i]), delta_array[i], depth + 1);

            current.exact_type = tryBuildExactArrayType(current_array);
            return current;
        }

        if (current.isObjectLike() && delta.isObjectLike())
        {
            auto current_fields = toObjectMap(current);
            const auto delta_fields = toObjectMap(delta);
            for (const auto & [key, delta_value] : delta_fields)
            {
                auto it = current_fields.find(key);
                if (it == current_fields.end())
                {
                    current_fields.emplace(key, delta_value);
                    continue;
                }

                it->second = self(self, std::move(it->second), delta_value, depth + 1);
            }

            return makeObjectValue(
                std::move(current_fields),
                std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON));
        }

        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Malformed `Parquet` `VARIANT`: shredded typed value and residual value both contain incompatible non-`NULL` data");
    };

    return merge_impl(merge_impl, std::move(base), patch, 1);
}

std::vector<ConvertedTypedValue> convertTypedColumnRange(
    const IColumn & column,
    const DataTypePtr & type,
    const VariantMetadata * const * metadata_by_row,
    size_t num_rows,
    size_t row_offset,
    const FormatSettings & format_settings,
    bool metadata_is_shared_across_rows,
    bool preserve_empty_containers_in_shared_metadata,
    std::optional<std::string_view> field_name,
    size_t depth)
{
    return convertTypedColumnRangeImpl(
        column,
        type,
        metadata_by_row,
        num_rows,
        row_offset,
        format_settings,
        metadata_is_shared_across_rows,
        preserve_empty_containers_in_shared_metadata,
        field_name,
        depth);
}

}
