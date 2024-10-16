#include <DataTypes/Serializations/SerializationDynamicElement.h>
#include <DataTypes/Serializations/SerializationVariantElement.h>
#include <DataTypes/Serializations/SerializationVariantElementNullMap.h>
#include <DataTypes/Serializations/SerializationDynamic.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnLowCardinality.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


struct DeserializeBinaryBulkStateDynamicElement : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr structure_state;
    SerializationPtr variant_serialization;
    ISerialization::DeserializeBinaryBulkStatePtr variant_element_state;
    bool read_from_shared_variant;
    ColumnPtr shared_variant;
};

void SerializationDynamicElement::enumerateStreams(
    DB::ISerialization::EnumerateStreamsSettings & settings,
    const DB::ISerialization::StreamCallback & callback,
    const DB::ISerialization::SubstreamData & data) const
{
    settings.path.push_back(Substream::DynamicStructure);
    callback(settings.path);
    settings.path.pop_back();

    /// If we didn't deserialize prefix yet, we don't know if we actually have this variant in Dynamic column,
    /// so we cannot enumerate variant streams.
    if (!data.deserialize_state)
        return;

    auto * deserialize_state = checkAndGetState<DeserializeBinaryBulkStateDynamicElement>(data.deserialize_state);
    /// If we don't have this variant, no need to enumerate streams for it as we won't read from any stream.
    if (!deserialize_state->variant_serialization)
        return;

    settings.path.push_back(Substream::DynamicData);
    auto variant_data = SubstreamData(deserialize_state->variant_serialization)
                            .withType(data.type)
                            .withColumn(data.column)
                            .withSerializationInfo(data.serialization_info)
                            .withDeserializeState(deserialize_state->variant_element_state);
    settings.path.back().data = variant_data;
    deserialize_state->variant_serialization->enumerateStreams(settings, callback, variant_data);
    settings.path.pop_back();
}

void SerializationDynamicElement::serializeBinaryBulkStatePrefix(const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationDynamicElement");
}

void SerializationDynamicElement::serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationDynamicElement");
}

void SerializationDynamicElement::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    DeserializeBinaryBulkStatePtr structure_state = SerializationDynamic::deserializeDynamicStructureStatePrefix(settings, cache);
    if (!structure_state)
        return;

    auto dynamic_element_state = std::make_shared<DeserializeBinaryBulkStateDynamicElement>();
    dynamic_element_state->structure_state = std::move(structure_state);
    const auto & variant_type = assert_cast<const DataTypeVariant &>(
        *checkAndGetState<SerializationDynamic::DeserializeBinaryBulkStateDynamicStructure>(dynamic_element_state->structure_state)->variant_type);
    /// Check if we actually have required element in the Variant.
    if (auto global_discr = variant_type.tryGetVariantDiscriminator(dynamic_element_name))
    {
        settings.path.push_back(Substream::DynamicData);
        if (is_null_map_subcolumn)
            dynamic_element_state->variant_serialization = std::make_shared<SerializationVariantElementNullMap>(dynamic_element_name, *global_discr);
        else
            dynamic_element_state->variant_serialization = std::make_shared<SerializationVariantElement>(nested_serialization, dynamic_element_name, *global_discr);
        dynamic_element_state->variant_serialization->deserializeBinaryBulkStatePrefix(settings, dynamic_element_state->variant_element_state, cache);
        dynamic_element_state->read_from_shared_variant = false;
        settings.path.pop_back();
    }
    /// If we don't have this element in the Variant, we will read shared variant and try to find it there.
    else
    {
        auto shared_variant_global_discr = variant_type.tryGetVariantDiscriminator(ColumnDynamic::getSharedVariantTypeName());
        chassert(shared_variant_global_discr.has_value());
        settings.path.push_back(Substream::DynamicData);
        dynamic_element_state->variant_serialization = std::make_shared<SerializationVariantElement>(
            ColumnDynamic::getSharedVariantDataType()->getDefaultSerialization(),
            ColumnDynamic::getSharedVariantTypeName(),
            *shared_variant_global_discr);
        dynamic_element_state->variant_serialization->deserializeBinaryBulkStatePrefix(settings, dynamic_element_state->variant_element_state, cache);
        dynamic_element_state->read_from_shared_variant = true;
        settings.path.pop_back();
    }

    state = std::move(dynamic_element_state);
}

void SerializationDynamicElement::serializeBinaryBulkWithMultipleStreams(const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationDynamicElement");
}

void SerializationDynamicElement::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & result_column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    if (!state)
    {
        if (is_null_map_subcolumn)
        {
            auto mutable_column = result_column->assumeMutable();
            auto & data = assert_cast<ColumnUInt8 &>(*mutable_column).getData();
            data.resize_fill(data.size() + limit, 1);
        }

        return;
    }

    auto * dynamic_element_state = checkAndGetState<DeserializeBinaryBulkStateDynamicElement>(state);

    /// Check if this subcolumn should not be read from shared variant.
    /// In this case just read data from the corresponding variant.
    if (!dynamic_element_state->read_from_shared_variant)
    {
        settings.path.push_back(Substream::DynamicData);
        dynamic_element_state->variant_serialization->deserializeBinaryBulkWithMultipleStreams(
            result_column, limit, settings, dynamic_element_state->variant_element_state, cache);
        settings.path.pop_back();
    }
    /// Otherwise, read the shared variant column and extract requested type from it.
    else
    {
        settings.path.push_back(Substream::DynamicData);
        /// Initialize shared_variant column if needed.
        if (result_column->empty())
            dynamic_element_state->shared_variant = makeNullable(ColumnDynamic::getSharedVariantDataType()->createColumn());
        size_t prev_size = result_column->size();
        dynamic_element_state->variant_serialization->deserializeBinaryBulkWithMultipleStreams(
            dynamic_element_state->shared_variant, limit, settings, dynamic_element_state->variant_element_state, cache);
        settings.path.pop_back();

        /// If we need to read a subcolumn from variant column, create an empty variant column, fill it and extract subcolumn.
        auto variant_type = DataTypeFactory::instance().get(dynamic_element_name);
        auto result_type = makeNullableOrLowCardinalityNullableSafe(variant_type);
        MutableColumnPtr variant_column = nested_subcolumn.empty() || is_null_map_subcolumn ? result_column->assumeMutable() : result_type->createColumn();
        variant_column->reserve(variant_column->size() + limit);
        MutableColumnPtr non_nullable_variant_column = variant_column->assumeMutable();
        NullMap * null_map = nullptr;
        bool is_low_cardinality_nullable = isColumnLowCardinalityNullable(*variant_column);
        /// Resulting subolumn can be Nullable, but value is serialized in shared variant as non-Nullable.
        /// Extract non-nullable column and remember the null map to fill it during deserialization.
        if (isColumnNullable(*variant_column))
        {
            auto & nullable_variant_column = assert_cast<ColumnNullable &>(*variant_column);
            non_nullable_variant_column = nullable_variant_column.getNestedColumnPtr()->assumeMutable();
            null_map = &nullable_variant_column.getNullMapData();
        }
        else if (is_null_map_subcolumn)
        {
            null_map = &assert_cast<ColumnUInt8 &>(*variant_column).getData();
        }

        auto variant_serialization = variant_type->getDefaultSerialization();

        const auto & nullable_shared_variant = assert_cast<const ColumnNullable &>(*dynamic_element_state->shared_variant);
        const auto & shared_null_map = nullable_shared_variant.getNullMapData();
        const auto & shared_variant = assert_cast<const ColumnString &>(nullable_shared_variant.getNestedColumn());
        const FormatSettings format_settings;
        for (size_t i = prev_size; i != shared_variant.size(); ++i)
        {
            if (!shared_null_map[i])
            {
                auto value = shared_variant.getDataAt(i);
                ReadBufferFromMemory buf(value.data, value.size);
                auto type = decodeDataType(buf);
                if (type->getName() == dynamic_element_name)
                {
                    /// When requested type is LowCardinality the subcolumn type name will be LowCardinality(Nullable).
                    /// Value in shared variant is serialized as LowCardinality and we cannot simply deserialize it
                    /// inside LowCardinality(Nullable) column (it will try to deserialize null bit). In this case we
                    /// have to create temporary LowCardinality column, deserialize value into it and insert it into
                    /// resulting LowCardinality(Nullable) (insertion from LowCardinality column to LowCardinality(Nullable)
                    /// column is allowed).
                    if (is_low_cardinality_nullable)
                    {
                        auto tmp_column = variant_type->createColumn();
                        variant_serialization->deserializeBinary(*tmp_column, buf, format_settings);
                        non_nullable_variant_column->insertFrom(*tmp_column, 0);
                    }
                    else if (is_null_map_subcolumn)
                    {
                        null_map->push_back(0);
                    }
                    else
                    {
                        variant_serialization->deserializeBinary(*non_nullable_variant_column, buf, format_settings);
                        if (null_map)
                            null_map->push_back(0);
                    }
                }
                else
                {
                    variant_column->insertDefault();
                }
            }
            else
            {
                variant_column->insertDefault();
            }
        }

        /// Extract nested subcolumn if needed.
        if (!nested_subcolumn.empty() && !is_null_map_subcolumn)
        {
            auto subcolumn = result_type->getSubcolumn(nested_subcolumn, variant_column->getPtr());
            result_column->assumeMutable()->insertRangeFrom(*subcolumn, 0, subcolumn->size());
        }
    }
}

}
