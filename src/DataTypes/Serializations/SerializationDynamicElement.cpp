#include <DataTypes/Serializations/SerializationDynamicElement.h>
#include <DataTypes/Serializations/SerializationVariantElement.h>
#include <DataTypes/Serializations/SerializationDynamic.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeFactory.h>
#include <Columns/ColumnDynamic.h>
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
    const auto & variant_type = checkAndGetState<SerializationDynamic::DeserializeBinaryBulkStateDynamicStructure>(dynamic_element_state->structure_state)->variant_type;
    /// Check if we actually have required element in the Variant.
    if (auto global_discr = assert_cast<const DataTypeVariant &>(*variant_type).tryGetVariantDiscriminator(dynamic_element_name))
    {
        settings.path.push_back(Substream::DynamicData);
        dynamic_element_state->variant_serialization = std::make_shared<SerializationVariantElement>(nested_serialization, dynamic_element_name, *global_discr);
        dynamic_element_state->variant_serialization->deserializeBinaryBulkStatePrefix(settings, dynamic_element_state->variant_element_state, cache);
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
        return;

    auto * dynamic_element_state = checkAndGetState<DeserializeBinaryBulkStateDynamicElement>(state);

    if (dynamic_element_state->variant_serialization)
    {
        settings.path.push_back(Substream::DynamicData);
        dynamic_element_state->variant_serialization->deserializeBinaryBulkWithMultipleStreams(result_column, limit, settings, dynamic_element_state->variant_element_state, cache);
        settings.path.pop_back();
    }
    else
    {
        auto mutable_column = result_column->assumeMutable();
        mutable_column->insertManyDefaults(limit);
        result_column = std::move(mutable_column);
    }
}

}
