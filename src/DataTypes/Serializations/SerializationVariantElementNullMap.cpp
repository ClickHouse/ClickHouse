#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationVariant.h>
#include <DataTypes/Serializations/SerializationVariantElement.h>
#include <DataTypes/Serializations/SerializationVariantElementNullMap.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

struct DeserializeBinaryBulkStateVariantElementNullMap : public ISerialization::DeserializeBinaryBulkState
{
    /// During deserialization discriminators streams can be shared.
    /// For example we can read several variant elements together: "select v.UInt32, v.String.null from table",
    /// or we can read the whole variant and some of variant elements or their subcolumns: "select v, v.UInt32.null from table".
    /// To read the same column from the same stream more than once we use substream cache,
    /// but this cache stores the whole column, not only the current range.
    /// During deserialization of variant elements or their subcolumns discriminators column is not stored
    /// in the result column, so we need to store them inside deserialization state, so we can use
    /// substream cache correctly.
    ColumnPtr discriminators;
    ISerialization::DeserializeBinaryBulkStatePtr discriminators_state;
};

void SerializationVariantElementNullMap::enumerateStreams(
    DB::ISerialization::EnumerateStreamsSettings & settings,
    const DB::ISerialization::StreamCallback & callback,
    const DB::ISerialization::SubstreamData &) const
{
    /// We will need stream for discriminators during deserialization.
    settings.path.push_back(Substream::VariantDiscriminators);
    callback(settings.path);
    settings.path.pop_back();
}

void SerializationVariantElementNullMap::serializeBinaryBulkStatePrefix(
    const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationVariantElementNullMap");
}

void SerializationVariantElementNullMap::serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationVariantElementNullMap");
}

void SerializationVariantElementNullMap::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    DeserializeBinaryBulkStatePtr discriminators_state = SerializationVariant::deserializeDiscriminatorsStatePrefix(settings, cache);
    if (!discriminators_state)
        return;

    auto variant_element_null_map_state = std::make_shared<DeserializeBinaryBulkStateVariantElementNullMap>();
    variant_element_null_map_state->discriminators_state = std::move(discriminators_state);
    state = std::move(variant_element_null_map_state);
}

void SerializationVariantElementNullMap::serializeBinaryBulkWithMultipleStreams(
    const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationVariantElementNullMap");
}

void SerializationVariantElementNullMap::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & result_column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    /// Deserialize discriminators from Variant column.
    settings.path.push_back(Substream::VariantDiscriminators);

    DeserializeBinaryBulkStateVariantElementNullMap * variant_element_null_map_state = nullptr;
    std::optional<size_t> variant_limit;
    if (auto cached_discriminators = getFromSubstreamsCache(cache, settings.path))
    {
        variant_element_null_map_state = checkAndGetState<DeserializeBinaryBulkStateVariantElementNullMap>(state);
        variant_element_null_map_state->discriminators = cached_discriminators;
    }
    else if (auto * discriminators_stream = settings.getter(settings.path))
    {
        variant_element_null_map_state = checkAndGetState<DeserializeBinaryBulkStateVariantElementNullMap>(state);
        auto * discriminators_state = checkAndGetState<SerializationVariant::DeserializeBinaryBulkStateVariantDiscriminators>(
            variant_element_null_map_state->discriminators_state);

        /// If we started to read a new column, reinitialize discriminators column in deserialization state.
        if (!variant_element_null_map_state->discriminators || result_column->empty())
            variant_element_null_map_state->discriminators = ColumnVariant::ColumnDiscriminators::create();

        /// Deserialize discriminators according to serialization mode.
        if (discriminators_state->mode.value == SerializationVariant::DiscriminatorsSerializationMode::BASIC)
            SerializationNumber<ColumnVariant::Discriminator>().deserializeBinaryBulk(
                *variant_element_null_map_state->discriminators->assumeMutable(), *discriminators_stream, limit, 0);
        else
            variant_limit = SerializationVariantElement::deserializeCompactDiscriminators(
                variant_element_null_map_state->discriminators,
                variant_discriminator,
                limit,
                discriminators_stream,
                settings.continuous_reading,
                variant_element_null_map_state->discriminators_state,
                this);

        addToSubstreamsCache(cache, settings.path, variant_element_null_map_state->discriminators);
    }
    else
    {
        /// There is no such stream or cached data, it means that there is no Variant column in this part (it could happen after alter table add column).
        /// In such cases columns are filled with default values, but for null-map column default value should be 1, not 0. Fill column with 1 here instead.
        MutableColumnPtr mutable_column = result_column->assumeMutable();
        auto & data = assert_cast<ColumnUInt8 &>(*mutable_column).getData();
        data.resize_fill(data.size() + limit, 1);
        settings.path.pop_back();
        return;
    }
    settings.path.pop_back();

    MutableColumnPtr mutable_column = result_column->assumeMutable();
    auto & data = assert_cast<ColumnUInt8 &>(*mutable_column).getData();
    /// Check if there are no such variant in read range.
    if (variant_limit && *variant_limit == 0)
    {
        data.resize_fill(data.size() + limit, 1);
    }
    /// Check if there is only our variant in read range.
    else if (variant_limit && *variant_limit == limit)
    {
        data.resize_fill(data.size() + limit, 0);
    }
    /// Iterate through new discriminators to calculate the null map of our variant.
    else
    {
        const auto & discriminators_data
            = assert_cast<const ColumnVariant::ColumnDiscriminators &>(*variant_element_null_map_state->discriminators).getData();
        size_t discriminators_offset = variant_element_null_map_state->discriminators->size() - limit;
        for (size_t i = discriminators_offset; i != discriminators_data.size(); ++i)
            data.push_back(discriminators_data[i] != variant_discriminator);
    }
}

SerializationVariantElementNullMap::VariantNullMapSubcolumnCreator::VariantNullMapSubcolumnCreator(
    const ColumnPtr & local_discriminators_,
    const String & variant_element_name_,
    ColumnVariant::Discriminator global_variant_discriminator_,
    ColumnVariant::Discriminator local_variant_discriminator_)
    : local_discriminators(local_discriminators_)
    , variant_element_name(variant_element_name_)
    , global_variant_discriminator(global_variant_discriminator_)
    , local_variant_discriminator(local_variant_discriminator_)
{
}

DataTypePtr SerializationVariantElementNullMap::VariantNullMapSubcolumnCreator::create(const DB::DataTypePtr &) const
{
    return std::make_shared<DataTypeUInt8>();
}

SerializationPtr SerializationVariantElementNullMap::VariantNullMapSubcolumnCreator::create(const DB::SerializationPtr &) const
{
    return std::make_shared<SerializationVariantElementNullMap>(variant_element_name, global_variant_discriminator);
}

ColumnPtr SerializationVariantElementNullMap::VariantNullMapSubcolumnCreator::create(const DB::ColumnPtr &) const
{
    /// Iterate through discriminators and create null-map for our variant.
    auto null_map_col = ColumnUInt8::create();
    auto & null_map_data = null_map_col->getData();
    null_map_data.reserve(local_discriminators->size());
    const auto & local_discriminators_data = assert_cast<const ColumnVariant::ColumnDiscriminators &>(*local_discriminators).getData();
    for (auto local_discr : local_discriminators_data)
        null_map_data.push_back(local_discr != local_variant_discriminator);

    return null_map_col;
}


}
