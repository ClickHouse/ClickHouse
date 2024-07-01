#include <DataTypes/Serializations/SerializationVariantElement.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

struct DeserializeBinaryBulkStateVariantElement : public ISerialization::DeserializeBinaryBulkState
{
    /// During deserialization discriminators and variant streams can be shared.
    /// For example we can read several variant elements together: "select v.UInt32, v.String from table",
    /// or we can read the whole variant and some of variant elements: "select v, v.UInt32 from table".
    /// To read the same column from the same stream more than once we use substream cache,
    /// but this cache stores the whole column, not only the current range.
    /// During deserialization of variant element discriminators and variant columns are not stored
    /// in the result column, so we need to store them inside deserialization state, so we can use
    /// substream cache correctly.
    ColumnPtr discriminators;
    ColumnPtr variant;

    ISerialization::DeserializeBinaryBulkStatePtr variant_element_state;
};

void SerializationVariantElement::enumerateStreams(
    DB::ISerialization::EnumerateStreamsSettings & settings,
    const DB::ISerialization::StreamCallback & callback,
    const DB::ISerialization::SubstreamData & data) const
{
    /// We will need stream for discriminators during deserialization.
    settings.path.push_back(Substream::VariantDiscriminators);
    callback(settings.path);
    settings.path.pop_back();

    const auto * deserialize_state = data.deserialize_state ? checkAndGetState<DeserializeBinaryBulkStateVariantElement>(data.deserialize_state) : nullptr;
    addVariantToPath(settings.path);
    auto nested_data = SubstreamData(nested_serialization)
                       .withType(data.type ? removeNullableOrLowCardinalityNullable(data.type) : nullptr)
                       .withColumn(data.column ? removeNullableOrLowCardinalityNullable(data.column) : nullptr)
                       .withSerializationInfo(data.serialization_info)
                       .withDeserializeState(deserialize_state ? deserialize_state->variant_element_state : nullptr);
    settings.path.back().data = nested_data;
    nested_serialization->enumerateStreams(settings, callback, nested_data);
    removeVariantFromPath(settings.path);
}

void SerializationVariantElement::serializeBinaryBulkStatePrefix(const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationVariantElement");
}

void SerializationVariantElement::serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationVariantElement");
}

void SerializationVariantElement::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    auto variant_element_state = std::make_shared<DeserializeBinaryBulkStateVariantElement>();

    addVariantToPath(settings.path);
    nested_serialization->deserializeBinaryBulkStatePrefix(settings, variant_element_state->variant_element_state, cache);
    removeVariantFromPath(settings.path);

    state = std::move(variant_element_state);
}

void SerializationVariantElement::serializeBinaryBulkWithMultipleStreams(const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationVariantElement");
}

void SerializationVariantElement::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & result_column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto * variant_element_state = checkAndGetState<DeserializeBinaryBulkStateVariantElement>(state);

    /// First, deserialize discriminators from Variant column.
    settings.path.push_back(Substream::VariantDiscriminators);
    if (auto cached_discriminators = getFromSubstreamsCache(cache, settings.path))
    {
        variant_element_state->discriminators = cached_discriminators;
    }
    else
    {
        auto * discriminators_stream = settings.getter(settings.path);
        if (!discriminators_stream)
            return;

        /// If we started to read a new column, reinitialize discriminators column in deserialization state.
        if (!variant_element_state->discriminators || result_column->empty())
            variant_element_state->discriminators = ColumnVariant::ColumnDiscriminators::create();

        SerializationNumber<ColumnVariant::Discriminator>().deserializeBinaryBulk(*variant_element_state->discriminators->assumeMutable(), *discriminators_stream, limit, 0);
        addToSubstreamsCache(cache, settings.path, variant_element_state->discriminators);
    }
    settings.path.pop_back();

    /// Iterate through new discriminators to calculate the limit for our variant.
    const auto & discriminators_data = assert_cast<const ColumnVariant::ColumnDiscriminators &>(*variant_element_state->discriminators).getData();
    size_t discriminators_offset = variant_element_state->discriminators->size() - limit;
    size_t variant_limit = 0;
    for (size_t i = discriminators_offset; i != discriminators_data.size(); ++i)
        variant_limit += (discriminators_data[i] == variant_discriminator);

    /// Now we know the limit for our variant and can deserialize it.

    /// If result column is Nullable, fill null map and extract nested column.
    MutableColumnPtr mutable_column = result_column->assumeMutable();
    if (isColumnNullable(*mutable_column))
    {
        auto & nullable_column = assert_cast<ColumnNullable &>(*mutable_column);
        NullMap & null_map = nullable_column.getNullMapData();
        /// If we have only our discriminator in range, fill null map with 0.
        if (variant_limit == limit)
        {
            null_map.resize_fill(null_map.size() + limit, 0);
        }
        /// If no our discriminator in current range, fill null map with 1.
        else if (variant_limit == 0)
        {
            null_map.resize_fill(null_map.size() + limit, 1);
        }
        /// Otherwise we should iterate through discriminators to fill null map.
        else
        {
            null_map.reserve(null_map.size() + limit);
            for (size_t i = discriminators_offset; i != discriminators_data.size(); ++i)
                null_map.push_back(discriminators_data[i] != variant_discriminator);
        }

        mutable_column = nullable_column.getNestedColumnPtr()->assumeMutable();
    }

    /// If we started to read a new column, reinitialize variant column in deserialization state.
    if (!variant_element_state->variant || mutable_column->empty())
    {
        variant_element_state->variant = mutable_column->cloneEmpty();

        /// When result column is LowCardinality(Nullable(T)) we should
        /// remove Nullable from variant column before deserialization.
        if (isColumnLowCardinalityNullable(*mutable_column))
            assert_cast<ColumnLowCardinality &>(*variant_element_state->variant->assumeMutable()).nestedRemoveNullable();
    }

    /// If nothing to deserialize, just insert defaults.
    if (variant_limit == 0)
    {
        mutable_column->insertManyDefaults(limit);
        return;
    }

    addVariantToPath(settings.path);
    nested_serialization->deserializeBinaryBulkWithMultipleStreams(variant_element_state->variant, variant_limit, settings, variant_element_state->variant_element_state, cache);
    removeVariantFromPath(settings.path);

    /// If nothing was deserialized when variant_limit > 0
    /// it means that we don't have a stream for such sub-column.
    /// It may happen during ALTER MODIFY column with Variant extension.
    /// In this case we should just insert default values.
    if (variant_element_state->variant->empty())
    {
        mutable_column->insertManyDefaults(limit);
        return;
    }

    size_t variant_offset = variant_element_state->variant->size() - variant_limit;

    /// If we have only our discriminator in range, insert the whole range to result column.
    if (variant_limit == limit)
    {
        mutable_column->insertRangeFrom(*variant_element_state->variant, variant_offset, variant_limit);
    }
    /// Otherwise iterate through discriminators and insert value from variant or default value depending on the discriminator.
    else
    {
        for (size_t i = discriminators_offset; i != discriminators_data.size(); ++i)
        {
            if (discriminators_data[i] == variant_discriminator)
                mutable_column->insertFrom(*variant_element_state->variant, variant_offset++);
            else
                mutable_column->insertDefault();
        }
    }
}

void SerializationVariantElement::addVariantToPath(DB::ISerialization::SubstreamPath & path) const
{
    path.push_back(Substream::VariantElements);
    path.push_back(Substream::VariantElement);
    path.back().variant_element_name = variant_element_name;
}

void SerializationVariantElement::removeVariantFromPath(DB::ISerialization::SubstreamPath & path) const
{
    path.pop_back();
    path.pop_back();
}

SerializationVariantElement::VariantSubcolumnCreator::VariantSubcolumnCreator(
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

DataTypePtr SerializationVariantElement::VariantSubcolumnCreator::create(const DB::DataTypePtr & prev) const
{
    return makeNullableOrLowCardinalityNullableSafe(prev);
}

SerializationPtr SerializationVariantElement::VariantSubcolumnCreator::create(const DB::SerializationPtr & prev) const
{
    return std::make_shared<SerializationVariantElement>(prev, variant_element_name, global_variant_discriminator);
}

ColumnPtr SerializationVariantElement::VariantSubcolumnCreator::create(const DB::ColumnPtr & prev) const
{
    /// Case when original Variant column contained only one non-empty variant and no NULLs.
    /// In this case just use this variant.
    if (prev->size() == local_discriminators->size())
        return makeNullableOrLowCardinalityNullableSafe(prev);

    /// If this variant is empty, fill result column with default values.
    if (prev->empty())
    {
        auto res = makeNullableOrLowCardinalityNullableSafe(prev)->cloneEmpty();
        res->insertManyDefaults(local_discriminators->size());
        return res;
    }

    /// In general case we should iterate through discriminators and create null-map for our variant.
    NullMap null_map;
    null_map.reserve(local_discriminators->size());
    const auto & local_discriminators_data = assert_cast<const ColumnVariant::ColumnDiscriminators &>(*local_discriminators).getData();
    for (auto local_discr : local_discriminators_data)
        null_map.push_back(local_discr != local_variant_discriminator);

    /// Now we can create new column from null-map and variant column using IColumn::expand.
    auto res_column = IColumn::mutate(prev);

    /// Special case for LowCardinality. We want the result to be LowCardinality(Nullable),
    /// but we don't have a good way to apply null-mask for LowCardinality(), so, we first
    /// convert our column to LowCardinality(Nullable()) and then use expand which will
    /// fill rows with 0 in mask with default value (that is NULL).
    if (prev->lowCardinality())
        res_column = assert_cast<ColumnLowCardinality &>(*res_column).cloneNullable();

    res_column->expand(null_map, /*inverted = */ true);

    if (res_column->canBeInsideNullable())
    {
        auto null_map_col = ColumnUInt8::create();
        null_map_col->getData() = std::move(null_map);
        return ColumnNullable::create(std::move(res_column), std::move(null_map_col));
    }

    return res_column;
}

}
