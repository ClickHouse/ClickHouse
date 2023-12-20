#include <DataTypes/Serializations/SerializationVariantElement.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

void SerializationVariantElement::enumerateStreams(
    DB::ISerialization::EnumerateStreamsSettings & settings,
    const DB::ISerialization::StreamCallback & callback,
    const DB::ISerialization::SubstreamData & data) const
{
    /// We will need stream for discriminators during deserialization.
    settings.path.push_back(Substream::VariantDiscriminators);
    callback(settings.path);
    settings.path.pop_back();

    addVariantToPath(settings.path);
    settings.path.back().data = data;
    nested_serialization->enumerateStreams(settings, callback, data);
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

void SerializationVariantElement::deserializeBinaryBulkStatePrefix(DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state) const
{
    addVariantToPath(settings.path);
    nested_serialization->deserializeBinaryBulkStatePrefix(settings, state);
    removeVariantFromPath(settings.path);
}

void SerializationVariantElement::serializeBinaryBulkWithMultipleStreams(const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationVariantElement");
}

void SerializationVariantElement::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto mutable_column = column->assumeMutable();
    ColumnNullable * nullable_col = typeid_cast<ColumnNullable *>(mutable_column.get());
    NullMap * null_map = nullable_col ? &nullable_col->getNullMapData() : nullptr;

    /// First, deserialize discriminators from Variant column.
    settings.path.push_back(Substream::VariantDiscriminators);
    ColumnPtr discriminators;
    if (auto cached_discriminators = getFromSubstreamsCache(cache, settings.path))
    {
        discriminators = cached_discriminators;
    }
    else
    {
        auto * discriminators_stream = settings.getter(settings.path);
        if (!discriminators_stream)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for VariantDiscriminators in SerializationVariantElement::deserializeBinaryBulkWithMultipleStreams");

        discriminators = ColumnVariant::ColumnDiscriminators::create();
        SerializationNumber<ColumnVariant::Discriminator>().deserializeBinaryBulk(*discriminators->assumeMutable(), *discriminators_stream, limit, 0);
        addToSubstreamsCache(cache, settings.path, discriminators);
    }
    settings.path.pop_back();

    /// Iterate through discriminators to calculate the size of the variant.
    const auto & discriminators_data = assert_cast<const ColumnVariant::ColumnDiscriminators &>(*discriminators).getData();
    size_t variant_size = 0;
    for (auto discr : discriminators_data)
        variant_size += discr == variant_discriminator;

    /// Now we know the size of the variant and can deserialize it.

    /// If the size of variant column is the same as the size of discriminators,
    /// we can deserialize new values directly into our column.
    if (variant_size == discriminators_data.size())
    {
        addVariantToPath(settings.path);
        /// Special case when our result column is LowCardinality(Nullable(T)).
        /// In this case the variant type is LowCardinality(T), and we cannot just
        /// deserialize its values directly into LowCardinality(Nullable(T)) column.
        /// We create a separate column with type LowCardinality(T), deserialize
        /// values into it and then insert into result column using insertRangeFrom.
        if (isColumnLowCardinalityNullable(*column))
        {
            ColumnPtr variant_col = mutable_column->cloneEmpty();
            /// LowCardinality(Nullable(T)) -> LowCardinality(T)
            assert_cast<ColumnLowCardinality &>(*variant_col->assumeMutable()).nestedRemoveNullable();
            nested_serialization->deserializeBinaryBulkWithMultipleStreams(variant_col, limit, settings, state, cache);
            mutable_column->insertRangeFrom(*variant_col, 0, variant_col->size());
        }
        else
        {
            nested_serialization->deserializeBinaryBulkWithMultipleStreams(nullable_col ? nullable_col->getNestedColumnPtr() : column, limit, settings, state, cache);
        }
        if (nullable_col)
            null_map->resize_fill(null_map->size() + limit, 0);
        removeVariantFromPath(settings.path);
        return;
    }

    /// If variant size is 0, just fill column with default values.
    if (variant_size == 0)
    {
        mutable_column->insertManyDefaults(limit);
        return;
    }

    /// In general case we should deserialize variant into a separate column,
    /// iterate through discriminators and insert values from variant only when
    /// row contains its discriminator and default value otherwise.
    mutable_column->reserve(mutable_column->size() + limit);
    mutable_column = nullable_col ? nullable_col->getNestedColumnPtr()->assumeMutable() : std::move(mutable_column);
    ColumnPtr variant_col = mutable_column->cloneEmpty();

    /// Special case when our result column is LowCardinality(Nullable(T)).
    /// We should remove Nullable from variant column before deserialization.
    if (isColumnLowCardinalityNullable(*column))
        assert_cast<ColumnLowCardinality &>(*variant_col->assumeMutable()).nestedRemoveNullable();

    addVariantToPath(settings.path);
    nested_serialization->deserializeBinaryBulkWithMultipleStreams(variant_col, variant_size, settings, state, cache);
    removeVariantFromPath(settings.path);

    size_t variant_index = 0;
    for (auto discr : discriminators_data)
    {
        if (discr == variant_discriminator)
        {
            if (null_map)
                null_map->push_back(0);
            mutable_column->insertFrom(*variant_col, variant_index++);
        }
        else
        {
            if (null_map)
                null_map->push_back(1);
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
    const DB::ColumnPtr & local_discriminators_,
    const DB::String & variant_element_name_,
    const ColumnVariant::Discriminator global_variant_discriminator_,
    const ColumnVariant::Discriminator local_variant_discriminator_)
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
