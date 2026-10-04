#include <Common/SipHash.h>
#include <DataTypes/Serializations/SerializationVariantElement.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationVariant.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/MaskOperations.h>
#include <IO/ReadHelpers.h>
#include <Columns/IColumn.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_ALL_DATA;
}

UInt128 SerializationVariantElement::getHash(const SerializationPtr & nested_, const String & variant_element_name_, ColumnVariant::Discriminator variant_discriminator_, size_t num_variants_, bool nullable_added_by_extraction_, bool selected_subcolumn_is_null_map_)
{
    SipHash hash;
    hash.update("VariantElement");
    hash.update(nested_->getHash());
    hash.update(variant_element_name_.size());
    hash.update(variant_element_name_);
    hash.update(variant_discriminator_);
    hash.update(num_variants_);
    hash.update(nullable_added_by_extraction_);
    hash.update(selected_subcolumn_is_null_map_);
    return hash.get128();
}

SerializationPtr SerializationVariantElement::create(
    const SerializationPtr & nested_,
    const String & variant_element_name_,
    ColumnVariant::Discriminator variant_discriminator_,
    size_t num_variants_,
    bool nullable_added_by_extraction_,
    bool selected_subcolumn_is_null_map_)
{
    if (!nested_->supportsPooling())
        return std::shared_ptr<ISerialization>(new SerializationVariantElement(nested_, variant_element_name_, variant_discriminator_, num_variants_, nullable_added_by_extraction_, selected_subcolumn_is_null_map_));
    return ISerialization::pooled(getHash(nested_, variant_element_name_, variant_discriminator_, num_variants_, nullable_added_by_extraction_, selected_subcolumn_is_null_map_), [&] { return new SerializationVariantElement(nested_, variant_element_name_, variant_discriminator_, num_variants_, nullable_added_by_extraction_, selected_subcolumn_is_null_map_); });
}

bool SerializationVariantElement::isNullMapSubstream(Substream::Type type)
{
    return type == Substream::NullMap || type == Substream::SparseNullMap || type == Substream::VariantElementNullMap;
}

void SerializationVariantElement::insertRowsForAbsentElement(IColumn & inner_column, size_t num_rows) const
{
    if (selected_subcolumn_is_null_map)
    {
        auto & null_map_data = assert_cast<ColumnUInt8 &>(inner_column).getData();
        null_map_data.resize_fill(null_map_data.size() + num_rows, 1);
        return;
    }

    /// `insertManyDefaults` reserves `size() + length`, which for a tuple or a map reserves every field, so
    /// the per-row caller must not go through it.
    if (num_rows == 1)
        inner_column.insertDefault();
    else
        inner_column.insertManyDefaults(num_rows);
}

struct SerializationVariantElement::DeserializeBinaryBulkStateVariantElement : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr discriminators_state;
    ISerialization::DeserializeBinaryBulkStatePtr variant_element_state;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<SerializationVariantElement::DeserializeBinaryBulkStateVariantElement>();
        new_state->discriminators_state = discriminators_state ? discriminators_state->clone() : nullptr;
        new_state->variant_element_state = variant_element_state ? variant_element_state->clone() : nullptr;
        return new_state;
    }
};


void SerializationVariantElement::enumerateStreams(
    DB::ISerialization::EnumerateStreamsSettings & settings,
    const DB::ISerialization::StreamCallback & callback,
    const DB::ISerialization::SubstreamData & data) const
{
    /// We will need stream for discriminators during deserialization.
    if (settings.use_specialized_prefixes_and_suffixes_substreams)
    {
        settings.path.push_back(Substream::VariantDiscriminatorsPrefix);
        callback(settings.path);
        settings.path.pop_back();
    }

    settings.path.push_back(Substream::VariantDiscriminators);
    callback(settings.path);
    settings.path.pop_back();

    const auto * deserialize_state = data.deserialize_state ? checkAndGetState<DeserializeBinaryBulkStateVariantElement>(data.deserialize_state) : nullptr;
    addVariantToPath(settings.path);
    /// Remove the nullability only when the extraction added it. If the requested type is
    /// intrinsically nullable, nested_serialization is a Nullable serialization and requires it.
    auto nested_type = data.type;
    auto nested_column = data.column;
    if (nullable_added_by_extraction)
    {
        nested_type = nested_type ? removeNullableOrLowCardinalityNullable(nested_type) : nullptr;
        nested_column = nested_column ? removeNullableOrLowCardinalityNullable(nested_column) : nullptr;
    }
    auto nested_data = SubstreamData(nested_serialization)
                       .withType(nested_type)
                       .withColumn(nested_column)
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
    DeserializeBinaryBulkStatePtr discriminators_state = SerializationVariant::deserializeDiscriminatorsStatePrefix(settings, cache);
    if (!discriminators_state)
        return;

    auto variant_element_state = std::make_shared<DeserializeBinaryBulkStateVariantElement>();
    variant_element_state->discriminators_state = discriminators_state;

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
    IColumn & result_column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    /// First, deserialize discriminators from Variant column.
    settings.path.push_back(Substream::VariantDiscriminators);

    DeserializeBinaryBulkStateVariantElement * variant_element_state = nullptr;
    std::optional<size_t> variant_limit;
    size_t num_read_discriminators = 0;
    ColumnPtr discriminators;
    if (auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
    {
        variant_element_state = checkAndGetState<DeserializeBinaryBulkStateVariantElement>(state);
        std::tie(discriminators, num_read_discriminators) = *cached_column_with_num_read_rows;
    }
    else if (auto * discriminators_stream = settings.getter(settings.path))
    {
        variant_element_state = checkAndGetState<DeserializeBinaryBulkStateVariantElement>(state);
        auto * discriminators_state = checkAndGetState<SerializationVariant::DeserializeBinaryBulkStateVariantDiscriminators>(variant_element_state->discriminators_state);

        /// Deserialize discriminators into a fresh column according to serialization mode.
        if (discriminators_state->mode.value == SerializationVariant::DiscriminatorsSerializationMode::BASIC)
        {
            auto mutable_discriminators = ColumnVariant::ColumnDiscriminators::create();
            SerializationNumber<ColumnVariant::Discriminator>::create()->deserializeBinaryBulk(
                *mutable_discriminators, *discriminators_stream, limit, 0);
            discriminators = std::move(mutable_discriminators);
        }
        else
        {
            auto mutable_discriminators = ColumnVariant::ColumnDiscriminators::create();
            variant_limit = deserializeCompactDiscriminators(
                *mutable_discriminators,
                variant_discriminator,
                limit,
                discriminators_stream,
                settings.continuous_reading,
                variant_element_state->discriminators_state,
                settings,
                num_variants,
                this);
            discriminators = std::move(mutable_discriminators);
        }

        num_read_discriminators = discriminators->size();
        addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, discriminators, num_read_discriminators);
    }
    else
    {
        /// There is no discriminators stream, so the element is absent from every row of the range: it can
        /// happen after `ALTER TABLE ADD COLUMN`. A null map still owes a value for each of them.
        if (selected_subcolumn_is_null_map)
            insertRowsForAbsentElement(result_column, limit);
        settings.path.pop_back();
        return;
    }

    settings.path.pop_back();

    size_t discriminators_offset = discriminators->size() - num_read_discriminators;

    /// Iterate through new discriminators to calculate the limit for our variant
    /// if we didn't do it during discriminators deserialization.
    const auto & discriminators_data = assert_cast<const ColumnVariant::ColumnDiscriminators &>(*discriminators).getData();
    if (!variant_limit)
    {
        variant_limit = 0;
        for (size_t i = discriminators_offset; i != discriminators_data.size(); ++i)
            *variant_limit += (discriminators_data[i] == variant_discriminator);
    }

    /// Now we know the limit for our variant and can deserialize it.

    /// A Nullable wrapper added by the extraction is unknown to nested_serialization, so its null map
    /// is filled here from the discriminators. An intrinsic Nullable belongs to nested_serialization,
    /// which reads the element's own null map; other variants' rows become `NULL` via `insertRowsForAbsentElement`.
    IColumn * inner_column = &result_column;
    if (nullable_added_by_extraction && isColumnNullable(result_column))
    {
        auto & nullable_column = assert_cast<ColumnNullable &>(result_column);
        NullMap & null_map = nullable_column.getNullMapData();
        /// If we have only our discriminator in range, fill null map with 0.
        if (variant_limit == num_read_discriminators)
        {
            null_map.resize_fill(null_map.size() + num_read_discriminators, 0);
        }
        /// If no our discriminator in current range, fill null map with 1.
        else if (variant_limit == 0)
        {
            null_map.resize_fill(null_map.size() + num_read_discriminators, 1);
        }
        /// Otherwise we should iterate through discriminators to fill null map.
        else
        {
            null_map.reserve(null_map.size() + num_read_discriminators);
            for (size_t i = discriminators_offset; i != discriminators_data.size(); ++i)
                null_map.push_back(discriminators_data[i] != variant_discriminator);
        }

        inner_column = &nullable_column.getNestedColumn();
    }

    /// Deserialize this variant's values for the current range into a fresh column.
    auto variant = inner_column->cloneEmpty();

    /// When result column is LowCardinality(Nullable(T)) and the Nullable was added by the
    /// extraction, we should remove it from variant column before deserialization.
    if (nullable_added_by_extraction && isColumnLowCardinalityNullable(*inner_column))
        assert_cast<ColumnLowCardinality &>(*variant).nestedRemoveNullable();

    addVariantToPath(settings.path);
    nested_serialization->deserializeBinaryBulkWithMultipleStreams(*variant, *variant_limit, settings, variant_element_state->variant_element_state, cache);
    removeVariantFromPath(settings.path);

    /// We want to keep dynamic structure of the variant during deserialization.
    /// Keeping dynamic structure improves performance of insertFrom/insertRangeFrom methods.
    if (inner_column->empty())
        inner_column->takeExactDynamicStructureFrom(*variant);

    /// If there was nothing to deserialize or nothing was actually deserialized when variant_limit > 0, just insert defaults.
    /// The second case means that we don't have a stream for such sub-column. It may happen during ALTER MODIFY column with Variant extension.
    if (variant_limit == 0 || variant->empty())
    {
        insertRowsForAbsentElement(*inner_column, num_read_discriminators);
        return;
    }

    if (variant->size() < *variant_limit)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Size of deserialized variant column less than the limit: {} < {}", variant->size(), *variant_limit);

    size_t variant_offset = variant->size() - *variant_limit;

    /// If we have only our discriminator in range, insert the whole range to result column.
    if (variant_limit == num_read_discriminators)
    {
        inner_column->insertRangeFrom(*variant, variant_offset, *variant_limit);
    }
    /// Otherwise iterate through discriminators and insert value from variant or default value depending on the discriminator.
    else
    {
        for (size_t i = discriminators_offset; i != discriminators_data.size(); ++i)
        {
            if (discriminators_data[i] == variant_discriminator)
                inner_column->insertFrom(*variant, variant_offset++);
            else
                insertRowsForAbsentElement(*inner_column, 1);
        }
    }
}

size_t SerializationVariantElement::deserializeCompactDiscriminators(
    IColumn & discriminators_column,
    ColumnVariant::Discriminator variant_discriminator,
    size_t limit,
    DB::ReadBuffer * stream,
    bool continuous_reading,
    DeserializeBinaryBulkStatePtr & discriminators_state_,
    const DeserializeBinaryBulkSettings & settings,
    size_t num_variants,
    const ISerialization * serialization)
{
    auto * discriminators_state = checkAndGetState<SerializationVariant::DeserializeBinaryBulkStateVariantDiscriminators>(discriminators_state_, serialization);
    auto & discriminators_data = assert_cast<ColumnVariant::ColumnDiscriminators &>(discriminators_column).getData();

    /// Reset state if we are reading from the start of the granule and not from the previous position in the file.
    if (!continuous_reading)
        discriminators_state->remaining_rows_in_granule = 0;

    /// Calculate our variant limit during discriminators deserialization.
    size_t variant_limit = 0;

    while (limit)
    {
        /// If we read all rows from current granule, start reading the next one.
        if (discriminators_state->remaining_rows_in_granule == 0)
        {
            if (stream->eof())
                return variant_limit;

            SerializationVariant::readDiscriminatorsGranuleStart(
                *discriminators_state, stream, num_variants, settings);
        }

        size_t limit_in_granule = std::min(limit, discriminators_state->remaining_rows_in_granule);
        if (discriminators_state->granule_format == SerializationVariant::CompactDiscriminatorsGranuleFormat::COMPACT)
        {
            discriminators_data.resize_fill(discriminators_data.size() + limit_in_granule, discriminators_state->compact_discr);

            if (discriminators_state->compact_discr == variant_discriminator)
                variant_limit += limit_in_granule;
        }
        else
        {
            size_t start = discriminators_data.size();
            SerializationNumber<ColumnVariant::Discriminator>::deserializeBinaryBulk(discriminators_data, *stream, limit_in_granule);
            size_t num_read = discriminators_data.size() - start;
            if (num_read != limit_in_granule)
                throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                    "Cannot read all discriminators in Variant granule. Expected: {}, got: {}", limit_in_granule, num_read);

            for (size_t i = start; i != discriminators_data.size(); ++i)
                variant_limit += (discriminators_data[i] == variant_discriminator);
        }

        discriminators_state->remaining_rows_in_granule -= limit_in_granule;
        limit -= limit_in_granule;
    }

    return variant_limit;
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
    ColumnVariant::Discriminator local_variant_discriminator_,
    bool make_nullable_,
    const ColumnPtr & null_map_,
    size_t num_variants_,
    bool selected_subcolumn_is_null_map_)
    : local_discriminators(local_discriminators_)
    , null_map(null_map_)
    , variant_element_name(variant_element_name_)
    , global_variant_discriminator(global_variant_discriminator_)
    , local_variant_discriminator(local_variant_discriminator_)
    , make_nullable(make_nullable_)
    , num_variants(num_variants_)
    , selected_subcolumn_is_null_map(selected_subcolumn_is_null_map_)
{
}

std::shared_ptr<const ISerialization::ISubcolumnCreator>
SerializationVariantElement::VariantSubcolumnCreator::specializeForSelectedSubcolumn(const Substream & selected) const
{
    if (selected_subcolumn_is_null_map || !isNullMapSubstream(selected.type))
        return nullptr;

    return std::make_shared<VariantSubcolumnCreator>(
        local_discriminators,
        variant_element_name,
        global_variant_discriminator,
        local_variant_discriminator,
        make_nullable,
        null_map,
        num_variants,
        /*selected_subcolumn_is_null_map_=*/true);
}

DataTypePtr SerializationVariantElement::VariantSubcolumnCreator::create(const DataTypePtr & prev) const
{
    return make_nullable ? makeNullableOrLowCardinalityNullableSafe(prev) : prev;
}

SerializationPtr SerializationVariantElement::VariantSubcolumnCreator::create(const SerializationPtr & prev, const DataTypePtr & prev_type) const
{
    /// prev_type is the type prev serializes, i.e. the requested subcolumn before create(prev_type)
    /// wraps it. The wrap only adds nullability when the type does not have it already.
    const bool nullable_added = make_nullable && prev_type && !isNullableOrLowCardinalityNullable(prev_type);
    const bool absent_element_is_null = absenceGoesIntoNullMap() && prev_type && isUInt8(prev_type);
    return SerializationVariantElement::create(
        prev, variant_element_name, global_variant_discriminator, num_variants, nullable_added, absent_element_is_null);
}

ColumnPtr SerializationVariantElement::VariantSubcolumnCreator::create(const DB::ColumnPtr & prev) const
{
    /// Case when original Variant column contained only one non-empty variant and no NULLs.
    /// In this case just use this variant.
    if (prev->size() == local_discriminators->size())
        return make_nullable ? makeNullableOrLowCardinalityNullableSafe(prev) : prev;

    /// A null map is the one selection whose value for an absent element is not its default: 0 reads as
    /// "not null", but the element is not there, so the extracted value is `NULL` and the map owes 1.
    const bool fill_absent_rows_with_null = absenceGoesIntoNullMap() && checkAndGetColumn<ColumnUInt8>(prev.get());

    /// If this variant is empty, fill result column with default values.
    if (prev->empty() && !fill_absent_rows_with_null)
    {
        auto res = make_nullable ? makeNullableOrLowCardinalityNullableSafe(prev)->cloneEmpty() : prev->cloneEmpty();
        res->insertManyDefaults(local_discriminators->size());
        return res;
    }

    /// In general case we should iterate through discriminators and create null-map for our variant if we don't already have it.
    std::optional<NullMap> null_map_from_discriminators;
    if (!null_map)
    {
        null_map_from_discriminators = NullMap();
        null_map_from_discriminators->reserve(local_discriminators->size());
        const auto & local_discriminators_data = assert_cast<const ColumnVariant::ColumnDiscriminators &>(*local_discriminators).getData();
        for (auto local_discr : local_discriminators_data)
            null_map_from_discriminators->push_back(local_discr != local_variant_discriminator);
    }

    /// Now we can create new column from null-map and variant column using IColumn::expand.
    auto res_column = IColumn::mutate(prev);

    /// Special case for LowCardinality when we want the result to be LowCardinality(Nullable),
    /// but we don't have a good way to apply null-mask for LowCardinality(), so, we first
    /// convert our column to LowCardinality(Nullable()) and then use expand which will
    /// fill rows with 0 in mask with default value (that is NULL).
    if (make_nullable && prev->lowCardinality())
        res_column = assert_cast<ColumnLowCardinality &>(*res_column).cloneNullable();

    const auto & absent_rows
        = null_map_from_discriminators ? *null_map_from_discriminators : assert_cast<const ColumnUInt8 &>(*null_map).getData();

    if (fill_absent_rows_with_null)
        expandDataByMask<UInt8>(
            assert_cast<ColumnUInt8 &>(*res_column).getData(), absent_rows, /*inverted =*/true, /*default_value =*/1);
    else
        res_column->expand(absent_rows, /*inverted = */ true);

    if (make_nullable && prev->canBeInsideNullable())
    {
        if (null_map_from_discriminators)
        {
            auto null_map_col = ColumnUInt8::create();
            null_map_col->getData() = std::move(*null_map_from_discriminators);
            return ColumnNullable::create(std::move(res_column), std::move(null_map_col));
        }

        return ColumnNullable::create(std::move(res_column), null_map);
    }

    return res_column;
}

size_t SerializationVariantElement::allocatedBytes() const
{
    return sizeof(*this) + variant_element_name.capacity();
}

MutableColumnPtr SerializationVariantElement::wrapColumnForDeserialization(MutableColumnPtr column) const
{
    /// The Nullable level is either one the extraction added, which deserializeBinaryBulkWithMultipleStreams
    /// fills from the discriminators and peels off before recursing, or nested_serialization's own, which this
    /// rebuild reproduces unchanged. A LowCardinality(Nullable) result is deserialized whole and keeps it.
    if (isColumnNullable(*column))
    {
        const auto & nullable = assert_cast<const ColumnNullable &>(*column);
        return ColumnNullable::create(
            nested_serialization->wrapColumnForDeserialization(nullable.getNestedColumnPtr()->cloneEmpty()),
            nullable.getNullMapColumnPtr()->cloneEmpty());
    }

    return nested_serialization->wrapColumnForDeserialization(std::move(column));
}

}
