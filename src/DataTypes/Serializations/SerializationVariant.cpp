#include <DataTypes/Serializations/SerializationVariant.h>
#include <DataTypes/Serializations/SerializationVariantElement.h>
#include <DataTypes/Serializations/SerializationVariantElementNullMap.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnVariant.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Columns/IColumn.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
}

struct SerializeBinaryBulkStateVariant : public ISerialization::SerializeBinaryBulkState
{
    explicit SerializeBinaryBulkStateVariant(UInt64 mode) : discriminators_mode(mode)
    {
    }

    SerializationVariant::DiscriminatorsSerializationMode discriminators_mode;
    std::vector<ISerialization::SerializeBinaryBulkStatePtr> variant_states;
};

struct DeserializeBinaryBulkStateVariant : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr discriminators_state;
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> variant_states;
    size_t num_rows_read = 0;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateVariant>();
        new_state->discriminators_state = discriminators_state ? discriminators_state->clone() : nullptr;
        new_state->variant_states.reserve(variant_states.size());
        for (const auto & variant_state : variant_states)
            new_state->variant_states.push_back(variant_state ? variant_state->clone() : nullptr);
        return new_state;
    }
};

void SerializationVariant::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    const auto * type_variant = data.type ? &assert_cast<const DataTypeVariant &>(*data.type) : nullptr;
    const auto * column_variant = data.column ? &assert_cast<const ColumnVariant &>(*data.column) : nullptr;
    const auto * variant_deserialize_state = data.deserialize_state ? checkAndGetState<DeserializeBinaryBulkStateVariant>(data.deserialize_state) : nullptr;

    auto discriminators_serialization = std::make_shared<SerializationNamed>(std::make_shared<SerializationNumber<ColumnVariant::Discriminator>>(), "discr", SubstreamType::NamedVariantDiscriminators);
    auto local_discriminators = column_variant ? column_variant->getLocalDiscriminatorsPtr() : nullptr;

    if (settings.use_specialized_prefixes_substreams)
    {
        settings.path.push_back(Substream::VariantDiscriminatorsPrefix);
        callback(settings.path);
        settings.path.pop_back();
    }

    settings.path.push_back(Substream::VariantDiscriminators);
    auto discriminators_data = SubstreamData(discriminators_serialization)
                             .withType(type_variant ? std::make_shared<DataTypeNumber<ColumnVariant::Discriminator>>() : nullptr)
                             .withColumn(column_variant ? column_variant->getLocalDiscriminatorsPtr() : nullptr)
                             .withSerializationInfo(data.serialization_info);

    settings.path.back().data = discriminators_data;
    callback(settings.path);
    settings.path.pop_back();

    settings.path.push_back(Substream::VariantElements);
    settings.path.back().data = data;

    for (size_t i = 0; i < variants.size(); ++i)
    {
        DataTypePtr type = type_variant ? type_variant->getVariant(i) : nullptr;
        settings.path.back().creator = std::make_shared<SerializationVariantElement::VariantSubcolumnCreator>(
            local_discriminators,
            variant_names[i],
            i,
            column_variant ? column_variant->localDiscriminatorByGlobal(i) : i,
            !type || type->canBeInsideNullable() || type->lowCardinality());

        auto variant_data = SubstreamData(variants[i])
                             .withType(type)
                             .withColumn(column_variant ? column_variant->getVariantPtrByGlobalDiscriminator(i) : nullptr)
                             .withSerializationInfo(data.serialization_info)
                             .withDeserializeState(variant_deserialize_state ? variant_deserialize_state->variant_states[i] : nullptr);

        addVariantElementToPath(settings.path, i);
        settings.path.back().data = variant_data;
        variants[i]->enumerateStreams(settings, callback, variant_data);
        settings.path.pop_back();
    }

    /// Variant subcolumns like variant.Type have type Nullable(Type), so we want to support reading null map subcolumn from it: variant.Type.null.
    /// Nullable column is created during deserialization of a variant subcolumn according to the discriminators, so we don't have actual Nullable
    /// serialization with null map subcolumn. To be able to read null map subcolumn from the variant subcolumn we use special serialization
    /// SerializationVariantElementNullMap.
    auto null_map_data = SubstreamData(std::make_shared<SerializationNumber<UInt8>>())
                             .withType(type_variant ? std::make_shared<DataTypeUInt8>() : nullptr)
                             .withColumn(column_variant ? ColumnUInt8::create() : nullptr);

    for (size_t i = 0; i < variants.size(); ++i)
    {
        settings.path.back().creator = std::make_shared<SerializationVariantElementNullMap::VariantNullMapSubcolumnCreator>(local_discriminators, variant_names[i], i, column_variant ? column_variant->localDiscriminatorByGlobal(i) : i);
        settings.path.push_back(Substream::VariantElementNullMap);
        settings.path.back().variant_element_name = variant_names[i];
        settings.path.back().data = null_map_data;
        callback(settings.path);
        settings.path.pop_back();
    }

    settings.path.pop_back();
}

void SerializationVariant::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(settings.use_specialized_prefixes_substreams ? Substream::VariantDiscriminatorsPrefix : Substream::VariantDiscriminators);
    auto * discriminators_stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!discriminators_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for VariantDiscriminators in SerializationVariant::serializeBinaryBulkStatePrefix");

    UInt64 mode = settings.use_compact_variant_discriminators_serialization ? DiscriminatorsSerializationMode::COMPACT : DiscriminatorsSerializationMode::BASIC;
    writeBinaryLittleEndian(mode, *discriminators_stream);

    const ColumnVariant & col = assert_cast<const ColumnVariant &>(column);
    auto variant_state = std::make_shared<SerializeBinaryBulkStateVariant>(mode);
    variant_state->variant_states.resize(variants.size());

    settings.path.push_back(Substream::VariantElements);

    for (size_t i = 0; i < variants.size(); ++i)
    {
        addVariantElementToPath(settings.path, i);
        variants[i]->serializeBinaryBulkStatePrefix(col.getVariantByGlobalDiscriminator(i), settings, variant_state->variant_states[i]);
        settings.path.pop_back();
    }

    settings.path.pop_back();
    state = std::move(variant_state);
}


void SerializationVariant::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * variant_state = checkAndGetState<SerializeBinaryBulkStateVariant>(state);

    settings.path.push_back(Substream::VariantElements);
    for (size_t i = 0; i < variants.size(); ++i)
    {
        addVariantElementToPath(settings.path, i);
        variants[i]->serializeBinaryBulkStateSuffix(settings, variant_state->variant_states[i]);
        settings.path.pop_back();
    }
    settings.path.pop_back();
}


void SerializationVariant::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    DeserializeBinaryBulkStatePtr discriminators_state = deserializeDiscriminatorsStatePrefix(settings, cache);
    if (!discriminators_state)
        return;

    auto variant_state = std::make_shared<DeserializeBinaryBulkStateVariant>();
    variant_state->discriminators_state = discriminators_state;
    variant_state->variant_states.resize(variants.size());

    settings.path.push_back(Substream::VariantElements);
    for (size_t i = 0; i < variants.size(); ++i)
    {
        addVariantElementToPath(settings.path, i);
        variants[i]->deserializeBinaryBulkStatePrefix(settings, variant_state->variant_states[i], cache);
        settings.path.pop_back();
    }

    settings.path.pop_back();
    state = std::move(variant_state);
}

ISerialization::DeserializeBinaryBulkStatePtr SerializationVariant::deserializeDiscriminatorsStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    SubstreamsDeserializeStatesCache * cache)
{
    settings.path.push_back(settings.use_specialized_prefixes_substreams ? Substream::VariantDiscriminatorsPrefix : Substream::VariantDiscriminators);

    DeserializeBinaryBulkStatePtr discriminators_state = nullptr;
    if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, settings.path))
    {
        discriminators_state = cached_state;
    }
    else if (auto * discriminators_stream = settings.getter(settings.path))
    {
        UInt64 mode;
        readBinaryLittleEndian(mode, *discriminators_stream);
        discriminators_state = std::make_shared<DeserializeBinaryBulkStateVariantDiscriminators>(mode);
        addToSubstreamsDeserializeStatesCache(cache, settings.path, discriminators_state);
    }

    settings.path.pop_back();
    return discriminators_state;
}


void SerializationVariant::serializeBinaryBulkWithMultipleStreamsAndUpdateVariantStatistics(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state,
    std::unordered_map<String, size_t> & variants_statistics,
    size_t & total_size_of_variants) const
{
    const ColumnVariant & col = assert_cast<const ColumnVariant &>(column);
    if (const size_t size = col.size(); limit == 0 || offset + limit > size)
        limit = size - offset;

    settings.path.push_back(Substream::VariantDiscriminators);
    auto * discriminators_stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!discriminators_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for VariantDiscriminators in SerializationVariant::serializeBinaryBulkWithMultipleStreams");

    auto * variant_state = checkAndGetState<SerializeBinaryBulkStateVariant>(state);

    /// Even if there is no data to write, we still have to call nested serialization,
    /// because we might need to call the stream getter for all existing substreams even
    /// if nothing is written there. It's needed in Compact parts when we write
    /// marks per substreams inside the stream getter.
    if (limit == 0)
    {
        settings.path.push_back(Substream::VariantElements);
        for (size_t i = 0; i != variants.size(); ++i)
        {
            addVariantElementToPath(settings.path, i);
            variants[i]->serializeBinaryBulkWithMultipleStreams(col.getVariantByGlobalDiscriminator(i), col.getVariantByGlobalDiscriminator(i).size(), 0, settings, variant_state->variant_states[i]);
            settings.path.pop_back();
        }
        settings.path.pop_back();
        return;
    }

    /// Write number of rows in this granule in compact mode.
    if (variant_state->discriminators_mode.value == DiscriminatorsSerializationMode::COMPACT)
        writeVarUInt(UInt64(limit), *discriminators_stream);

    /// If column has only one none empty discriminators and no NULLs we don't need to
    /// calculate limits for variants and use provided offset/limit.
    if (auto non_empty_local_discr = col.getLocalDiscriminatorOfOneNoneEmptyVariantNoNulls())
    {
        auto non_empty_global_discr = col.globalDiscriminatorByLocal(*non_empty_local_discr);

        /// In compact mode write the format of the granule and single non-empty discriminator.
        if (variant_state->discriminators_mode.value == DiscriminatorsSerializationMode::COMPACT)
        {
            writeBinaryLittleEndian(UInt8(CompactDiscriminatorsGranuleFormat::COMPACT), *discriminators_stream);
            writeBinaryLittleEndian(non_empty_global_discr, *discriminators_stream);
        }
        /// For basic mode just serialize this discriminator limit times.
        else
        {
            for (size_t i = 0; i < limit; ++i)
                writeBinaryLittleEndian(non_empty_global_discr, *discriminators_stream);
        }

        settings.path.push_back(Substream::VariantElements);
        for (size_t i = 0; i != variants.size(); ++i)
        {
            addVariantElementToPath(settings.path, i);
            /// We can use the same offset/limit as for whole Variant column
            if (i == non_empty_global_discr)
                variants[i]->serializeBinaryBulkWithMultipleStreams(col.getVariantByGlobalDiscriminator(i), offset, limit, settings, variant_state->variant_states[i]);
            else
                variants[i]->serializeBinaryBulkWithMultipleStreams(col.getVariantByGlobalDiscriminator(i), col.getVariantByGlobalDiscriminator(i).size(), 0, settings, variant_state->variant_states[i]);
            settings.path.pop_back();
        }
        variants_statistics[variant_names[non_empty_global_discr]] += limit;
        total_size_of_variants += limit;
        settings.path.pop_back();
        return;
    }
    /// If column has only NULLs, just serialize NULL discriminators.
    if (col.hasOnlyNulls())
    {
        /// In compact mode write single NULL_DISCRIMINATOR.
        if (variant_state->discriminators_mode.value == DiscriminatorsSerializationMode::COMPACT)
        {
            writeBinaryLittleEndian(UInt8(CompactDiscriminatorsGranuleFormat::COMPACT), *discriminators_stream);
            writeBinaryLittleEndian(ColumnVariant::NULL_DISCRIMINATOR, *discriminators_stream);
        }
        /// In basic mode write NULL_DISCRIMINATOR limit times.
        else
        {
            for (size_t i = 0; i < limit; ++i)
                writeBinaryLittleEndian(ColumnVariant::NULL_DISCRIMINATOR, *discriminators_stream);
        }

        settings.path.push_back(Substream::VariantElements);
        for (size_t i = 0; i != variants.size(); ++i)
        {
            addVariantElementToPath(settings.path, i);
            variants[i]->serializeBinaryBulkWithMultipleStreams(col.getVariantByGlobalDiscriminator(i), col.getVariantByGlobalDiscriminator(i).size(), 0, settings, variant_state->variant_states[i]);
            settings.path.pop_back();
        }
        settings.path.pop_back();
        return;
    }

    /// If offset = 0 and limit == col.size() we don't need to calculate
    /// offsets and limits for variants and need to just serialize whole columns.
    if ((offset == 0 && limit == col.size()))
    {
        /// First, serialize discriminators.
        /// Here we are sure that column contains different discriminators, use plain granule format in compact mode.
        if (variant_state->discriminators_mode.value == DiscriminatorsSerializationMode::COMPACT)
            writeBinaryLittleEndian(UInt8(CompactDiscriminatorsGranuleFormat::PLAIN), *discriminators_stream);

        /// If local and global discriminators are the same, just serialize the column as is.
        if (col.hasGlobalVariantsOrder())
        {
            SerializationNumber<ColumnVariant::Discriminator>().serializeBinaryBulk(col.getLocalDiscriminatorsColumn(), *discriminators_stream, offset, limit);
        }
        /// If local and global discriminators are different, we should convert local to global before serializing (because we don't serialize the mapping).
        else
        {
            const auto & local_discriminators = col.getLocalDiscriminators();
            for (size_t i = offset; i != offset + limit; ++i)
                writeBinaryLittleEndian(col.globalDiscriminatorByLocal(local_discriminators[i]), *discriminators_stream);
        }

        /// Second, serialize variants in global order.
        settings.path.push_back(Substream::VariantElements);
        for (size_t i = 0; i != variants.size(); ++i)
        {
            addVariantElementToPath(settings.path, i);
            variants[i]->serializeBinaryBulkWithMultipleStreams(col.getVariantByGlobalDiscriminator(i), 0, 0, settings, variant_state->variant_states[i]);
            size_t variant_size = col.getVariantByGlobalDiscriminator(i).size();
            variants_statistics[variant_names[i]] += variant_size;
            total_size_of_variants += variant_size;
            settings.path.pop_back();
        }
        settings.path.pop_back();
        return;
    }

    /// In general case we should iterate through local discriminators in range [offset, offset + limit] to serialize global discriminators and calculate offset/limit pair for each variant.
    const auto & local_discriminators = col.getLocalDiscriminators();
    const auto & offsets = col.getOffsets();
    std::vector<std::pair<size_t, size_t>> variant_offsets_and_limits(variants.size(), {0, 0});
    size_t end = offset + limit;
    size_t num_non_empty_variants_in_range = 0;
    ColumnVariant::Discriminator last_non_empty_variant_discr = 0;
    for (size_t i = offset; i < end; ++i)
    {
        auto global_discr = col.globalDiscriminatorByLocal(local_discriminators[i]);
        if (global_discr != ColumnVariant::NULL_DISCRIMINATOR)
        {
            /// If we see this discriminator for the first time, update offset
            if (!variant_offsets_and_limits[global_discr].second)
                variant_offsets_and_limits[global_discr].first = offsets[i];
            /// Update limit for this discriminator.
            ++variant_offsets_and_limits[global_discr].second;
            ++num_non_empty_variants_in_range;
            last_non_empty_variant_discr = global_discr;
        }
    }

    /// In basic mode just serialize discriminators as is row by row.
    if (variant_state->discriminators_mode.value == DiscriminatorsSerializationMode::BASIC)
    {
        for (size_t i = offset; i < end; ++i)
            writeBinaryLittleEndian(col.globalDiscriminatorByLocal(local_discriminators[i]), *discriminators_stream);
    }
    /// In compact mode check if we have the same discriminator for all rows in this granule.
    /// First, check if all values in granule are NULLs.
    else if (num_non_empty_variants_in_range == 0)
    {
        writeBinaryLittleEndian(UInt8(CompactDiscriminatorsGranuleFormat::COMPACT), *discriminators_stream);
        writeBinaryLittleEndian(ColumnVariant::NULL_DISCRIMINATOR, *discriminators_stream);
    }
    /// Then, check if there is only 1 variant and no NULLs in this granule.
    else if (num_non_empty_variants_in_range == 1 && variant_offsets_and_limits[last_non_empty_variant_discr].second == limit)
    {
        writeBinaryLittleEndian(UInt8(CompactDiscriminatorsGranuleFormat::COMPACT), *discriminators_stream);
        writeBinaryLittleEndian(last_non_empty_variant_discr, *discriminators_stream);
    }
    /// Otherwise there are different discriminators in this granule.
    else
    {
        writeBinaryLittleEndian(UInt8(CompactDiscriminatorsGranuleFormat::PLAIN), *discriminators_stream);
        for (size_t i = offset; i < end; ++i)
            writeBinaryLittleEndian(col.globalDiscriminatorByLocal(local_discriminators[i]), *discriminators_stream);
    }

    /// Serialize variants in global order.
    settings.path.push_back(Substream::VariantElements);
    for (size_t i = 0; i != variants.size(); ++i)
    {
        addVariantElementToPath(settings.path, i);
        variants[i]->serializeBinaryBulkWithMultipleStreams(
            col.getVariantByGlobalDiscriminator(i),
            variant_offsets_and_limits[i].second ? variant_offsets_and_limits[i].first : col.getVariantByGlobalDiscriminator(i).size(),
            variant_offsets_and_limits[i].second,
            settings,
            variant_state->variant_states[i]);
        variants_statistics[variant_names[i]] += variant_offsets_and_limits[i].second;
        total_size_of_variants += variant_offsets_and_limits[i].second;
        settings.path.pop_back();
    }
    settings.path.pop_back();
}

void SerializationVariant::serializeBinaryBulkWithMultipleStreams(
    const DB::IColumn & column,
    size_t offset,
    size_t limit,
    DB::ISerialization::SerializeBinaryBulkSettings & settings,
    DB::ISerialization::SerializeBinaryBulkStatePtr & state) const
{
    std::unordered_map<String, size_t> tmp_statistics;
    size_t tmp_size;
    serializeBinaryBulkWithMultipleStreamsAndUpdateVariantStatistics(column, offset, limit, settings, state, tmp_statistics, tmp_size);
}

void SerializationVariant::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto mutable_column = column->assumeMutable();
    ColumnVariant & col = assert_cast<ColumnVariant &>(*mutable_column);
    /// We always serialize Variant column with global variants order,
    /// so while deserialization column should be always with global variants order.
    if (!col.hasGlobalVariantsOrder())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to deserialize data into Variant column with not global variants order");

    /// First, deserialize discriminators.
    settings.path.push_back(Substream::VariantDiscriminators);

    DeserializeBinaryBulkStateVariant * variant_state = nullptr;
    std::vector<size_t> variant_rows_offsets;
    std::vector<size_t> variant_limits;

    if (auto cached_discriminators = getFromSubstreamsCache(cache, settings.path))
    {
        variant_state = checkAndGetState<DeserializeBinaryBulkStateVariant>(state);
        col.getLocalDiscriminatorsPtr() = cached_discriminators;
    }
    else if (auto * discriminators_stream = settings.getter(settings.path))
    {
        variant_state = checkAndGetState<DeserializeBinaryBulkStateVariant>(state);
        auto * discriminators_state = checkAndGetState<DeserializeBinaryBulkStateVariantDiscriminators>(variant_state->discriminators_state);
        /// Now we are sure that discriminators are not in cache and we can save the size of discriminators now to know how
        /// many discriminators were actually deserialized to iterate over them later to calculate limits for variants.
        variant_state->num_rows_read = col.getLocalDiscriminatorsPtr()->size();

        /// Deserialize discriminators according to serialization mode.
        if (discriminators_state->mode.value == DiscriminatorsSerializationMode::BASIC)
        {
            SerializationNumber<ColumnVariant::Discriminator>().deserializeBinaryBulk(
                *col.getLocalDiscriminatorsPtr()->assumeMutable(), *discriminators_stream, rows_offset, limit, 0);
        }
        else
        {
            auto variant_pair = deserializeCompactDiscriminators(
                col.getLocalDiscriminatorsPtr(), rows_offset, limit, discriminators_stream, settings.continuous_reading,
                *discriminators_state);

            variant_rows_offsets = variant_pair.first;
            variant_limits = variant_pair.second;
        }

        if (rows_offset)
            addToSubstreamsCache(cache, settings.path, IColumn::mutate(col.getLocalDiscriminatorsPtr()));
        else
            addToSubstreamsCache(cache, settings.path, col.getLocalDiscriminatorsPtr());
    }
    /// It may happen that there is no such stream, in this case just do nothing.
    else
    {
        settings.path.pop_back();
        return;
    }

    settings.path.pop_back();

    /// Deserialization state saves `num_rows_read` to track rows processed in previous deserialization.
    /// Whether discriminators are cached or not, `num_rows_read` serves as the starting offset
    /// for new discriminators.
    /// Must reset `num_rows_read` to 0 when upper layer re-initiates read with an empty column.
    if (col.empty())
        variant_state->num_rows_read = 0;
    size_t discriminators_offset = variant_state->num_rows_read;

    /// Second, calculate offsets and limits for each variant by iterating through new discriminators
    /// if we didn't do it during discriminators deserialization.
    if (variant_rows_offsets.empty())
    {
        variant_rows_offsets.resize(variants.size(), 0);

        if (rows_offset)
        {
            auto & discriminators_data = col.getLocalDiscriminators();

            for (size_t i = discriminators_offset; i != discriminators_offset + rows_offset; ++i)
            {
                ColumnVariant::Discriminator discr = discriminators_data[i];
                if (discr != ColumnVariant::NULL_DISCRIMINATOR)
                    ++variant_rows_offsets[discr];
            }
        }
    }

    if (rows_offset)
    {
        auto & discriminators_data = col.getLocalDiscriminators();

        for (size_t i = discriminators_offset; i + rows_offset < discriminators_data.size(); ++i)
            discriminators_data[i] = discriminators_data[i + rows_offset];
        col.getLocalDiscriminatorsPtr()->assumeMutable()->popBack(rows_offset);
    }

    if (variant_limits.empty())
    {
        variant_limits.resize(variants.size(), 0);
        auto & discriminators_data = col.getLocalDiscriminators();

        for (size_t i = discriminators_offset ; i != discriminators_data.size(); ++i)
        {
            ColumnVariant::Discriminator discr = discriminators_data[i];
            if (discr != ColumnVariant::NULL_DISCRIMINATOR)
                ++variant_limits[discr];
        }
    }

    /// Now we can deserialize variants according to their limits.
    settings.path.push_back(Substream::VariantElements);
    for (size_t i = 0; i != variants.size(); ++i)
    {
        addVariantElementToPath(settings.path, i);
        variants[i]->deserializeBinaryBulkWithMultipleStreams(
            col.getVariantPtrByLocalDiscriminator(i), variant_rows_offsets[i], variant_limits[i],
            settings, variant_state->variant_states[i], cache);
        settings.path.pop_back();
    }
    settings.path.pop_back();

    /// Fill offsets column.
    /// It's important to do it after deserialization of all variants, because to fill offsets we need
    /// initial variants sizes without values in current range, but some variants can be shared with
    /// other columns via substream cache and they can already contain values from this range even
    /// before we call deserialize for them. So, before deserialize we cannot know for sure if
    /// variant columns already contain values from current range or not. But after calling deserialize
    /// we know for sure that they contain these values, so we can use valiant limits and their
    /// new sizes to calculate correct offsets.
    settings.path.push_back(Substream::VariantOffsets);
    if (auto cached_offsets = getFromSubstreamsCache(cache, settings.path))
    {
        col.getOffsetsPtr() = cached_offsets;
    }
    else
    {
        std::vector<size_t> variant_offsets;
        variant_offsets.reserve(variants.size());
        size_t num_non_empty_variants = 0;
        ColumnVariant::Discriminator last_non_empty_discr = 0;
        for (size_t i = 0; i != variants.size(); ++i)
        {
            if (variant_limits[i])
            {
                ++num_non_empty_variants;
                last_non_empty_discr = i;
            }

            variant_offsets.push_back(col.getVariantByLocalDiscriminator(i).size() - variant_limits[i]);
        }

        auto & discriminators_data = col.getLocalDiscriminators();
        auto & offsets = col.getOffsets();
        size_t num_new_offsets = discriminators_data.size() - offsets.size();
        offsets.reserve(offsets.size() + num_new_offsets);
        /// If there are only NULLs were read, fill offsets with 0.
        if (num_non_empty_variants == 0)
        {
            offsets.resize_fill(discriminators_data.size(), 0);
        }
        /// If there is only 1 variant and no NULLs was read, fill offsets with sequential offsets of this variant.
        else if (num_non_empty_variants == 1 && variant_limits[last_non_empty_discr] == num_new_offsets)
        {
            size_t first_offset = col.getVariantByLocalDiscriminator(last_non_empty_discr).size() - num_new_offsets;
            for (size_t i = 0; i != num_new_offsets; ++i)
                offsets.push_back(first_offset + i);
        }
        /// Otherwise iterate through discriminators and fill offsets accordingly.
        else
        {
            size_t start = offsets.size();
            for (size_t i = start; i != discriminators_data.size(); ++i)
            {
                ColumnVariant::Discriminator discr = discriminators_data[i];
                if (discr == ColumnVariant::NULL_DISCRIMINATOR)
                    offsets.emplace_back();
                else
                    offsets.push_back(variant_offsets[discr]++);
            }
        }

        addToSubstreamsCache(cache, settings.path, col.getOffsetsPtr());
    }
    settings.path.pop_back();

    variant_state->num_rows_read = col.size();
}

std::pair<std::vector<size_t>, std::vector<size_t>> SerializationVariant::deserializeCompactDiscriminators(
    DB::ColumnPtr & discriminators_column,
    size_t rows_offset,
    size_t limit,
    ReadBuffer * stream,
    bool continuous_reading,
    DeserializeBinaryBulkStateVariantDiscriminators & state) const
{
    auto & discriminators = assert_cast<ColumnVariant::ColumnDiscriminators &>(*discriminators_column->assumeMutable());
    auto & discriminators_data = discriminators.getData();

    /// Reset state if we are reading from the start of the granule and not from the previous position in the file.
    if (!continuous_reading)
        state.remaining_rows_in_granule = 0;

    /// Calculate limits for variants during discriminators deserialization.
    std::vector<size_t> variant_rows_offsets(variants.size(), 0);
    std::vector<size_t> variant_limits(variants.size(), 0);
    limit += rows_offset;

    while (limit)
    {
        /// If we read all rows from current granule, start reading the next one.
        if (state.remaining_rows_in_granule == 0)
        {
            if (stream->eof())
                return {variant_rows_offsets, variant_limits};

            readDiscriminatorsGranuleStart(state, stream);
        }

        size_t limit_in_granule = std::min(limit, state.remaining_rows_in_granule);
        if (state.granule_format == CompactDiscriminatorsGranuleFormat::COMPACT)
        {
            auto & data = discriminators.getData();
            data.resize_fill(data.size() + limit_in_granule, state.compact_discr);
            auto remained_limit_in_granule = limit_in_granule;

            if (rows_offset)
            {
                size_t skipped_rows = std::min(rows_offset, limit_in_granule);
                if (state.compact_discr != ColumnVariant::NULL_DISCRIMINATOR)
                    variant_rows_offsets[state.compact_discr] += skipped_rows;

                remained_limit_in_granule -= skipped_rows;
                rows_offset -= skipped_rows;
            }

            if (remained_limit_in_granule && state.compact_discr != ColumnVariant::NULL_DISCRIMINATOR)
                variant_limits[state.compact_discr] += remained_limit_in_granule;
        }
        else
        {
            SerializationNumber<ColumnVariant::Discriminator>().deserializeBinaryBulk(discriminators, *stream, 0, limit_in_granule, 0);
            size_t start = discriminators_data.size() - limit_in_granule;
            size_t skipped_rows = std::min(rows_offset, limit_in_granule);

            for (size_t i = start; i != start + skipped_rows; ++i)
            {
                ColumnVariant::Discriminator discr = discriminators_data[i];
                if (discr != ColumnVariant::NULL_DISCRIMINATOR)
                    ++variant_rows_offsets[discr];
            }

            for (size_t i = start + skipped_rows; i != discriminators_data.size(); ++i)
            {
                ColumnVariant::Discriminator discr = discriminators_data[i];
                if (discr != ColumnVariant::NULL_DISCRIMINATOR)
                    ++variant_limits[discr];
            }

            rows_offset -= skipped_rows;
        }

        state.remaining_rows_in_granule -= limit_in_granule;
        limit -= limit_in_granule;
    }

    return {variant_rows_offsets, variant_limits};
}

void SerializationVariant::readDiscriminatorsGranuleStart(DeserializeBinaryBulkStateVariantDiscriminators & state, DB::ReadBuffer * stream)
{
    UInt64 granule_size;
    readVarUInt(granule_size, *stream);
    state.remaining_rows_in_granule = granule_size;
    UInt8 granule_format;
    readBinaryLittleEndian(granule_format, *stream);
    state.granule_format = static_cast<CompactDiscriminatorsGranuleFormat>(granule_format);
    if (granule_format == CompactDiscriminatorsGranuleFormat::COMPACT)
        readBinaryLittleEndian(state.compact_discr, *stream);
}

void SerializationVariant::addVariantElementToPath(DB::ISerialization::SubstreamPath & path, size_t i) const
{
    path.push_back(Substream::VariantElement);
    path.back().variant_element_name = variant_names[i];
}

void SerializationVariant::serializeBinary(const Field & /*field*/, WriteBuffer & /*ostr*/, const FormatSettings & /*settings*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinary from a field is not implemented for SerializationVariant");
}

void SerializationVariant::deserializeBinary(Field & /*field*/, ReadBuffer & /*istr*/, const FormatSettings & /*settings*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method deserializeBinary to a field is not implemented for SerializationVariant");
}

void SerializationVariant::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnVariant & col = assert_cast<const ColumnVariant &>(column);
    auto global_discr = col.globalDiscriminatorAt(row_num);
    writeBinaryLittleEndian(global_discr, ostr);
    if (global_discr != ColumnVariant::NULL_DISCRIMINATOR)
        variants[global_discr]->serializeBinary(col.getVariantByGlobalDiscriminator(global_discr), col.offsetAt(row_num), ostr, settings);
}

void SerializationVariant::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    ColumnVariant & col = assert_cast<ColumnVariant &>(column);
    ColumnVariant::Discriminator global_discr;
    readBinaryLittleEndian(global_discr, istr);
    if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
    {
        col.insertDefault();
    }
    else if (global_discr < variants.size())
    {
        auto & variant_column = col.getVariantByGlobalDiscriminator(global_discr);
        variants[global_discr]->deserializeBinary(variant_column, istr, settings);
        col.getLocalDiscriminators().push_back(col.localDiscriminatorByGlobal(global_discr));
        col.getOffsets().push_back(variant_column.size() - 1);
    }
    else
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot read value of {}: unexpected discriminator {}", variant_name, UInt64(global_discr));
    }
}

namespace
{

const std::unordered_map<TypeIndex, size_t> & getTypesTextDeserializePriorityMap()
{
    static std::unordered_map<TypeIndex, size_t> priority_map = []
    {
        static constexpr std::array priorities = {
            /// Complex types have highest priority.
            TypeIndex::Array,
            TypeIndex::Tuple,
            TypeIndex::Map,
            TypeIndex::AggregateFunction,

            /// Enums can be parsed both from strings and numbers.
            /// So they have high enough priority.
            TypeIndex::Enum8,
            TypeIndex::Enum16,

            /// Types that can be parsed from strings.
            TypeIndex::UUID,
            TypeIndex::IPv4,
            TypeIndex::IPv6,

            /// Types that can be parsed from numbers.
            /// The order:
            ///    1) Integers
            ///    2) Big Integers
            ///    3) Decimals
            ///    4) Floats
            /// In each group small types have higher priority.
            TypeIndex::Int8,
            TypeIndex::UInt8,
            TypeIndex::Int16,
            TypeIndex::UInt16,
            TypeIndex::Int32,
            TypeIndex::UInt32,
            TypeIndex::Int64,
            TypeIndex::UInt64,
            TypeIndex::Int128,
            TypeIndex::UInt128,
            TypeIndex::Int256,
            TypeIndex::UInt256,
            TypeIndex::Decimal32,
            TypeIndex::Decimal64,
            TypeIndex::Decimal128,
            TypeIndex::Decimal256,
            TypeIndex::Float32,
            TypeIndex::Float64,

            /// Dates and DateTimes. More simple Date types have higher priority.
            /// They have lower priority as numbers as some DateTimes sometimes can
            /// be also parsed from numbers, but we don't want it usually.
            TypeIndex::Date,
            TypeIndex::Date32,
            TypeIndex::DateTime,
            TypeIndex::DateTime64,

            /// String types have almost the lowest priority,
            /// as in text formats almost all data can
            /// be deserialized into String type.
            TypeIndex::FixedString,
            TypeIndex::String,
        };

        std::unordered_map<TypeIndex, size_t> pm;

        pm.reserve(priorities.size());
        for (size_t i = 0; i != priorities.size(); ++i)
            pm[priorities[i]] = priorities.size() - i;
        return pm;
    }();

    return priority_map;
}

/// We want to create more or less optimal order of types in which we will try text deserializations.
/// To do it, for each type we calculate a priority and then sort them by this priority.
/// Above we defined priority of each data type, but types can be nested and also we can have LowCardinality and Nullable.
/// To sort any nested types we create a priority that is a tuple of 3 elements:
/// 1) The maximum depth of nested types like Array/Map/Tuple.
/// 2) The combination of simple and complex types priorities.
/// 3) The depth of nested types LowCardinality/Nullable.
/// So, when we will sort types, first we will sort by the maximum depth of nested types, so more nested types are deserialized first,
/// then for types with the same depth we sort by the types priority, and last we sort by the depth of LowCardinality/Nullable types,
/// so if we have types with the same level of nesting and the same priority, we will first try to deserialize LowCardinality/Nullable types
/// (for example if we have types Array(Array(String)) and Array(Array(Nullable(String))).
/// This is just a batch of heuristics.
std::tuple<size_t, size_t, size_t> getTypeTextDeserializePriority(const DataTypePtr & type, size_t nested_depth, size_t simple_nested_depth, const std::unordered_map<TypeIndex, size_t> & priority_map)
{
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
        return getTypeTextDeserializePriority(nullable_type->getNestedType(), nested_depth, simple_nested_depth + 1, priority_map);

    if (const auto * lc_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return getTypeTextDeserializePriority(lc_type->getDictionaryType(), nested_depth, simple_nested_depth + 1, priority_map);

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        auto [elements_nested_depth, elements_priority, elements_simple_nested_depth] = getTypeTextDeserializePriority(array_type->getNestedType(), nested_depth + 1, simple_nested_depth, priority_map);
        return {elements_nested_depth, elements_priority + priority_map.at(TypeIndex::Array), elements_simple_nested_depth};
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        size_t max_nested_depth = 0;
        size_t sum_priority = 0;
        size_t max_simple_nested_depth = 0;
        for (const auto & elem : tuple_type->getElements())
        {
            auto [elem_nested_depth, elem_priority, elem_simple_nested_depth] = getTypeTextDeserializePriority(elem, nested_depth + 1, simple_nested_depth, priority_map);
            sum_priority += elem_priority;
            max_nested_depth = std::max(elem_nested_depth, max_nested_depth);
            max_simple_nested_depth = std::max(elem_simple_nested_depth, max_simple_nested_depth);
        }

        return {max_nested_depth, sum_priority + priority_map.at(TypeIndex::Tuple), max_simple_nested_depth};
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
    {
        auto [key_max_depth, key_priority, key_simple_nested_depth] = getTypeTextDeserializePriority(map_type->getKeyType(), nested_depth + 1, simple_nested_depth, priority_map);
        auto [value_max_depth, value_priority, value_simple_nested_depth] = getTypeTextDeserializePriority(map_type->getValueType(), nested_depth + 1, simple_nested_depth, priority_map);
        return {std::max(key_max_depth, value_max_depth), key_priority + value_priority + priority_map.at(TypeIndex::Map), std::max(key_simple_nested_depth, value_simple_nested_depth)};
    }

    if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(type.get()))
    {
        size_t max_priority = 0;
        size_t max_depth = 0;
        size_t max_simple_nested_depth = 0;
        for (const auto & variant : variant_type->getVariants())
        {
            auto [variant_max_depth, variant_priority, variant_simple_nested_depth] = getTypeTextDeserializePriority(variant, nested_depth, simple_nested_depth, priority_map);
            max_priority = std::max(variant_priority, max_priority);
            max_depth = std::max(variant_max_depth, max_depth);
            max_simple_nested_depth = std::max(variant_simple_nested_depth, max_simple_nested_depth);
        }

        return {max_depth, max_priority, max_simple_nested_depth};
    }

    /// Bool type should have priority higher then all integers.
    if (isBool(type))
        return {nested_depth, priority_map.at(TypeIndex::Int8) + 1, simple_nested_depth};

    auto it = priority_map.find(type->getTypeId());
    return {nested_depth, it == priority_map.end() ? 0 : it->second, simple_nested_depth};
}

}

std::vector<size_t> SerializationVariant::getVariantsDeserializeTextOrder(const DB::DataTypes & variant_types)
{
    std::vector<std::tuple<size_t, size_t, size_t>> priorities;
    priorities.reserve(variant_types.size());
    std::vector<size_t> order;
    order.reserve(variant_types.size());
    const auto & priority_map = getTypesTextDeserializePriorityMap();
    for (size_t i = 0; i != variant_types.size(); ++i)
    {
        priorities.push_back(getTypeTextDeserializePriority(variant_types[i], 0, 0, priority_map));
        order.push_back(i);
    }

    std::sort(order.begin(), order.end(), [&](size_t left, size_t right) { return priorities[left] > priorities[right]; });
    return order;
}


bool SerializationVariant::tryDeserializeImpl(
    IColumn & column,
    const String & field,
    std::function<bool(ReadBuffer &)> check_for_null,
    std::function<bool(IColumn & variant_column, const SerializationPtr & variant_serialization, ReadBuffer &, const FormatSettings &)> try_deserialize_nested,
    const FormatSettings & settings) const
{
    auto & column_variant = assert_cast<ColumnVariant &>(column);
    ReadBufferFromString null_buf(field);
    if (check_for_null(null_buf) && null_buf.eof())
    {
        column_variant.insertDefault();
        return true;
    }

    FormatSettings modified_settings = settings;
    modified_settings.allow_special_bool_values = settings.allow_special_bool_values_inside_variant;
    for (size_t global_discr : deserialize_text_order)
    {
        ReadBufferFromString variant_buf(field);
        auto & variant_column = column_variant.getVariantByGlobalDiscriminator(global_discr);
        size_t prev_size = variant_column.size();
        if (try_deserialize_nested(variant_column, variants[global_discr], variant_buf, modified_settings) && variant_buf.eof())
        {
            column_variant.getLocalDiscriminators().push_back(column_variant.localDiscriminatorByGlobal(global_discr));
            column_variant.getOffsets().push_back(prev_size);
            return true;
        }
        if (variant_column.size() > prev_size)
        {
            variant_column.popBack(1);
        }
    }

    return false;
}

void SerializationVariant::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnVariant & col = assert_cast<const ColumnVariant &>(column);
    auto global_discr = col.globalDiscriminatorAt(row_num);
    if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
        SerializationNullable::serializeNullEscaped(ostr, settings);
    else
        variants[global_discr]->serializeTextEscaped(col.getVariantByGlobalDiscriminator(global_discr), col.offsetAt(row_num), ostr, settings);
}

bool SerializationVariant::tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    settings.tsv.crlf_end_of_line_input ? readEscapedStringCRLF(field, istr) : readEscapedString(field, istr);
    return tryDeserializeTextEscapedImpl(column, field, settings);
}

void SerializationVariant::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    settings.tsv.crlf_end_of_line_input ? readEscapedStringCRLF(field, istr) : readEscapedString(field, istr);
    if (!tryDeserializeTextEscapedImpl(column, field, settings))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse escaped value of type {} here: {}", variant_name, field);
}

bool SerializationVariant::tryDeserializeTextEscapedImpl(DB::IColumn & column, const String & field, const DB::FormatSettings & settings) const
{
    auto check_for_null = [&](ReadBuffer & buf)
    {
        return SerializationNullable::tryDeserializeNullEscaped(buf, settings);
    };
    auto try_deserialize_variant = [](IColumn & variant_column, const SerializationPtr & variant_serialization, ReadBuffer & buf, const FormatSettings & settings_)
    {
        return variant_serialization->tryDeserializeTextEscaped(variant_column, buf, settings_);
    };

    return tryDeserializeImpl(column, field, check_for_null, try_deserialize_variant, settings);
}

void SerializationVariant::serializeTextRaw(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnVariant & col = assert_cast<const ColumnVariant &>(column);
    auto global_discr = col.globalDiscriminatorAt(row_num);
    if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
        SerializationNullable::serializeNullRaw(ostr, settings);
    else
        variants[global_discr]->serializeTextRaw(col.getVariantByGlobalDiscriminator(global_discr), col.offsetAt(row_num), ostr, settings);
}

bool SerializationVariant::tryDeserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    readString(field, istr);
    return tryDeserializeTextRawImpl(column, field, settings);
}

void SerializationVariant::deserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    readString(field, istr);
    if (!tryDeserializeTextRawImpl(column, field, settings))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse raw value of type {} here: {}", variant_name, field);
}

bool SerializationVariant::tryDeserializeTextRawImpl(DB::IColumn & column, const String & field, const DB::FormatSettings & settings) const
{
    auto check_for_null = [&](ReadBuffer & buf)
    {
        return SerializationNullable::tryDeserializeNullRaw(buf, settings);
    };
    auto try_deserialize_variant = [](IColumn & variant_column, const SerializationPtr & variant_serialization, ReadBuffer & buf, const FormatSettings & settings_)
    {
        return variant_serialization->tryDeserializeTextRaw(variant_column, buf, settings_);
    };

    return tryDeserializeImpl(column, field, check_for_null, try_deserialize_variant, settings);
}

void SerializationVariant::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnVariant & col = assert_cast<const ColumnVariant &>(column);
    auto global_discr = col.globalDiscriminatorAt(row_num);
    if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
        SerializationNullable::serializeNullQuoted(ostr);
    else
        variants[global_discr]->serializeTextQuoted(col.getVariantByGlobalDiscriminator(global_discr), col.offsetAt(row_num), ostr, settings);
}

bool SerializationVariant::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    if (!tryReadQuotedField(field, istr))
        return false;
    return tryDeserializeTextQuotedImpl(column, field, settings);
}

void SerializationVariant::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    readQuotedField(field, istr);
    if (!tryDeserializeTextQuotedImpl(column, field, settings))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse quoted value of type {} here: {}", variant_name, field);
}

bool SerializationVariant::tryDeserializeTextQuotedImpl(DB::IColumn & column, const String & field, const DB::FormatSettings & settings) const
{
    auto check_for_null = [&](ReadBuffer & buf)
    {
        return SerializationNullable::tryDeserializeNullQuoted(buf);
    };
    auto try_deserialize_variant = [](IColumn & variant_column, const SerializationPtr & variant_serialization, ReadBuffer & buf, const FormatSettings & settings_)
    {
        return variant_serialization->tryDeserializeTextQuoted(variant_column, buf, settings_);
    };

    return tryDeserializeImpl(column, field, check_for_null, try_deserialize_variant, settings);
}

void SerializationVariant::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnVariant & col = assert_cast<const ColumnVariant &>(column);
    auto global_discr = col.globalDiscriminatorAt(row_num);
    if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
        SerializationNullable::serializeNullCSV(ostr, settings);
    else
        variants[global_discr]->serializeTextCSV(col.getVariantByGlobalDiscriminator(global_discr), col.offsetAt(row_num), ostr, settings);
}

bool SerializationVariant::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    readCSVStringInto<String, true, false>(field, istr, settings.csv);
    return tryDeserializeTextCSVImpl(column, field, settings);
}

void SerializationVariant::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    readCSVField(field, istr, settings.csv);
    if (!tryDeserializeTextCSVImpl(column, field, settings))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse CSV value of type {} here: {}", variant_name, field);
}

bool SerializationVariant::tryDeserializeTextCSVImpl(DB::IColumn & column, const String & field, const DB::FormatSettings & settings) const
{
    auto check_for_null = [&](ReadBuffer & buf)
    {
        return SerializationNullable::tryDeserializeNullCSV(buf, settings);
    };
    auto try_deserialize_variant = [](IColumn & variant_column, const SerializationPtr & variant_serialization, ReadBuffer & buf, const FormatSettings & settings_)
    {
        return variant_serialization->tryDeserializeTextCSV(variant_column, buf, settings_);
    };

    return tryDeserializeImpl(column, field, check_for_null, try_deserialize_variant, settings);
}

void SerializationVariant::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnVariant & col = assert_cast<const ColumnVariant &>(column);
    auto global_discr = col.globalDiscriminatorAt(row_num);
    if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
        SerializationNullable::serializeNullText(ostr, settings);
    else
        variants[global_discr]->serializeText(col.getVariantByGlobalDiscriminator(global_discr), col.offsetAt(row_num), ostr, settings);
}

bool SerializationVariant::tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    readStringUntilEOF(field, istr);
    return tryDeserializeWholeTextImpl(column, field, settings);
}

void SerializationVariant::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    readStringUntilEOF(field, istr);
    if (!tryDeserializeWholeTextImpl(column, field, settings))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse text value of type {} here: {}", variant_name, field);
}

bool SerializationVariant::tryDeserializeWholeTextImpl(DB::IColumn & column, const String & field, const DB::FormatSettings & settings) const
{
    auto check_for_null = [&](ReadBuffer & buf)
    {
        return SerializationNullable::tryDeserializeNullText(buf);
    };
    auto try_deserialize_variant = [](IColumn & variant_column, const SerializationPtr & variant_serialization, ReadBuffer & buf, const FormatSettings & settings_)
    {
        return variant_serialization->tryDeserializeWholeText(variant_column, buf, settings_);
    };

    return tryDeserializeImpl(column, field, check_for_null, try_deserialize_variant, settings);
}

void SerializationVariant::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnVariant & col = assert_cast<const ColumnVariant &>(column);
    auto global_discr = col.globalDiscriminatorAt(row_num);
    if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
        SerializationNullable::serializeNullJSON(ostr);
    else
        variants[global_discr]->serializeTextJSON(col.getVariantByGlobalDiscriminator(global_discr), col.offsetAt(row_num), ostr, settings);
}

void SerializationVariant::serializeTextJSONPretty(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const
{
    const ColumnVariant & col = assert_cast<const ColumnVariant &>(column);
    auto global_discr = col.globalDiscriminatorAt(row_num);
    if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
        SerializationNullable::serializeNullJSON(ostr);
    else
        variants[global_discr]->serializeTextJSONPretty(col.getVariantByGlobalDiscriminator(global_discr), col.offsetAt(row_num), ostr, settings, indent);
}

bool SerializationVariant::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    if (!tryReadJSONField(field, istr, settings.json))
        return false;
    return tryDeserializeTextJSONImpl(column, field, settings);
}

void SerializationVariant::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String field;
    readJSONField(field, istr, settings.json);
    if (!tryDeserializeTextJSONImpl(column, field, settings))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse JSON value of type {} here: {}", variant_name, field);
}

bool SerializationVariant::tryDeserializeTextJSONImpl(DB::IColumn & column, const String & field, const DB::FormatSettings & settings) const
{
    auto check_for_null = [&](ReadBuffer & buf)
    {
        return SerializationNullable::tryDeserializeNullJSON(buf);
    };
    auto try_deserialize_variant = [](IColumn & variant_column, const SerializationPtr & variant_serialization, ReadBuffer & buf, const FormatSettings & settings_)
    {
        return variant_serialization->tryDeserializeTextJSON(variant_column, buf, settings_);
    };

    return tryDeserializeImpl(column, field, check_for_null, try_deserialize_variant, settings);
}

void SerializationVariant::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnVariant & col = assert_cast<const ColumnVariant &>(column);
    auto global_discr = col.globalDiscriminatorAt(row_num);
    if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
        SerializationNullable::serializeNullXML(ostr);
    else
        variants[global_discr]->serializeTextXML(col.getVariantByGlobalDiscriminator(global_discr), col.offsetAt(row_num), ostr, settings);
}

}
