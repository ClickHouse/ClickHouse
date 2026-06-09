#include <Common/SipHash.h>
#include <DataTypes/Serializations/SerializationDynamic.h>
#include <DataTypes/Serializations/SerializationVariant.h>
#include <DataTypes/Serializations/SerializationDynamicHelpers.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/DataTypesCache.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnNullable.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Formats/EscapingRuleUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

UInt128 SerializationDynamic::getHash(size_t max_dynamic_types_, const SerializationInfoSettings & serialization_info_settings_)
{
    SipHash hash;
    hash.update("Dynamic");
    hash.update(max_dynamic_types_);
    serialization_info_settings_.updateHash(hash);
    return hash.get128();
}

struct SerializeBinaryBulkStateDynamic : public ISerialization::SerializeBinaryBulkState
{
    SerializationDynamic::SerializationVersion structure_version;
    size_t num_dynamic_types{};
    DataTypePtr variant_type;
    Names variant_names;
    SerializationPtr variant_serialization;
    ISerialization::SerializeBinaryBulkStatePtr variant_state;

    /// Variants statistics.
    ColumnDynamic::Statistics statistics;
    /// If true, statistics will be recalculated during serialization.
    bool recalculate_statistics = false;

    /// For NARROWED serialization only.
    /// Concrete variant type the column was narrowed to (without Nullable wrapper).
    DataTypePtr narrowed_type;
    /// Whether the on-disk data is `Nullable(narrowed_type)` (true) or just `narrowed_type` (false).
    bool narrowed_stored_as_nullable = false;
    /// Serialization for the on-disk layout (`Nullable(narrowed_type)` or `narrowed_type`).
    SerializationPtr narrowed_serialization;
    ISerialization::SerializeBinaryBulkStatePtr narrowed_state;

    /// For flattened serialization only.
    std::optional<FlattenedDynamicColumn> flattened_column;
    std::vector<ISerialization::SerializeBinaryBulkStatePtr> flattened_states;
    ISerialization::SerializeBinaryBulkStatePtr flattened_indexes_state;

    explicit SerializeBinaryBulkStateDynamic(SerializationDynamic::SerializationVersion structure_version_)
        : structure_version(structure_version_)
    {
    }
};

struct DeserializeBinaryBulkStateDynamic : public ISerialization::DeserializeBinaryBulkState
{
    SerializationPtr variant_serialization;
    ISerialization::DeserializeBinaryBulkStatePtr variant_state;
    ISerialization::DeserializeBinaryBulkStatePtr structure_state;

    /// For NARROWED serialization only.
    /// Serialization that reads the on-disk layout (`Nullable(narrowed_type)` or `narrowed_type`).
    SerializationPtr narrowed_serialization;
    ISerialization::DeserializeBinaryBulkStatePtr narrowed_state;

    /// For flattened serialization only.
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> flattened_states;
    ISerialization::DeserializeBinaryBulkStatePtr flattened_indexes_state;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateDynamic>(*this);
        new_state->variant_state = variant_state ? variant_state->clone() : nullptr;
        new_state->structure_state = structure_state ? structure_state->clone() : nullptr;
        new_state->narrowed_state = narrowed_state ? narrowed_state->clone() : nullptr;
        return new_state;
    }
};

namespace
{

/// Detect single-type narrowing from reliable Dynamic statistics. Returns:
///   - the concrete variant type (non-Nullable) if the statistics show exactly one non-empty,
///     non-SharedVariant variant and `shared_variants_statistics` is empty;
///   - the type's `canBeInsideNullable()` is checked because the on-disk layout always wraps
///     with `Nullable` to represent NULL rows uniformly;
///   - nullptr otherwise.
///
/// IMPORTANT: callers must verify `statistics->isReliable()` first.
DataTypePtr detectNarrowedTypeFromStatistics(
    const ColumnDynamic::Statistics & statistics,
    const Names & variant_names,
    const DataTypeVariant & variant_type)
{
    chassert(statistics.isReliable());

    /// Shared variant must be empty: any value stuck in shared data implies multiple types.
    if (!statistics.shared_variants_statistics.empty())
        return nullptr;

    /// Look for exactly one variant with count > 0 that is not the SharedVariant placeholder.
    const String shared_variant_name = ColumnDynamic::getSharedVariantTypeName();
    std::optional<size_t> single_non_empty_index;
    for (size_t i = 0; i != variant_names.size(); ++i)
    {
        if (variant_names[i] == shared_variant_name)
            continue;
        auto it = statistics.variants_statistics.find(variant_names[i]);
        if (it == statistics.variants_statistics.end() || it->second == 0)
            continue;
        if (single_non_empty_index)
            return nullptr;
        single_non_empty_index = i;
    }

    if (!single_non_empty_index)
        return nullptr;

    auto type = variant_type.getVariants().at(*single_non_empty_index);
    /// The on-disk layout uses `Nullable(T)` to encode rows that are NULL in the Dynamic column.
    /// If T already cannot live inside Nullable (e.g., `Array(Nullable(String))`), fall back to V3.
    if (!type->canBeInsideNullable())
        return nullptr;
    return type;
}

/// Materialize the on-disk column for NARROWED serialization from an in-memory Dynamic column.
/// All rows whose discriminator is NULL_DISCRIMINATOR or any variant other than `narrowed_type`
/// become NULL in the produced column. Rows that match `narrowed_type` are copied through.
/// Rest of the variant slots in the source column must be empty in a reliably-narrowed part —
/// this invariant is checked via the statistics guard at prefix time.
ColumnPtr buildNarrowedColumn(
    const ColumnDynamic & column_dynamic,
    const DataTypePtr & narrowed_type,
    bool stored_as_nullable,
    size_t offset,
    size_t limit)
{
    const auto & variant_info = column_dynamic.getVariantInfo();
    const auto & variant_column = column_dynamic.getVariantColumn();
    auto it = variant_info.variant_name_to_discriminator.find(narrowed_type->getName());
    if (it == variant_info.variant_name_to_discriminator.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Narrowed Dynamic column does not contain the expected variant {}", narrowed_type->getName());

    auto narrowed_global_discr = it->second;
    auto narrowed_local_discr = variant_column.localDiscriminatorByGlobal(narrowed_global_discr);
    const auto & local_discriminators = variant_column.getLocalDiscriminators();
    const auto & variant_offsets = variant_column.getOffsets();
    const auto & narrowed_variant_data = variant_column.getVariantByGlobalDiscriminator(narrowed_global_discr);

    size_t end = (limit == 0 || offset + limit > local_discriminators.size())
        ? local_discriminators.size()
        : offset + limit;

    if (stored_as_nullable)
    {
        auto nullable_type = makeNullable(narrowed_type);
        auto storage_column = nullable_type->createColumn();
        auto & nullable = assert_cast<ColumnNullable &>(*storage_column);
        auto & nested = nullable.getNestedColumn();
        auto & null_map = nullable.getNullMapData();
        nested.reserve(end - offset);
        null_map.reserve(end - offset);
        for (size_t i = offset; i != end; ++i)
        {
            const auto discr = local_discriminators[i];
            if (discr == narrowed_local_discr)
            {
                nested.insertFrom(narrowed_variant_data, variant_offsets[i]);
                null_map.push_back(static_cast<UInt8>(0));
            }
            else if (discr == ColumnVariant::NULL_DISCRIMINATOR)
            {
                nested.insertDefault();
                null_map.push_back(static_cast<UInt8>(1));
            }
            else
            {
                /// The narrowing precondition is "exactly one non-empty variant" — any other
                /// variant must have been empty when the decision was made. If we reach here
                /// the block has rows whose discriminator is neither the narrowed variant nor
                /// NULL. The narrowing decision is no longer valid; we cannot represent these
                /// rows on disk. This usually means the writer received blocks whose Dynamic
                /// structure changed across `serializeBinaryBulkWithMultipleStreams` calls
                /// (e.g., mutations that produce multiple blocks with different variant types).
                /// Fail loudly so the user can disable v4 narrowing for this workload.
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Dynamic NARROWED layout cannot encode row {} of variant {} as Nullable({}). "
                    "The narrowing decision was made at prefix-time based on block_sample statistics "
                    "but the actual data contains values outside the narrowed variant. Disable "
                    "`dynamic_serialization_version = 'v4'` for this workload.",
                    i, static_cast<int>(discr), narrowed_type->getName());
            }
        }
        return storage_column;
    }

    /// stored_as_nullable == false: caller guarantees no NULL rows. Just copy the variant data.
    auto storage_column = narrowed_type->createColumn();
    storage_column->reserve(end - offset);
    for (size_t i = offset; i != end; ++i)
    {
        if (local_discriminators[i] != narrowed_local_discr)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Dynamic NARROWED layout (non-nullable variant) requires every row to use the "
                "narrowed variant. Row {} has discriminator {}, expected {}. Disable "
                "`dynamic_serialization_version = 'v4'` for this workload.",
                i, static_cast<int>(local_discriminators[i]), static_cast<int>(narrowed_local_discr));
        storage_column->insertFrom(narrowed_variant_data, variant_offsets[i]);
    }
    return storage_column;
}

}

void SerializationDynamic::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    settings.path.push_back(Substream::DynamicStructure);
    callback(settings.path);
    settings.path.pop_back();

    const auto * column_dynamic = data.column ? &assert_cast<const ColumnDynamic &>(*data.column) : nullptr;
    const auto * deserialize_state = data.deserialize_state ? checkAndGetState<DeserializeBinaryBulkStateDynamic>(data.deserialize_state) : nullptr;

    /// If column is nullptr and we don't have deserialize state yet, nothing to enumerate as we don't have any variants.
    if (!settings.enumerate_dynamic_streams || (!column_dynamic && !deserialize_state))
        return;

    /// If we already know the part uses the NARROWED layout, the data substreams are those of
    /// `Nullable(narrowed_type)` (or `narrowed_type`), not the Variant streams. We can know this
    /// from a deserialize state (read path) or from reliable statistics on the column when the
    /// writer asks for the v4 layout (write path).
    auto enumerate_narrowed = [&](const DataTypePtr & narrowed_type, bool stored_as_nullable, const auto * read_state)
    {
        auto storage_type = stored_as_nullable ? makeNullable(narrowed_type) : narrowed_type;
        auto storage_serialization = storage_type->getSerialization(serialization_info_settings);
        settings.path.push_back(Substream::DynamicData);
        auto storage_data = SubstreamData(storage_serialization)
                             .withType(storage_type)
                             .withSerializationInfo(data.serialization_info)
                             .withDeserializeState(read_state ? read_state->narrowed_state : ISerialization::DeserializeBinaryBulkStatePtr{});
        settings.path.back().data = storage_data;
        storage_serialization->enumerateStreams(settings, callback, storage_data);
        settings.path.pop_back();
    };

    if (deserialize_state)
    {
        const auto * structure_state = checkAndGetState<DeserializeBinaryBulkStateDynamicStructure>(deserialize_state->structure_state);
        if (structure_state->structure_version.value == SerializationVersion::NARROWED)
        {
            enumerate_narrowed(structure_state->narrowed_type, structure_state->narrowed_stored_as_nullable, deserialize_state);
            return;
        }
    }
    else if (column_dynamic
        && settings.dynamic_serialization_version == MergeTreeDynamicSerializationVersion::V4
        && settings.data_part_type == MergeTreeDataPartType::Wide)
    {
        /// Write path: detect narrowing from in-memory statistics, just like the prefix writer does.
        const auto & stats_ptr = column_dynamic->getStatistics();
        if (stats_ptr && stats_ptr->isReliable())
        {
            const auto & variant_type = assert_cast<const DataTypeVariant &>(*column_dynamic->getVariantInfo().variant_type);
            auto narrowed_type = detectNarrowedTypeFromStatistics(*stats_ptr, column_dynamic->getVariantInfo().variant_names, variant_type);
            if (narrowed_type)
            {
                /// Wrap in `Nullable` unless statistics prove there are no NULL rows in the part.
                /// `null_count == nullopt` means "unknown" — conservative choice is to wrap.
                /// Must match the choice made by `serializeBinaryBulkStatePrefix`.
                const bool stored_as_nullable = !stats_ptr->null_count || *stats_ptr->null_count > 0;
                enumerate_narrowed(narrowed_type, stored_as_nullable, static_cast<const DeserializeBinaryBulkStateDynamic *>(nullptr));
                return;
            }
        }
    }

    const auto & variant_type = column_dynamic ? column_dynamic->getVariantInfo().variant_type : checkAndGetState<DeserializeBinaryBulkStateDynamicStructure>(deserialize_state->structure_state)->variant_type;
    auto variant_serialization = variant_type->getSerialization(serialization_info_settings);

    settings.path.push_back(Substream::DynamicData);
    auto variant_data = SubstreamData(variant_serialization)
                         .withType(variant_type)
                         .withColumn(column_dynamic ? column_dynamic->getVariantColumnPtr() : nullptr)
                         .withSerializationInfo(data.serialization_info)
                         .withDeserializeState(deserialize_state ? deserialize_state->variant_state : nullptr);
    settings.path.back().data = variant_data;
    variant_serialization->enumerateStreams(settings, callback, variant_data);
    settings.path.pop_back();
}

SerializationDynamic::SerializationVersion::SerializationVersion(UInt64 version) : value(static_cast<Value>(version))
{
    checkVersion(version);
}

void SerializationDynamic::SerializationVersion::checkVersion(UInt64 version)
{
    if (version != V1 && version != V2 && version != FLATTENED && version != V3 && version != NARROWED)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid version for Dynamic structure serialization: {}", version);
}

SerializationDynamic::SerializationVersion::SerializationVersion(MergeTreeDynamicSerializationVersion version)
{
    switch (version)
    {
        case MergeTreeDynamicSerializationVersion::V1:
            value = V1;
            break;
        case MergeTreeDynamicSerializationVersion::V2:
            value = V2;
            break;
        case MergeTreeDynamicSerializationVersion::V3:
            value = V3;
            break;
        case MergeTreeDynamicSerializationVersion::V4:
            /// V4 starts as V3 layout. The writer upgrades it to NARROWED per part when
            /// statistics show a single non-empty variant.
            value = V3;
            break;
    }
}

void SerializationDynamic::serializeBinaryBulkStatePrefix(
    const DB::IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    const auto & column_dynamic = assert_cast<const ColumnDynamic &>(column);
    const auto & variant_info = column_dynamic.getVariantInfo();

    settings.path.push_back(Substream::DynamicStructure);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing stream for Dynamic column structure during serialization of binary bulk state prefix");

    /// Choose serialization type.
    /// By default we use serialization V2.
    SerializationVersion structure_version(settings.dynamic_serialization_version);
    /// Check if we are writing data in Native format and have FLATTENED serialization enabled.
    if (settings.native_format && settings.format_settings && settings.format_settings->native.use_flattened_dynamic_and_json_serialization)
        structure_version = SerializationVersion(SerializationVersion::FLATTENED);

    /// Try to upgrade to the NARROWED layout when the user explicitly opted in via `v4`.
    /// Gates (all must hold):
    ///   - the V3 layout was already chosen (FLATTENED/V1/V2 are not eligible);
    ///   - the user asked for `v4` (we conservatively only narrow when explicitly requested);
    ///   - the writer is producing a Wide MergeTree part — Compact parts write statistics in the
    ///     prefix, which only reflects the first granule and is unreliable for narrowing decisions
    ///     (the read side enforces the same rule via `Statistics::reliable`);
    ///   - statistics are reliable AND show a single non-empty variant (see `detectNarrowedTypeFromStatistics`);
    ///   - the writer is producing a real MergeTree part (write_statistics != NONE excludes
    ///     `SerializationObjectSharedData` sub-paths which write nested Dynamic columns with no
    ///     statistics);
    ///   - the writer is not producing a Native protocol stream.
    DataTypePtr narrowed_type;
    bool narrowed_stored_as_nullable = false;
    if (structure_version.value == SerializationVersion::V3
        && settings.dynamic_serialization_version == MergeTreeDynamicSerializationVersion::V4
        && settings.data_part_type == MergeTreeDataPartType::Wide
        && settings.write_statistics != SerializeBinaryBulkSettings::StatisticsMode::NONE
        && !settings.native_format)
    {
        const auto & maybe_stats = column_dynamic.getStatistics();
        if (maybe_stats && maybe_stats->isReliable())
        {
            const auto & variant_type_ref = assert_cast<const DataTypeVariant &>(*variant_info.variant_type);
            narrowed_type = detectNarrowedTypeFromStatistics(*maybe_stats, variant_info.variant_names, variant_type_ref);
            if (narrowed_type)
            {
                structure_version = SerializationVersion(SerializationVersion::NARROWED);
                /// Skip the `.null` substream only when statistics prove there are no NULL rows in
                /// the part. `null_count == nullopt` ("unknown") falls back to wrapping in Nullable
                /// — that is the conservative choice because foreign-variant rows after the prefix
                /// decision would otherwise crash `buildNarrowedColumn` (non-nullable branch).
                narrowed_stored_as_nullable = !maybe_stats->null_count || *maybe_stats->null_count > 0;
            }
        }
    }

    /// Write selected structure serialization version.
    writeBinaryLittleEndian(static_cast<UInt64>(structure_version.value), *stream);

    auto dynamic_state = std::make_shared<SerializeBinaryBulkStateDynamic>(structure_version);
    if (structure_version.value == SerializationVersion::FLATTENED)
    {
        auto flattened_column = flattenDynamicColumn(column_dynamic);
        /// Write the list of all flattened types.
        writeVarUInt(flattened_column.types.size(), *stream);
        for (const auto & type : flattened_column.types)
        {
            if (settings.native_format && settings.format_settings && settings.format_settings->native.encode_types_in_binary_format)
                encodeDataType(type, *stream);
            else
                writeStringBinary(type->getName(), *stream);
        }

        /// Write prefixes for indexes and all flattened types.
        settings.path.push_back(Substream::DynamicData);
        auto indexes_serialization = flattened_column.indexes_type->getDefaultSerialization();
        indexes_serialization->serializeBinaryBulkStatePrefix(*flattened_column.indexes_column, settings, dynamic_state->flattened_indexes_state);
        for (size_t i = 0; i != flattened_column.types.size(); ++i)
        {
            auto serialization = flattened_column.types[i]->getDefaultSerialization();
            dynamic_state->flattened_states.emplace_back();
            serialization->serializeBinaryBulkStatePrefix(*flattened_column.columns[i], settings, dynamic_state->flattened_states.back());
        }
        settings.path.pop_back();

        dynamic_state->flattened_column = std::move(flattened_column);
        state = std::move(dynamic_state);
        return;
    }

    dynamic_state->variant_type = variant_info.variant_type;
    dynamic_state->variant_names = variant_info.variant_names;
    const auto & variant_column = column_dynamic.getVariantColumn();

    /// Write information about dynamic types.
    dynamic_state->num_dynamic_types = dynamic_state->variant_names.size() - 1; ///  -1 for SharedVariant

    /// In V1 version we had max_dynamic_types parameter written, but now we need only actual number of variants.
    /// For compatibility we need to write V1 version sometimes, but we should write number of variants instead of
    /// max_dynamic_types (because now max_dynamic_types can be different in different serialized columns).
    if (structure_version.value == SerializationVersion::V1)
        writeVarUInt(dynamic_state->num_dynamic_types, *stream);

    writeVarUInt(dynamic_state->num_dynamic_types, *stream);
    /// V3 and NARROWED encode variant names in binary format. Other versions use the legacy
    /// length-prefixed string format unless `native_format` requests binary encoding.
    if ((settings.native_format && settings.format_settings && settings.format_settings->native.encode_types_in_binary_format)
        || structure_version.value == SerializationVersion::V3
        || structure_version.value == SerializationVersion::NARROWED)
    {
        const auto & variants = assert_cast<const DataTypeVariant &>(*dynamic_state->variant_type).getVariants();
        for (const auto & variant: variants)
        {
            if (variant->getName() != ColumnDynamic::getSharedVariantTypeName())
                encodeDataType(variant, *stream);
        }
    }
    else
    {
        for (const auto & name : dynamic_state->variant_names)
        {
            if (name != ColumnDynamic::getSharedVariantTypeName())
                writeStringBinary(name, *stream);
        }
    }

    /// Write statistics in prefix if needed.
    if (settings.write_statistics == SerializeBinaryBulkSettings::StatisticsMode::PREFIX)
    {
        const auto & statistics = column_dynamic.getOrCalculateStatistics();

        /// In V3 and NARROWED write flag that statistics is not empty.
        /// It is needed to be able to write empty statistics if needed.
        if (structure_version.value == SerializationVersion::V3 || structure_version.value == SerializationVersion::NARROWED)
            writeBinary(true, *stream);

        /// First, write statistics for usual variants.
        /// Statistics should always have entries for all variants, but just in case
        /// use `find` instead of `at` to avoid exceptions in release builds.
        for (const auto & variant_name : variant_info.variant_names)
        {
            auto it = statistics->variants_statistics.find(variant_name);
            chassert(it != statistics->variants_statistics.end());
            writeVarUInt(it != statistics->variants_statistics.end() ? it->second : 0, *stream);
        }

        /// Second, write statistics for variants in shared variant.
        writeVarUInt(statistics->shared_variants_statistics.size(), *stream);
        for (const auto & [variant_name, size] : statistics->shared_variants_statistics)
        {
            writeStringBinary(variant_name, *stream);
            writeVarUInt(size, *stream);
        }
    }
    else if (settings.write_statistics == SerializeBinaryBulkSettings::StatisticsMode::PREFIX_EMPTY)
    {
        /// V3 and NARROWED support an explicit "no statistics" flag.
        if (structure_version.value == SerializationVersion::V3 || structure_version.value == SerializationVersion::NARROWED)
        {
            writeBinary(false, *stream);
        }
        /// Otherwise serialize the minimum version of statistics.
        else
        {
            /// Write 0 for each variant.
            for (size_t i = 0; i != variant_info.variant_names.size(); ++i)
                writeVarUInt(0, *stream);

            /// Write 0 elements for shared variant statistics.
            writeVarUInt(0, *stream);
        }
    }
    /// Otherwise statistics will be written in the suffix, in this case we will recalculate
    /// statistics during serialization to make it more precise.
    else if (settings.write_statistics == SerializeBinaryBulkSettings::StatisticsMode::SUFFIX)
    {
        dynamic_state->recalculate_statistics = true;
    }

    if (structure_version.value == SerializationVersion::NARROWED)
    {
        /// For the NARROWED layout we always need to know `narrowed_type` and `stored_as_nullable`
        /// on the read side. The trailer location depends on `write_statistics`:
        ///   - PREFIX / PREFIX_EMPTY: the prefix already wrote (or omitted) the statistics block,
        ///     so the trailer goes right here, in the prefix, after the statistics block.
        ///   - SUFFIX: the prefix has not written statistics yet; the suffix will write the
        ///     statistics block, and the trailer must follow it. The suffix code handles that.
        if (settings.write_statistics != SerializeBinaryBulkSettings::StatisticsMode::SUFFIX)
        {
            encodeDataType(narrowed_type, *stream);
            writeBinary(narrowed_stored_as_nullable, *stream);
        }

        /// Substream layout for the data stream is `Nullable(narrowed_type)` (or `narrowed_type`).
        auto storage_type = narrowed_stored_as_nullable ? makeNullable(narrowed_type) : narrowed_type;
        dynamic_state->narrowed_type = narrowed_type;
        dynamic_state->narrowed_stored_as_nullable = narrowed_stored_as_nullable;
        dynamic_state->narrowed_serialization = storage_type->getSerialization(serialization_info_settings);

        /// We still need variant_type/serialization on the state because the suffix may need them
        /// (e.g., for the statistics block recalculated at suffix time).
        dynamic_state->variant_serialization = dynamic_state->variant_type->getSerialization(serialization_info_settings);

        /// Build the storage column for the prefix call. The prefix only inspects column structure,
        /// never row data, so any column with the right type is fine here — including an empty
        /// clone. Pass an empty sample built from the in-memory column.
        auto sample_storage = storage_type->createColumn();
        settings.path.push_back(Substream::DynamicData);
        dynamic_state->narrowed_serialization->serializeBinaryBulkStatePrefix(*sample_storage, settings, dynamic_state->narrowed_state);
        settings.path.pop_back();

        state = std::move(dynamic_state);
        return;
    }

    dynamic_state->variant_serialization = dynamic_state->variant_type->getSerialization(serialization_info_settings);
    settings.path.push_back(Substream::DynamicData);
    dynamic_state->variant_serialization->serializeBinaryBulkStatePrefix(variant_column, settings, dynamic_state->variant_state);
    settings.path.pop_back();

    state = std::move(dynamic_state);
}

void SerializationDynamic::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    DeserializeBinaryBulkStatePtr structure_state = deserializeDynamicStructureStatePrefix(settings, cache);
    if (!structure_state)
        return;

    auto dynamic_state = std::make_shared<DeserializeBinaryBulkStateDynamic>();
    dynamic_state->structure_state = std::move(structure_state);
    auto * structure_state_typed = checkAndGetState<DeserializeBinaryBulkStateDynamicStructure>(dynamic_state->structure_state);
    if (structure_state_typed->structure_version.value == SerializationVersion::FLATTENED)
    {
        dynamic_state->flattened_states.reserve(structure_state_typed->flattened_data_types.size());
        /// Read prefix of indexes and all flattened types.
        settings.path.push_back(Substream::DynamicData);
        structure_state_typed->flattened_indexes_type->getDefaultSerialization()->deserializeBinaryBulkStatePrefix(settings, dynamic_state->flattened_indexes_state, cache);
        for (const auto & type : structure_state_typed->flattened_data_types)
        {
            dynamic_state->flattened_states.emplace_back();
            type->getDefaultSerialization()->deserializeBinaryBulkStatePrefix(settings, dynamic_state->flattened_states.back(), cache);
        }
        settings.path.pop_back();
        state = std::move(dynamic_state);
        return;
    }

    dynamic_state->variant_serialization = structure_state_typed->variant_type->getSerialization(serialization_info_settings);

    settings.path.push_back(Substream::DynamicData);

    if (structure_state_typed->structure_version.value == SerializationVersion::NARROWED)
    {
        /// On-disk layout for NARROWED parts is just `Nullable(narrowed_type)` (or `narrowed_type`).
        /// We don't need to read variant-style prefix substreams.
        auto storage_type = structure_state_typed->narrowed_stored_as_nullable
            ? makeNullable(structure_state_typed->narrowed_type)
            : structure_state_typed->narrowed_type;
        dynamic_state->narrowed_serialization = storage_type->getSerialization(serialization_info_settings);
        dynamic_state->narrowed_serialization->deserializeBinaryBulkStatePrefix(settings, dynamic_state->narrowed_state, cache);
        settings.path.pop_back();
        state = std::move(dynamic_state);
        return;
    }

    /// Call callback for newly discovered dynamic subcolumns if needed.
    if (settings.dynamic_subcolumns_callback)
    {
        EnumerateStreamsSettings enumerate_settings;
        enumerate_settings.path = settings.path;
        dynamic_state->variant_serialization->enumerateStreams(enumerate_settings, settings.dynamic_subcolumns_callback, SubstreamData(dynamic_state->variant_serialization));
    }

    dynamic_state->variant_serialization->deserializeBinaryBulkStatePrefix(settings, dynamic_state->variant_state, cache);
    settings.path.pop_back();

    state = std::move(dynamic_state);
}

ISerialization::DeserializeBinaryBulkStatePtr SerializationDynamic::deserializeDynamicStructureStatePrefix(
    DeserializeBinaryBulkSettings & settings, SubstreamsDeserializeStatesCache * cache)
{
    settings.path.push_back(Substream::DynamicStructure);

    DeserializeBinaryBulkStatePtr state = nullptr;
    if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, settings.path))
    {
        state = std::move(cached_state);
    }
    else if (auto * structure_stream = settings.getter(settings.path))
    {
        /// Read structure serialization version.
        UInt64 structure_version = 0;
        readBinaryLittleEndian(structure_version, *structure_stream);
        auto structure_state = std::make_shared<DeserializeBinaryBulkStateDynamicStructure>(structure_version);
        if (structure_state->structure_version.value == SerializationVersion::FLATTENED)
        {
            /// Read the flattened list of types.
            size_t num_types = 0;
            readVarUInt(num_types, *structure_stream);
            structure_state->flattened_data_types.reserve(num_types);
            String data_type_name;
            for (size_t i = 0; i != num_types; ++i)
            {
                if (settings.native_format && settings.format_settings && settings.format_settings->native.decode_types_in_binary_format)
                {
                    structure_state->flattened_data_types.push_back(decodeDataType(*structure_stream));
                }
                else
                {
                    readStringBinary(data_type_name, *structure_stream);
                    structure_state->flattened_data_types.push_back(getDataTypesCache().getType(data_type_name));
                }
            }

            structure_state->flattened_indexes_type = getSmallestIndexesType(num_types + 1); /// +1 for NULL index.
        }
        else
        {
            if (structure_state->structure_version.value == SerializationVersion::V1)
            {
                /// Skip max_dynamic_types parameter in V1 serialization version.
                size_t max_dynamic_types = 0;
                readVarUInt(max_dynamic_types, *structure_stream);
            }
            /// Read information about variants.
            DataTypes variants;
            readVarUInt(structure_state->num_dynamic_types, *structure_stream);
            variants.reserve(structure_state->num_dynamic_types + 1); /// +1 for shared variant.
            const bool binary_type_encoding = (settings.native_format && settings.format_settings && settings.format_settings->native.decode_types_in_binary_format)
                || structure_state->structure_version.value == SerializationVersion::V3
                || structure_state->structure_version.value == SerializationVersion::NARROWED;
            if (binary_type_encoding)
            {
                for (size_t i = 0; i != structure_state->num_dynamic_types; ++i)
                    variants.push_back(decodeDataType(*structure_stream));
            }
            else
            {
                String data_type_name;
                for (size_t i = 0; i != structure_state->num_dynamic_types; ++i)
                {
                    readStringBinary(data_type_name, *structure_stream);
                    variants.push_back(getDataTypesCache().getType(data_type_name));
                }
            }
            /// Add shared variant, Dynamic column should always have it.
            variants.push_back(ColumnDynamic::getSharedVariantDataType());
            auto variant_type = std::make_shared<DataTypeVariant>(variants);

            /// Read statistics.
            if (settings.object_and_dynamic_read_statistics)
            {
                bool has_statistics = true;
                /// In V3 and NARROWED we have an additional flag that indicates if we have statistics or not.
                if (structure_state->structure_version.value == SerializationVersion::V3
                    || structure_state->structure_version.value == SerializationVersion::NARROWED)
                    readBinary(has_statistics, *structure_stream);
                if (has_statistics)
                {
                    ColumnDynamic::Statistics statistics;
                    /// In Compact parts the statistics block is written by the prefix and reflects only the
                    /// first granule of the part. In Wide parts the structure stream's statistics block is
                    /// produced by the suffix writer and accumulated over the whole part, so it is full-part
                    /// accurate. Optimizations like Dynamic narrowing must skip the Compact case.
                    statistics.reliable = settings.data_part_type != MergeTreeDataPartType::Compact;

                    /// First, read statistics for usual variants.
                    for (const auto & variant : variant_type->getVariants())
                        readVarUInt(statistics.variants_statistics[variant->getName()], *structure_stream);

                    /// Second, read statistics for shared variants.
                    size_t statistics_size = 0;
                    readVarUInt(statistics_size, *structure_stream);
                    String variant_name;
                    for (size_t i = 0; i != statistics_size; ++i)
                    {
                        readStringBinary(variant_name, *structure_stream);
                        readVarUInt(statistics.shared_variants_statistics[variant_name], *structure_stream);
                    }

                    structure_state->statistics = std::make_shared<const ColumnDynamic::Statistics>(std::move(statistics));
                }
            }

            if (structure_state->structure_version.value == SerializationVersion::NARROWED)
            {
                /// Trailer for NARROWED: <encoded narrowed_type> <bool stored_as_nullable>.
                structure_state->narrowed_type = decodeDataType(*structure_stream);
                readBinary(structure_state->narrowed_stored_as_nullable, *structure_stream);

                /// Recover an approximate `null_count` for downstream consumers (e.g., later
                /// narrowing decisions during merge). We don't know the exact count from disk,
                /// but `narrowed_stored_as_nullable == true` proves NULLs may exist (writer wraps
                /// in Nullable only when null_count > 0 or unknown). Mark as nullopt to force
                /// downstream merges to wrap in Nullable instead of assuming "no NULLs".
                if (structure_state->statistics && structure_state->narrowed_stored_as_nullable)
                {
                    auto stats_copy = std::make_shared<ColumnDynamic::Statistics>(*structure_state->statistics);
                    stats_copy->null_count.reset();
                    structure_state->statistics = std::move(stats_copy);
                }
                else if (structure_state->statistics)
                {
                    /// `stored_as_nullable == false` guarantees the part has no NULLs.
                    auto stats_copy = std::make_shared<ColumnDynamic::Statistics>(*structure_state->statistics);
                    stats_copy->null_count = 0;
                    structure_state->statistics = std::move(stats_copy);
                }
            }
            else if (structure_state->statistics)
            {
                /// V1/V2/V3 parts don't track `null_count` on disk; readers cannot infer it.
                /// Mark as `nullopt` so downstream merges treat it as "unknown" and conservatively
                /// wrap any v4 narrowing result in `Nullable`.
                auto stats_copy = std::make_shared<ColumnDynamic::Statistics>(*structure_state->statistics);
                stats_copy->null_count.reset();
                structure_state->statistics = std::move(stats_copy);
            }

            structure_state->variant_type = std::move(variant_type);
        }

        state = structure_state;
        addToSubstreamsDeserializeStatesCache(cache, settings.path, state);

        /// We won't read from this stream anymore so we can release it.
        if (settings.release_stream_callback)
            settings.release_stream_callback(settings.path);
    }

    settings.path.pop_back();
    return state;
}

void SerializationDynamic::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    auto * dynamic_state = checkAndGetState<SerializeBinaryBulkStateDynamic>(state);

    if (dynamic_state->structure_version.value == SerializationVersion::FLATTENED)
    {
        /// Write suffix for indexes and all flattened types.
        settings.path.push_back(Substream::DynamicData);
        auto indexes_serialization = dynamic_state->flattened_column->indexes_type->getDefaultSerialization();
        indexes_serialization->serializeBinaryBulkStateSuffix(settings, dynamic_state->flattened_indexes_state);
        for (size_t i = 0; i != dynamic_state->flattened_column->types.size(); ++i)
        {
            auto serialization = dynamic_state->flattened_column->types[i]->getDefaultSerialization();
            serialization->serializeBinaryBulkStateSuffix(settings, dynamic_state->flattened_states[i]);
        }
        settings.path.pop_back();
        return;
    }

    /// Write statistics in suffix if needed.
    if (settings.write_statistics == SerializeBinaryBulkSettings::StatisticsMode::SUFFIX)
    {
        settings.path.push_back(Substream::DynamicStructure);
        auto * stream = settings.getter(settings.path);
        settings.path.pop_back();

        if (!stream)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing stream for Dynamic column structure during serialization of binary bulk state suffix");

        /// In V3 and NARROWED we have an additional flag that indicates if we have statistics or not.
        /// It is needed to be able to write empty statistics if needed.
        if (dynamic_state->structure_version.value == SerializationVersion::V3
            || dynamic_state->structure_version.value == SerializationVersion::NARROWED)
            writeBinary(true, *stream);

        /// First, write statistics for usual variants.
        for (const auto & variant_name : dynamic_state->variant_names)
            writeVarUInt(dynamic_state->statistics.variants_statistics[variant_name], *stream);
        /// Second, write statistics for shared variants.
        writeVarUInt(dynamic_state->statistics.shared_variants_statistics.size(), *stream);
        for (const auto & [variant_name, size] : dynamic_state->statistics.shared_variants_statistics)
        {
            writeStringBinary(variant_name, *stream);
            writeVarUInt(size, *stream);
        }

        /// For NARROWED parts the structure stream's suffix must also re-emit the trailer
        /// (encoded narrowed_type + nullable flag) so the read-side parser, which slurps the
        /// whole structure stream upfront, finds the trailer after the statistics block in Wide
        /// parts. In Compact parts the trailer was emitted in the prefix and the suffix only
        /// adds an empty statistics block — but here `write_statistics == SUFFIX` is Wide-only.
        if (dynamic_state->structure_version.value == SerializationVersion::NARROWED)
        {
            chassert(dynamic_state->narrowed_type);
            encodeDataType(dynamic_state->narrowed_type, *stream);
            writeBinary(dynamic_state->narrowed_stored_as_nullable, *stream);
        }
    }

    settings.path.push_back(Substream::DynamicData);
    if (dynamic_state->structure_version.value == SerializationVersion::NARROWED)
        dynamic_state->narrowed_serialization->serializeBinaryBulkStateSuffix(settings, dynamic_state->narrowed_state);
    else
        dynamic_state->variant_serialization->serializeBinaryBulkStateSuffix(settings, dynamic_state->variant_state);
    settings.path.pop_back();
}

void SerializationDynamic::serializeBinaryBulkWithMultipleStreams(
    const DB::IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    size_t tmp_size = 0;
    serializeBinaryBulkWithMultipleStreamsAndCountTotalSizeOfVariants(column, offset, limit, settings, state, tmp_size);
}

void SerializationDynamic::serializeBinaryBulkWithMultipleStreamsAndCountTotalSizeOfVariants(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state,
    size_t & total_size_of_variants) const
{
    const auto & column_dynamic = assert_cast<const ColumnDynamic &>(column);
    auto * dynamic_state = checkAndGetState<SerializeBinaryBulkStateDynamic>(state);
    if (dynamic_state->structure_version.value == SerializationVersion::FLATTENED)
    {
        if (offset != 0 || (limit != 0 && limit != column.size()))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Flattened serialization for Dynamic column is supported only when the whole column is serialized. Provided offset and limit: {}/{}", offset, limit);

        settings.path.push_back(Substream::DynamicData);
        /// First, write indexes.
        dynamic_state->flattened_column->indexes_type->getDefaultSerialization()->serializeBinaryBulkWithMultipleStreams(*dynamic_state->flattened_column->indexes_column, 0, 0, settings, dynamic_state->flattened_indexes_state);
        /// Second, write all data of flattened types in corresponding order.
        for (size_t i = 0; i != dynamic_state->flattened_column->types.size(); ++i)
        {
            auto serialization = dynamic_state->flattened_column->types[i]->getDefaultSerialization();
            serialization->serializeBinaryBulkWithMultipleStreams(*dynamic_state->flattened_column->columns[i], 0, 0, settings, dynamic_state->flattened_states[i]);
        }
        settings.path.pop_back();
        return;
    }

    const auto & variant_info = column_dynamic.getVariantInfo();
    const auto * variant_column = &column_dynamic.getVariantColumn();

    if (!variant_info.variant_type->equals(*dynamic_state->variant_type))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mismatch of internal columns of Dynamic. Expected: {}, Got: {}", dynamic_state->variant_type->getName(), variant_info.variant_type->getName());

    if (dynamic_state->structure_version.value == SerializationVersion::NARROWED)
    {
        /// Materialize the requested slice as the on-disk storage column and write it.
        auto storage_column = buildNarrowedColumn(column_dynamic, dynamic_state->narrowed_type, dynamic_state->narrowed_stored_as_nullable, offset, limit);

        settings.path.push_back(Substream::DynamicData);
        dynamic_state->narrowed_serialization->serializeBinaryBulkWithMultipleStreams(*storage_column, 0, 0, settings, dynamic_state->narrowed_state);
        settings.path.pop_back();

        /// Count the narrowed-variant rows in the slice. Used both to update suffix-time statistics
        /// and to report a non-zero size to the caller — `SerializationObject` uses this number as
        /// the path's `dynamic_paths_statistics`, which drives later `chooseDynamicStructureForMerge`
        /// decisions. Reporting 0 would silently demote narrowed dynamic paths to shared data after
        /// the next merge.
        const auto narrowed_global_discr = variant_info.variant_name_to_discriminator.at(dynamic_state->narrowed_type->getName());
        const auto narrowed_local_discr = variant_column->localDiscriminatorByGlobal(narrowed_global_discr);
        const auto & local_discriminators = variant_column->getLocalDiscriminators();
        const size_t end = (limit == 0 || offset + limit > local_discriminators.size()) ? local_discriminators.size() : offset + limit;
        size_t narrowed_rows_in_slice = 0;
        for (size_t i = offset; i != end; ++i)
            if (local_discriminators[i] == narrowed_local_discr)
                ++narrowed_rows_in_slice;
        total_size_of_variants += narrowed_rows_in_slice;

        if (dynamic_state->recalculate_statistics)
        {
            dynamic_state->statistics.variants_statistics[dynamic_state->narrowed_type->getName()] += narrowed_rows_in_slice;
            /// Make sure every variant has an entry, even if zero, so the writer can serialize them.
            for (const auto & vname : variant_info.variant_names)
                dynamic_state->statistics.variants_statistics.try_emplace(vname, 0);
            /// Track NULL rows we materialized through `buildNarrowedColumn` so that downstream
            /// consumers of these suffix-time stats see an accurate `null_count`.
            const size_t null_rows_in_slice = (end - offset) - narrowed_rows_in_slice;
            if (!dynamic_state->statistics.null_count)
                dynamic_state->statistics.null_count = 0;
            dynamic_state->statistics.null_count = *dynamic_state->statistics.null_count + null_rows_in_slice;
        }
        return;
    }

    settings.path.push_back(Substream::DynamicData);
    assert_cast<const SerializationVariant &>(*dynamic_state->variant_serialization)
        .serializeBinaryBulkWithMultipleStreamsAndUpdateVariantStatistics(
            *variant_column,
            offset,
            limit,
            settings,
            dynamic_state->variant_state,
            dynamic_state->statistics.variants_statistics,
            total_size_of_variants);

    if (dynamic_state->recalculate_statistics)
    {
        /// Calculate statistics for shared variants.
        const auto & shared_variant = column_dynamic.getSharedVariant();
        if (!shared_variant.empty())
        {
            const auto & local_discriminators = variant_column->getLocalDiscriminators();
            const auto & offsets = variant_column->getOffsets();
            const auto shared_variant_discr = variant_column->localDiscriminatorByGlobal(column_dynamic.getSharedVariantDiscriminator());
            size_t end = limit == 0 || offset + limit > local_discriminators.size() ? local_discriminators.size() : offset + limit;
            for (size_t i = offset; i != end; ++i)
            {
                if (local_discriminators[i] == shared_variant_discr)
                {
                    auto value = shared_variant.getDataAt(offsets[i]);
                    ReadBufferFromMemory buf(value);
                    auto type = decodeDataType(buf);
                    auto type_name = type->getName();
                    if (auto it = dynamic_state->statistics.shared_variants_statistics.find(type_name); it != dynamic_state->statistics.shared_variants_statistics.end())
                        ++it->second;
                    else if (dynamic_state->statistics.shared_variants_statistics.size() < ColumnDynamic::Statistics::MAX_SHARED_VARIANT_STATISTICS_SIZE)
                        dynamic_state->statistics.shared_variants_statistics.emplace(type_name, 1);
                }
            }
        }
    }
    settings.path.pop_back();
}

void SerializationDynamic::deserializeBinaryBulkWithMultipleStreams(
    DB::ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    if (!state)
        return;

    auto mutable_column = column->assumeMutable();
    auto & column_dynamic = assert_cast<ColumnDynamic &>(*mutable_column);
    auto * dynamic_state = checkAndGetState<DeserializeBinaryBulkStateDynamic>(state);
    auto * structure_state = checkAndGetState<DeserializeBinaryBulkStateDynamicStructure>(dynamic_state->structure_state);

    if (structure_state->structure_version.value == SerializationVersion::FLATTENED)
    {
        settings.path.push_back(Substream::DynamicData);

        FlattenedDynamicColumn flattened_column;
        flattened_column.types = structure_state->flattened_data_types;
        flattened_column.indexes_type = structure_state->flattened_indexes_type;
        flattened_column.indexes_column = flattened_column.indexes_type->createColumn();
        /// First, read indexes.
        auto indexes_serialization = flattened_column.indexes_type->getDefaultSerialization();
        indexes_serialization->deserializeBinaryBulkWithMultipleStreams(flattened_column.indexes_column, 0, limit, settings, dynamic_state->flattened_indexes_state, cache);
        /// Second, read data of all flattened types in corresponding order.
        auto flattened_limits = getLimitsForFlattenedDynamicColumn(*flattened_column.indexes_column, flattened_column.types.size());
        flattened_column.columns.reserve(flattened_column.types.size());
        for (size_t i = 0; i != flattened_column.types.size(); ++i)
        {
            ColumnPtr type_column = flattened_column.types[i]->createColumn();
            flattened_column.types[i]->getDefaultSerialization()->deserializeBinaryBulkWithMultipleStreams(type_column, 0, flattened_limits[i], settings, dynamic_state->flattened_states[i], cache);
            flattened_column.columns.emplace_back(std::move(type_column));
        }

        settings.path.pop_back();

        unflattenDynamicColumn(std::move(flattened_column), column_dynamic);
        column = std::move(mutable_column);
        return;
    }

    if (mutable_column->empty())
    {
        column_dynamic.setMaxDynamicPaths(structure_state->num_dynamic_types);
        column_dynamic.setVariantType(structure_state->variant_type);
        column_dynamic.setStatistics(structure_state->statistics);
    }

    const auto & variant_info = column_dynamic.getVariantInfo();
    if (!variant_info.variant_type->equals(*structure_state->variant_type))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mismatch of internal columns of Dynamic. Expected: {}, Got: {}", structure_state->variant_type->getName(), variant_info.variant_type->getName());

    if (structure_state->structure_version.value == SerializationVersion::NARROWED)
    {
        /// Read the on-disk storage column and expand it back into the in-memory ColumnVariant.
        auto storage_type = structure_state->narrowed_stored_as_nullable
            ? makeNullable(structure_state->narrowed_type)
            : structure_state->narrowed_type;
        ColumnPtr storage_column = storage_type->createColumn();

        settings.path.push_back(Substream::DynamicData);
        dynamic_state->narrowed_serialization->deserializeBinaryBulkWithMultipleStreams(storage_column, rows_offset, limit, settings, dynamic_state->narrowed_state, cache);
        settings.path.pop_back();

        auto & target_variant_column = assert_cast<ColumnVariant &>(column_dynamic.getVariantColumn());
        const auto narrowed_global_discr = variant_info.variant_name_to_discriminator.at(structure_state->narrowed_type->getName());
        const auto narrowed_local_discr = target_variant_column.localDiscriminatorByGlobal(narrowed_global_discr);
        auto & narrowed_variant_data = target_variant_column.getVariantByGlobalDiscriminator(narrowed_global_discr);
        auto & local_discriminators = target_variant_column.getLocalDiscriminators();
        auto & offsets = target_variant_column.getOffsets();

        const size_t added = storage_column->size();
        local_discriminators.reserve(local_discriminators.size() + added);
        offsets.reserve(offsets.size() + added);

        if (structure_state->narrowed_stored_as_nullable)
        {
            const auto & nullable = assert_cast<const ColumnNullable &>(*storage_column);
            const auto & nested = nullable.getNestedColumn();
            const auto & null_map = nullable.getNullMapData();
            for (size_t i = 0; i != added; ++i)
            {
                if (null_map[i])
                {
                    local_discriminators.push_back(ColumnVariant::NULL_DISCRIMINATOR);
                    offsets.push_back(0);
                }
                else
                {
                    narrowed_variant_data.insertFrom(nested, i);
                    local_discriminators.push_back(narrowed_local_discr);
                    offsets.push_back(narrowed_variant_data.size() - 1);
                }
            }
        }
        else
        {
            for (size_t i = 0; i != added; ++i)
            {
                narrowed_variant_data.insertFrom(*storage_column, i);
                local_discriminators.push_back(narrowed_local_discr);
                offsets.push_back(narrowed_variant_data.size() - 1);
            }
        }

        column = std::move(mutable_column);
        return;
    }

    settings.path.push_back(Substream::DynamicData);
    dynamic_state->variant_serialization->deserializeBinaryBulkWithMultipleStreams(column_dynamic.getVariantColumnPtr(), rows_offset, limit, settings, dynamic_state->variant_state, cache);
    settings.path.pop_back();

    column = std::move(mutable_column);
}

void SerializationDynamic::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    /// Serialize NULL as Nothing type with no value.
    if (field.isNull())
    {
        encodeDataType(std::make_shared<DataTypeNothing>(), ostr);
        return;
    }

    auto field_type = applyVisitor(FieldToDataType(), field);
    encodeDataType(field_type, ostr);
    field_type->getDefaultSerialization()->serializeBinary(field, ostr, settings);
}

void SerializationDynamic::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto field_type = decodeDataType(istr);
    if (isNothing(field_type))
    {
        field = Null();
        return;
    }

    field_type->getDefaultSerialization()->deserializeBinary(field, istr, settings);
}

void SerializationDynamic::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeBinary(assert_cast<const ColumnDynamic &>(column), row_num, ostr, settings);
}

void SerializationDynamic::serializeBinary(const ColumnDynamic & dynamic_column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & variant_info = dynamic_column.getVariantInfo();
    const auto & variant_column = dynamic_column.getVariantColumn();
    auto global_discr = variant_column.globalDiscriminatorAt(row_num);

    /// Serialize NULL as Nothing type with no value.
    if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
    {
        encodeDataType(std::make_shared<DataTypeNothing>(), ostr);
        return;
    }
    /// Check if this value is in shared variant. In this case it's already
    /// in desired binary format.
    if (global_discr == dynamic_column.getSharedVariantDiscriminator())
    {
        auto value = dynamic_column.getSharedVariant().getDataAt(variant_column.offsetAt(row_num));
        ostr.write(value.data(), value.size());
        return;
    }

    const auto & variant_type = assert_cast<const DataTypeVariant &>(*variant_info.variant_type).getVariant(global_discr);
    const auto & variant_type_name = variant_info.variant_names[global_discr];
    encodeDataType(variant_type, ostr);
    getDataTypesCache().getSerialization(variant_type_name)->serializeBinary(variant_column.getVariantByGlobalDiscriminator(global_discr), variant_column.offsetAt(row_num), ostr, settings);
}

void SerializationDynamic::serializeForHashCalculation(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const auto & dynamic_column = assert_cast<const ColumnDynamic &>(column);
    const auto & variant_info = dynamic_column.getVariantInfo();
    const auto & variant_column = dynamic_column.getVariantColumn();
    auto global_discr = variant_column.globalDiscriminatorAt(row_num);

    /// Serialize NULL as Nothing type with no value.
    if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
    {
        writeStringBinary("Nothing", ostr);
        return;
    }

    /// Check if this value is in shared variant.
    if (global_discr == dynamic_column.getSharedVariantDiscriminator())
    {
        auto value = dynamic_column.getSharedVariant().getDataAt(variant_column.offsetAt(row_num));
        ReadBufferFromMemory value_buf(value);
        auto type = decodeDataType(value_buf);
        auto type_name = type->getName();
        auto serialization = getDataTypesCache().getSerialization(type_name);
        auto tmp_column = type->createColumn();
        serialization->deserializeBinary(*tmp_column, value_buf, {});
        serializeVariantForHashCalculation(*tmp_column, serialization, type, 0, ostr);
        return;
    }

    const auto & variant_type_name = variant_info.variant_names[global_discr];
    const auto & variant_type = assert_cast<const DataTypeVariant &>(*variant_info.variant_type).getVariant(global_discr);
    serializeVariantForHashCalculation(
        variant_column.getVariantByGlobalDiscriminator(global_discr),
        getDataTypesCache().getSerialization(variant_type_name),
        variant_type,
        variant_column.offsetAt(row_num),
        ostr);
}

void SerializationDynamic::serializeVariantForHashCalculation(const IColumn & column, const SerializationPtr & serialization, const DataTypePtr & type, size_t row_num, WriteBuffer & ostr)
{
    /// For hash calculation we serialize value type name and then the value.
    encodeDataTypeForHashCalculation(type, ostr);
    serialization->serializeForHashCalculation(column, row_num, ostr);
}

template <typename ReturnType = void, typename DeserializeFunc>
static ReturnType deserializeVariant(
    ColumnVariant & variant_column,
    const SerializationPtr & variant_serialization,
    ColumnVariant::Discriminator global_discr,
    ReadBuffer & istr,
    DeserializeFunc deserialize)
{
    auto & variant = variant_column.getVariantByGlobalDiscriminator(global_discr);
    if constexpr (std::is_same_v<ReturnType, bool>)
    {
        if (!deserialize(*variant_serialization, variant, istr))
            return ReturnType(false);
    }
    else
    {
        deserialize(*variant_serialization, variant, istr);
    }
    variant_column.getLocalDiscriminators().push_back(variant_column.localDiscriminatorByGlobal(global_discr));
    variant_column.getOffsets().push_back(variant.size() - 1);
    return ReturnType(true);
}

void SerializationDynamic::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeBinary(assert_cast<ColumnDynamic &>(column), istr, settings);
}

void SerializationDynamic::deserializeBinary(ColumnDynamic & dynamic_column, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto variant_type = decodeDataType(istr);
    if (isNothing(variant_type))
    {
        dynamic_column.insertDefault();
        return;
    }

    auto variant_type_name = variant_type->getName();
    const auto & variant_serialization = dynamic_column.getVariantSerialization(variant_type, variant_type_name);
    const auto & variant_info = dynamic_column.getVariantInfo();
    auto it = variant_info.variant_name_to_discriminator.find(variant_type_name);
    if (it != variant_info.variant_name_to_discriminator.end())
    {
        deserializeVariant(dynamic_column.getVariantColumn(), variant_serialization, it->second, istr, [&settings](const ISerialization & serialization, IColumn & variant, ReadBuffer & buf){ serialization.deserializeBinary(variant, buf, settings); });
        return;
    }

    /// We don't have this variant yet. Let's try to add it.
    if (dynamic_column.addNewVariant(variant_type))
    {
        auto discr = variant_info.variant_name_to_discriminator.at(variant_type_name);
        deserializeVariant(dynamic_column.getVariantColumn(), variant_serialization, discr, istr, [&settings](const ISerialization & serialization, IColumn & variant, ReadBuffer & buf){ serialization.deserializeBinary(variant, buf, settings); });
        return;
    }

    /// We reached maximum number of variants and couldn't add new variant.
    /// In this case we insert this value into shared variant in binary form.
    auto tmp_variant_column = variant_type->createColumn();
    variant_serialization->deserializeBinary(*tmp_variant_column, istr, settings);
    dynamic_column.insertValueIntoSharedVariant(*tmp_variant_column, variant_type, variant_type_name, 0);
}

template <typename ReadFieldFunc, typename TryDeserializeVariantFunc, typename DeserializeVariant>
static void deserializeTextImpl(
    IColumn & column,
    ReadBuffer & istr,
    const FormatSettings & settings,
    ReadFieldFunc read_field,
    FormatSettings::EscapingRule escaping_rule,
    TryDeserializeVariantFunc try_deserialize_variant,
    DeserializeVariant deserialize_variant)
{
    auto & dynamic_column = assert_cast<ColumnDynamic &>(column);
    auto & variant_column = dynamic_column.getVariantColumn();
    const auto & variant_info = dynamic_column.getVariantInfo();
    const auto & variant_types = assert_cast<const DataTypeVariant &>(*variant_info.variant_type).getVariants();
    String field = read_field(istr);
    JSONInferenceInfo json_info;
    auto variant_type = tryInferDataTypeByEscapingRule(field, settings, escaping_rule, &json_info);
    if (escaping_rule == FormatSettings::EscapingRule::JSON)
        transformFinalInferredJSONTypeIfNeeded(variant_type, settings, &json_info);

    /// If inferred type is not complete, we cannot add it as a new variant.
    /// Let's try to deserialize this field into existing variants.
    /// If failed, insert this value as String.
    if (!checkIfTypeIsComplete(variant_type))
    {
        size_t shared_variant_discr = dynamic_column.getSharedVariantDiscriminator();
        for (ColumnVariant::Discriminator i = 0; i != variant_types.size(); ++i)
        {
            auto field_buf = std::make_unique<ReadBufferFromString>(field);
            if (i != shared_variant_discr
                && deserializeVariant<bool>(
                    variant_column,
                    dynamic_column.getVariantSerialization(variant_types[i], variant_info.variant_names[i]),
                    i,
                    *field_buf,
                    try_deserialize_variant))
                return;
        }

        /// We cannot insert value with incomplete type, insert it as String.
        variant_type = std::make_shared<DataTypeString>();
        /// To be able to deserialize field as String with Quoted escaping rule, it should be quoted.
        /// Use `writeQuotedString` so inner single quotes and backslashes are escaped properly;
        /// naive concatenation `"'" + field + "'"` would terminate prematurely at the first inner
        /// single quote, truncating the stored value (issue #105441).
        if (escaping_rule == FormatSettings::EscapingRule::Quoted && (field.size() < 2 || field.front() != '\'' || field.back() != '\''))
        {
            WriteBufferFromOwnString quoted;
            writeQuotedString(field, quoted);
            field = std::move(quoted.str());
        }
    }

    if (dynamic_column.addNewVariant(variant_type, variant_type->getName()))
    {
        auto field_buf = std::make_unique<ReadBufferFromString>(field);
        auto discr = variant_info.variant_name_to_discriminator.at(variant_type->getName());
        deserializeVariant(dynamic_column.getVariantColumn(), dynamic_column.getVariantSerialization(variant_type), discr, *field_buf, deserialize_variant);
        return;
    }

    /// We couldn't add new variant. Insert it into shared variant.
    auto tmp_variant_column = variant_type->createColumn();
    auto field_buf = std::make_unique<ReadBufferFromString>(field);
    auto variant_type_name = variant_type->getName();
    deserialize_variant(*dynamic_column.getVariantSerialization(variant_type, variant_type_name), *tmp_variant_column, *field_buf);
    dynamic_column.insertValueIntoSharedVariant(*tmp_variant_column, variant_type, variant_type_name, 0);
}

template <typename NestedSerialize>
static void serializeTextImpl(
    const IColumn & column,
    size_t row_num,
    WriteBuffer & ostr,
    NestedSerialize nested_serialize)
{
    const auto & dynamic_column = assert_cast<const ColumnDynamic &>(column);
    const auto & variant_column = dynamic_column.getVariantColumn();
    /// Check if this row has value in shared variant. In this case we should first deserialize it from binary format.
    if (variant_column.globalDiscriminatorAt(row_num) == dynamic_column.getSharedVariantDiscriminator())
    {
        auto value = dynamic_column.getSharedVariant().getDataAt(variant_column.offsetAt(row_num));
        ReadBufferFromMemory buf(value);
        auto variant_type = decodeDataType(buf);
        auto tmp_variant_column = variant_type->createColumn();
        auto variant_serialization = variant_type->getDefaultSerialization();
        variant_serialization->deserializeBinary(*tmp_variant_column, buf, FormatSettings{});
        nested_serialize(*variant_serialization, *tmp_variant_column, 0, ostr);
    }
    /// Otherwise just use serialization for Variant.
    else
    {
        nested_serialize(*dynamic_column.getVariantInfo().variant_type->getDefaultSerialization(), variant_column, row_num, ostr);
    }
}

SerializationPtr SerializationDynamic::create(size_t max_dynamic_types_, const SerializationInfoSettings & serialization_info_settings_)
{
    return ISerialization::pooled(getHash(max_dynamic_types_, serialization_info_settings_), [=] { return new SerializationDynamic(max_dynamic_types_, serialization_info_settings_); });
}

void SerializationDynamic::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto nested_serialize = [&settings](const ISerialization & serialization, const IColumn & col, size_t row, WriteBuffer & buf)
    {
        serialization.serializeTextCSV(col, row, buf, settings);
    };

    serializeTextImpl(column, row_num, ostr, nested_serialize);
}

void SerializationDynamic::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto read_field = [&settings](ReadBuffer & buf)
    {
        String field;
        readCSVField(field, buf, settings.csv);
        return field;
    };

    auto try_deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        return serialization.tryDeserializeTextCSV(col, buf, settings);
    };

    auto deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        serialization.deserializeTextCSV(col, buf, settings);
    };

    deserializeTextImpl(column, istr, settings, read_field, FormatSettings::EscapingRule::CSV, try_deserialize_variant, deserialize_variant);
}

bool SerializationDynamic::tryDeserializeTextCSV(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings) const
{
    deserializeTextCSV(column, istr, settings);
    return true;
}

void SerializationDynamic::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto nested_serialize = [&settings](const ISerialization & serialization, const IColumn & col, size_t row, WriteBuffer & buf)
    {
        serialization.serializeTextEscaped(col, row, buf, settings);
    };

    serializeTextImpl(column, row_num, ostr, nested_serialize);
}

void SerializationDynamic::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto read_field = [](ReadBuffer & buf)
    {
        String field;
        readEscapedString(field, buf);
        return field;
    };

    auto try_deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        return serialization.tryDeserializeTextEscaped(col, buf, settings);
    };

    auto deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        serialization.deserializeTextEscaped(col, buf, settings);
    };

    deserializeTextImpl(column, istr, settings, read_field, FormatSettings::EscapingRule::Escaped, try_deserialize_variant, deserialize_variant);
}

bool SerializationDynamic::tryDeserializeTextEscaped(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings) const
{
    deserializeTextEscaped(column, istr, settings);
    return true;
}

void SerializationDynamic::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto nested_serialize = [&settings](const ISerialization & serialization, const IColumn & col, size_t row, WriteBuffer & buf)
    {
        serialization.serializeTextQuoted(col, row, buf, settings);
    };

    serializeTextImpl(column, row_num, ostr, nested_serialize);
}

void SerializationDynamic::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto read_field = [](ReadBuffer & buf)
    {
        String field;
        readQuotedField(field, buf);
        return field;
    };

    auto try_deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        return serialization.tryDeserializeTextQuoted(col, buf, settings);
    };

    auto deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        serialization.deserializeTextQuoted(col, buf, settings);
    };

    deserializeTextImpl(column, istr, settings, read_field, FormatSettings::EscapingRule::Quoted, try_deserialize_variant, deserialize_variant);
}

bool SerializationDynamic::tryDeserializeTextQuoted(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings) const
{
    deserializeTextQuoted(column, istr, settings);
    return true;
}

void SerializationDynamic::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto nested_serialize = [&settings](const ISerialization & serialization, const IColumn & col, size_t row, WriteBuffer & buf)
    {
        serialization.serializeTextJSON(col, row, buf, settings);
    };

    serializeTextImpl(column, row_num, ostr, nested_serialize);
}

void SerializationDynamic::serializeTextJSONPretty(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const
{
    auto nested_serialize = [&settings, indent](const ISerialization & serialization, const IColumn & col, size_t row, WriteBuffer & buf)
    {
        serialization.serializeTextJSONPretty(col, row, buf, settings, indent);
    };

    serializeTextImpl(column, row_num, ostr, nested_serialize);
}

void SerializationDynamic::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto read_field = [&settings](ReadBuffer & buf)
    {
        String field;
        readJSONField(field, buf, settings.json);
        return field;
    };

    auto try_deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        return serialization.tryDeserializeTextJSON(col, buf, settings);
    };

    auto deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        serialization.deserializeTextJSON(col, buf, settings);
    };

    deserializeTextImpl(column, istr, settings, read_field, FormatSettings::EscapingRule::JSON, try_deserialize_variant, deserialize_variant);
}

bool SerializationDynamic::tryDeserializeTextJSON(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings) const
{
    deserializeTextJSON(column, istr, settings);
    return true;
}

void SerializationDynamic::serializeTextRaw(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto nested_serialize = [&settings](const ISerialization & serialization, const IColumn & col, size_t row, WriteBuffer & buf)
    {
        serialization.serializeTextRaw(col, row, buf, settings);
    };

    serializeTextImpl(column, row_num, ostr, nested_serialize);
}

void SerializationDynamic::deserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto read_field = [](ReadBuffer & buf)
    {
        String field;
        readString(field, buf);
        return field;
    };

    auto try_deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        return serialization.tryDeserializeTextRaw(col, buf, settings);
    };

    auto deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        serialization.deserializeTextRaw(col, buf, settings);
    };

    deserializeTextImpl(column, istr, settings, read_field, FormatSettings::EscapingRule::Raw, try_deserialize_variant, deserialize_variant);
}

bool SerializationDynamic::tryDeserializeTextRaw(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings) const
{
    deserializeTextRaw(column, istr, settings);
    return true;
}

void SerializationDynamic::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto nested_serialize = [&settings](const ISerialization & serialization, const IColumn & col, size_t row, WriteBuffer & buf)
    {
        serialization.serializeText(col, row, buf, settings);
    };

    serializeTextImpl(column, row_num, ostr, nested_serialize);
}

void SerializationDynamic::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto read_field = [](ReadBuffer & buf)
    {
        String field;
        readStringUntilEOF(field, buf);
        return field;
    };

    auto try_deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        return serialization.tryDeserializeWholeText(col, buf, settings);
    };

    auto deserialize_variant = [&settings](const ISerialization & serialization, IColumn & col, ReadBuffer & buf)
    {
        serialization.deserializeWholeText(col, buf, settings);
    };

    deserializeTextImpl(column, istr, settings, read_field, FormatSettings::EscapingRule::Raw, try_deserialize_variant, deserialize_variant);
}

bool SerializationDynamic::tryDeserializeWholeText(DB::IColumn & column, DB::ReadBuffer & istr, const DB::FormatSettings & settings) const
{
    deserializeWholeText(column, istr, settings);
    return true;
}

void SerializationDynamic::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto nested_serialize = [&settings](const ISerialization & serialization, const IColumn & col, size_t row, WriteBuffer & buf)
    {
        serialization.serializeTextXML(col, row, buf, settings);
    };

    serializeTextImpl(column, row_num, ostr, nested_serialize);
}

SerializationPtr SerializationDynamic::createSerializationForType(const DataTypePtr & type) const
{
    return type->getSerialization(serialization_info_settings);
}

}
