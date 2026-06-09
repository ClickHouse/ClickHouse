#include <Common/SipHash.h>
#include <DataTypes/Serializations/SerializationDynamicElement.h>
#include <DataTypes/Serializations/SerializationVariantElement.h>
#include <DataTypes/Serializations/SerializationVariantElementNullMap.h>
#include <DataTypes/Serializations/SerializationDynamic.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NullableUtils.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

UInt128 SerializationDynamicElement::getHash(const SerializationPtr & nested_, const SerializationPtr & shared_variant_serialization_, const String & dynamic_element_name_, const String & nested_subcolumn_, bool is_null_map_subcolumn_, const SerializationInfoSettings & parent_serialization_info_settings_)
{
    SipHash hash;
    hash.update("DynamicElement");
    hash.update(nested_->getHash());
    hash.update(shared_variant_serialization_->getHash());
    hash.update(dynamic_element_name_.size());
    hash.update(dynamic_element_name_);
    hash.update(nested_subcolumn_.size());
    hash.update(nested_subcolumn_);
    hash.update(is_null_map_subcolumn_);
    parent_serialization_info_settings_.updateHash(hash);
    return hash.get128();
}

SerializationPtr SerializationDynamicElement::create(const SerializationPtr & nested_, const SerializationPtr & shared_variant_serialization_, const String & dynamic_element_name_, const String & nested_subcolumn_, bool is_null_map_subcolumn_, const SerializationInfoSettings & parent_serialization_info_settings_)
{
    if (!nested_->supportsPooling() || !shared_variant_serialization_->supportsPooling())
        return std::shared_ptr<ISerialization>(new SerializationDynamicElement(nested_, shared_variant_serialization_, dynamic_element_name_, nested_subcolumn_, is_null_map_subcolumn_, parent_serialization_info_settings_));
    return ISerialization::pooled(getHash(nested_, shared_variant_serialization_, dynamic_element_name_, nested_subcolumn_, is_null_map_subcolumn_, parent_serialization_info_settings_), [&] { return new SerializationDynamicElement(nested_, shared_variant_serialization_, dynamic_element_name_, nested_subcolumn_, is_null_map_subcolumn_, parent_serialization_info_settings_); });
}

struct DeserializeBinaryBulkStateDynamicElement : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr structure_state;
    SerializationPtr variant_serialization;
    ISerialization::DeserializeBinaryBulkStatePtr variant_element_state;
    bool read_from_shared_variant{};
    ColumnPtr shared_variant;
    size_t shared_variant_size = 0;

    /// For NARROWED parts.
    /// True when this element is read directly from the narrowed Nullable(T)/T storage column.
    bool read_from_narrowed = false;
    /// Storage serialization (`Nullable(narrowed_type)` or `narrowed_type`).
    SerializationPtr narrowed_serialization;
    ISerialization::DeserializeBinaryBulkStatePtr narrowed_state;
    /// `narrowed_stored_as_nullable` copied from structure state for convenience.
    bool narrowed_stored_as_nullable = false;
    /// Type of the narrowed variant (non-Nullable). Same as `structure_state->narrowed_type`.
    DataTypePtr narrowed_type;


    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateDynamicElement>(*this);
        new_state->structure_state = structure_state ? structure_state->clone() : nullptr;
        new_state->variant_element_state = variant_element_state ? variant_element_state->clone() : nullptr;
        new_state->narrowed_state = narrowed_state ? narrowed_state->clone() : nullptr;
        return new_state;
    }
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
    if (deserialize_state->read_from_narrowed)
    {
        /// The element is read from the storage column of a NARROWED part. Expose the storage's
        /// substreams (`Nullable(narrowed_type)` or `narrowed_type`).
        auto storage_type = deserialize_state->narrowed_stored_as_nullable
            ? makeNullable(deserialize_state->narrowed_type)
            : deserialize_state->narrowed_type;
        settings.path.push_back(Substream::DynamicData);
        auto storage_data = SubstreamData(deserialize_state->narrowed_serialization)
                                .withType(storage_type)
                                .withSerializationInfo(data.serialization_info)
                                .withDeserializeState(deserialize_state->narrowed_state);
        settings.path.back().data = storage_data;
        deserialize_state->narrowed_serialization->enumerateStreams(settings, callback, storage_data);
        settings.path.pop_back();
        return;
    }

    if (!deserialize_state->variant_serialization)
        return;

    settings.path.push_back(Substream::DynamicData);
    auto variant_data = SubstreamData(deserialize_state->variant_serialization)
                            .withType(deserialize_state->read_from_shared_variant ? ColumnDynamic::getSharedVariantDataType() : data.type)
                            .withColumn(deserialize_state->read_from_shared_variant ? ColumnDynamic::getSharedVariantDataType()->createColumn() : data.column)
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
    auto * structure_state_typed = checkAndGetState<SerializationDynamic::DeserializeBinaryBulkStateDynamicStructure>(dynamic_element_state->structure_state);

    /// NARROWED parts: the storage column is `Nullable(narrowed_type)` (or `narrowed_type`).
    /// - If the requested element matches `narrowed_type` exactly, read it directly from storage.
    /// - Otherwise this part has no values for the requested element; leave both serializations
    ///   unset, and `deserializeBinaryBulkWithMultipleStreams` will produce defaults/NULLs.
    if (structure_state_typed->structure_version.value == SerializationDynamic::SerializationVersion::NARROWED)
    {
        dynamic_element_state->narrowed_type = structure_state_typed->narrowed_type;
        dynamic_element_state->narrowed_stored_as_nullable = structure_state_typed->narrowed_stored_as_nullable;
        if (dynamic_element_name == structure_state_typed->narrowed_type->getName())
        {
            auto storage_type = structure_state_typed->narrowed_stored_as_nullable
                ? makeNullable(structure_state_typed->narrowed_type)
                : structure_state_typed->narrowed_type;
            /// Build the storage serialization with the parent `SerializationDynamic`'s settings so
            /// that the substream layout matches whatever the writer produced (e.g., non-default
            /// `string_serialization_version` or `nullable_serialization_version`).
            dynamic_element_state->narrowed_serialization = storage_type->getSerialization(parent_serialization_info_settings);
            dynamic_element_state->read_from_narrowed = true;
            settings.path.push_back(Substream::DynamicData);
            dynamic_element_state->narrowed_serialization->deserializeBinaryBulkStatePrefix(settings, dynamic_element_state->narrowed_state, cache);
            settings.path.pop_back();
        }
        state = std::move(dynamic_element_state);
        return;
    }

    const auto & variant_type = assert_cast<const DataTypeVariant &>(*structure_state_typed->variant_type);
    /// Check if we actually have required element in the Variant.
    if (auto global_discr = variant_type.tryGetVariantDiscriminator(dynamic_element_name))
    {
        settings.path.push_back(Substream::DynamicData);
        if (is_null_map_subcolumn)
            dynamic_element_state->variant_serialization = SerializationVariantElementNullMap::create(dynamic_element_name, *global_discr);
        else
            dynamic_element_state->variant_serialization = SerializationVariantElement::create(nested_serialization, dynamic_element_name, *global_discr);
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
        dynamic_element_state->variant_serialization = SerializationVariantElement::create(
            shared_variant_serialization,
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
    size_t rows_offset,
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

    /// NARROWED part: either read directly from the narrowed storage column, or — if the requested
    /// element doesn't match the narrowed type — produce a column of NULLs/defaults of length `limit`.
    if (dynamic_element_state->read_from_narrowed)
    {
        settings.path.push_back(Substream::DynamicData);

        /// Always read the full storage column (`Nullable(narrowed_type)` or `narrowed_type`).
        /// We then either pass it through, project the null map, or extract a nested subcolumn —
        /// matching whatever shape `result_column` expects.
        auto storage_type = dynamic_element_state->narrowed_stored_as_nullable
            ? makeNullable(dynamic_element_state->narrowed_type)
            : dynamic_element_state->narrowed_type;
        ColumnPtr storage_column = storage_type->createColumn();
        dynamic_element_state->narrowed_serialization->deserializeBinaryBulkWithMultipleStreams(
            storage_column, rows_offset, limit, settings, dynamic_element_state->narrowed_state, cache);
        settings.path.pop_back();
        const size_t added = storage_column->size();

        if (is_null_map_subcolumn)
        {
            /// Project storage column's null map (or all-zeros if non-nullable storage) into the
            /// result column, which is `UInt8`.
            auto mutable_column = result_column->assumeMutable();
            auto & null_map = assert_cast<ColumnUInt8 &>(*mutable_column).getData();
            const size_t prev_size = null_map.size();
            null_map.resize(prev_size + added);
            if (dynamic_element_state->narrowed_stored_as_nullable)
            {
                const auto & storage_null_map = assert_cast<const ColumnNullable &>(*storage_column).getNullMapData();
                for (size_t i = 0; i != added; ++i)
                    null_map[prev_size + i] = storage_null_map[i];
            }
            else
            {
                std::fill(null_map.begin() + prev_size, null_map.end(), static_cast<UInt8>(0));
            }
            return;
        }

        if (!nested_subcolumn.empty())
        {
            /// Extract the requested subcolumn from the storage column. When storage is
            /// `Nullable(T)`, `Nullable.getSubcolumn(name)` returns a `Nullable(subcolumn_type)`
            /// even if the subcolumn itself cannot be inside `Nullable` (the framework adds
            /// `NullableSubcolumnCreator`). The result_column on the other hand may be the bare
            /// subcolumn type — `DataTypeDynamic::getDynamicSubcolumnData` only wraps in `Nullable`
            /// when `canExtractedSubcolumnsBeInsideNullable` is true (e.g., never for `Tuple` under
            /// the default profile). To avoid shape mismatch we unwrap to the non-nullable storage
            /// before extracting, and project the storage's null map separately if it would have
            /// influenced the result. For the cases that actually matter (sub-subcolumn reads),
            /// the inner subcolumn just gives the raw value; NULL rows are still represented as
            /// defaults of the inner type, which matches the behavior of the equivalent V3 read
            /// path that goes through `SerializationVariantElement`.
            ColumnPtr base_storage = storage_column;
            DataTypePtr base_type = storage_type;
            if (dynamic_element_state->narrowed_stored_as_nullable)
            {
                base_storage = assert_cast<const ColumnNullable &>(*storage_column).getNestedColumnPtr();
                base_type = dynamic_element_state->narrowed_type;
            }
            auto subcolumn = base_type->getSubcolumn(nested_subcolumn, base_storage);
            result_column->assumeMutable()->insertRangeFrom(*subcolumn, 0, subcolumn->size());
            return;
        }

        /// Top-level element read (no nested subcolumn). `result_column`'s type is
        /// `Nullable(narrowed_type)` because `DataTypeDynamic::getSubcolumnData` wraps element
        /// types via `makeNullableOrLowCardinalityNullableSafe`. When storage is already nullable
        /// the shapes match; otherwise we have to wrap the non-nullable storage in a null map.
        auto mutable_column = result_column->assumeMutable();
        if (dynamic_element_state->narrowed_stored_as_nullable)
        {
            mutable_column->insertRangeFrom(*storage_column, 0, added);
        }
        else
        {
            /// Storage is the bare `narrowed_type`; `result_column` is `Nullable(narrowed_type)`
            /// (or a LowCardinality(Nullable) variant). Wrap and append.
            auto nullable_storage = makeNullable(dynamic_element_state->narrowed_type)->createColumn();
            auto & nullable = assert_cast<ColumnNullable &>(*nullable_storage);
            nullable.getNestedColumnPtr()->assumeMutable()->insertRangeFrom(*storage_column, 0, added);
            auto & null_map = nullable.getNullMapData();
            null_map.resize_fill(added, static_cast<UInt8>(0));
            mutable_column->insertRangeFrom(*nullable_storage, 0, added);
        }
        return;
    }

    /// NARROWED part but the requested element is not the narrowed type → no values; emit `limit` defaults/nulls.
    if (dynamic_element_state->narrowed_type && !dynamic_element_state->read_from_narrowed)
    {
        auto mutable_column = result_column->assumeMutable();
        if (is_null_map_subcolumn)
        {
            auto & data = assert_cast<ColumnUInt8 &>(*mutable_column).getData();
            data.resize_fill(data.size() + limit, 1);
        }
        else
        {
            for (size_t i = 0; i != limit; ++i)
                mutable_column->insertDefault();
        }
        return;
    }

    /// Check if this subcolumn should not be read from shared variant.
    /// In this case just read data from the corresponding variant.
    if (!dynamic_element_state->read_from_shared_variant)
    {
        settings.path.push_back(Substream::DynamicData);
        dynamic_element_state->variant_serialization->deserializeBinaryBulkWithMultipleStreams(
            result_column, rows_offset, limit, settings, dynamic_element_state->variant_element_state, cache);
        settings.path.pop_back();
    }
    /// Otherwise, read the shared variant column and extract requested type from it.
    else
    {
        settings.path.push_back(Substream::DynamicData);
        /// Initialize shared_variant column if needed.
        if (result_column->empty() || !dynamic_element_state->shared_variant)
        {
            dynamic_element_state->shared_variant = makeNullable(ColumnDynamic::getSharedVariantDataType()->createColumn());
            dynamic_element_state->shared_variant_size = 0;
        }

        dynamic_element_state->variant_serialization->deserializeBinaryBulkWithMultipleStreams(
            dynamic_element_state->shared_variant, rows_offset, limit, settings, dynamic_element_state->variant_element_state, cache);
        size_t prev_shared_variant_size = dynamic_element_state->shared_variant_size;
        dynamic_element_state->shared_variant_size = dynamic_element_state->shared_variant->size();
        settings.path.pop_back();

        /// If we need to read a subcolumn from variant column, create an empty variant column, fill it and extract subcolumn.
        auto variant_type = DataTypeFactory::instance().get(dynamic_element_name);
        auto result_type = makeExtractedSubcolumnsNullableOrLowCardinalityNullableSafe(variant_type);
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
        for (size_t i = prev_shared_variant_size; i != shared_variant.size(); ++i)
        {
            if (!shared_null_map[i])
            {
                auto value = shared_variant.getDataAt(i);
                ReadBufferFromMemory buf(value);
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
                        null_map->push_back(static_cast<UInt8>(0));
                    }
                    else
                    {
                        variant_serialization->deserializeBinary(*non_nullable_variant_column, buf, format_settings);
                        if (null_map)
                            null_map->push_back(static_cast<UInt8>(0));
                    }
                }
                else if (is_null_map_subcolumn)
                {
                    null_map->push_back(static_cast<UInt8>(1));
                }
                else
                {
                    variant_column->insertDefault();
                }
            }
            else if (is_null_map_subcolumn)
            {
                null_map->push_back(static_cast<UInt8>(1));
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

size_t SerializationDynamicElement::allocatedBytes() const
{
    return sizeof(*this) + dynamic_element_name.capacity() + nested_subcolumn.capacity();
}

}
