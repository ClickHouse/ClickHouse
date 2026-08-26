#include <Common/SipHash.h>
#include <DataTypes/Serializations/SerializationDynamicElement.h>
#include <DataTypes/Serializations/SerializationDynamicHelpers.h>
#include <DataTypes/Serializations/SerializationVariantElement.h>
#include <DataTypes/Serializations/SerializationVariantElementNullMap.h>
#include <DataTypes/Serializations/SerializationDynamic.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/NullableUtils.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/castColumn.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

namespace
{

struct DynamicElementVariantReader
{
    DataTypePtr type;
    SerializationPtr serialization;
    ISerialization::DeserializeBinaryBulkStatePtr state;
    SerializationPtr null_map_serialization;
    ISerialization::DeserializeBinaryBulkStatePtr null_map_state;
    bool reads_nested_subcolumn_directly = false;

    DynamicElementVariantReader clone() const
    {
        auto new_reader = *this;
        new_reader.state = state ? state->clone() : nullptr;
        new_reader.null_map_state = null_map_state ? null_map_state->clone() : nullptr;
        return new_reader;
    }
};

/// Values of a single variant deserialized for the current range. They are deliberately not kept
/// in the deserialization state: the state is cloned by the substreams cache, so it must not own
/// data columns (see `SerializationVariantElement` and the shared variant handling below).
struct DynamicElementVariantReaderData
{
    MutableColumnPtr column;
    MutableColumnPtr null_map;
};

void insertSourceValueIntoColumn(MutableColumnPtr & dst, const IColumn & src, size_t row)
{
    if (auto * nullable_dst = typeid_cast<ColumnNullable *>(dst.get()))
    {
        if (const auto * nullable_src = typeid_cast<const ColumnNullable *>(&src))
            nullable_dst->insertFrom(*nullable_src, row);
        else
            nullable_dst->insertFromNotNullable(src, row);
    }
    else if (auto * low_cardinality_dst = typeid_cast<ColumnLowCardinality *>(dst.get()))
    {
        if (const auto * low_cardinality_src = typeid_cast<const ColumnLowCardinality *>(&src))
            low_cardinality_dst->insertFrom(*low_cardinality_src, row);
        else
            low_cardinality_dst->insertFromFullColumn(src, row);
    }
    else if (const auto * nullable_src = typeid_cast<const ColumnNullable *>(&src))
    {
        if (nullable_src->isNullAt(row))
            dst->insertDefault();
        else
            dst->insertFrom(nullable_src->getNestedColumn(), row);
    }
    else
    {
        dst->insertFrom(src, row);
    }
}

DynamicElementVariantReaderData deserializeVariantReader(
    DynamicElementVariantReader & reader,
    size_t limit,
    bool read_value,
    const IColumn & result_column_sample,
    ISerialization::DeserializeBinaryBulkSettings & settings,
    ISerialization::SubstreamsCache * cache)
{
    settings.path.push_back(ISerialization::Substream::DynamicData);

    DynamicElementVariantReaderData data;
    data.null_map = ColumnUInt8::create();

    reader.null_map_serialization->deserializeBinaryBulkWithMultipleStreams(
        *data.null_map, limit, settings, reader.null_map_state, cache);

    if (data.null_map->size() != limit)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unexpected size of deserialized Dynamic variant null map: {} instead of {}",
            data.null_map->size(),
            limit);

    if (read_value)
    {
        if (reader.reads_nested_subcolumn_directly)
            data.column = result_column_sample.cloneEmpty();
        else
            data.column = makeExtractedSubcolumnsNullableOrLowCardinalityNullableSafe(reader.type)->createColumn();

        reader.serialization->deserializeBinaryBulkWithMultipleStreams(
            *data.column, limit, settings, reader.state, cache);

        if (data.column->size() != limit)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Unexpected size of deserialized Dynamic variant column: {} instead of {}",
                data.column->size(),
                limit);
    }

    settings.path.pop_back();
    return data;
}

}

UInt128 SerializationDynamicElement::getHash(
    const SerializationPtr & nested_,
    const SerializationPtr & shared_variant_serialization_,
    const String & dynamic_element_name_,
    const String & nested_subcolumn_,
    const SerializationInfoSettings & serialization_info_settings_,
    bool is_null_map_subcolumn_)
{
    SipHash hash;
    hash.update("DynamicElement");
    hash.update(nested_->getHash());
    hash.update(shared_variant_serialization_->getHash());
    hash.update(dynamic_element_name_.size());
    hash.update(dynamic_element_name_);
    hash.update(nested_subcolumn_.size());
    hash.update(nested_subcolumn_);
    serialization_info_settings_.updateHash(hash);
    hash.update(is_null_map_subcolumn_);
    return hash.get128();
}

SerializationPtr SerializationDynamicElement::create(
    const SerializationPtr & nested_,
    const SerializationPtr & shared_variant_serialization_,
    const String & dynamic_element_name_,
    const String & nested_subcolumn_,
    const SerializationInfoSettings & serialization_info_settings_,
    bool is_null_map_subcolumn_)
{
    if (!nested_->supportsPooling() || !shared_variant_serialization_->supportsPooling())
        return std::shared_ptr<ISerialization>(new SerializationDynamicElement(
            nested_, shared_variant_serialization_, dynamic_element_name_, nested_subcolumn_, serialization_info_settings_, is_null_map_subcolumn_));
    return ISerialization::pooled(
        getHash(nested_, shared_variant_serialization_, dynamic_element_name_, nested_subcolumn_, serialization_info_settings_, is_null_map_subcolumn_),
        [&]
        {
            return new SerializationDynamicElement(
                nested_, shared_variant_serialization_, dynamic_element_name_, nested_subcolumn_, serialization_info_settings_, is_null_map_subcolumn_);
        });
}

struct DeserializeBinaryBulkStateDynamicElement : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr structure_state;
    std::vector<DynamicElementVariantReader> variant_readers;
    SerializationPtr shared_variant_serialization;
    ISerialization::DeserializeBinaryBulkStatePtr shared_variant_state;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateDynamicElement>(*this);
        new_state->structure_state = structure_state ? structure_state->clone() : nullptr;
        new_state->variant_readers.clear();
        new_state->variant_readers.reserve(variant_readers.size());
        for (const auto & reader : variant_readers)
            new_state->variant_readers.push_back(reader.clone());
        new_state->shared_variant_state = shared_variant_state ? shared_variant_state->clone() : nullptr;
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
    /// If we don't have any compatible variants and won't read the shared variant, there are no streams to enumerate.
    if (deserialize_state->variant_readers.empty() && !deserialize_state->shared_variant_serialization)
        return;

    for (const auto & reader : deserialize_state->variant_readers)
    {
        if (!reader.serialization && !reader.null_map_serialization)
            continue;

        settings.path.push_back(Substream::DynamicData);
        /// A reader that reads the nested subcolumn directly wraps `nested_serialization`, which
        /// produces the requested subcolumn type, not the variant type, so it must be enumerated
        /// with the subcolumn type (e.g. `SerializationString::enumerateStreamsWithSize` would
        /// otherwise be given the enclosing `Variant` type and fail with a bad cast).
        auto variant_data = SubstreamData(reader.serialization ? reader.serialization : reader.null_map_serialization)
                                .withType(reader.reads_nested_subcolumn_directly ? data.type : reader.type)
                                .withColumn(reader.reads_nested_subcolumn_directly ? data.column : nullptr)
                                .withSerializationInfo(data.serialization_info)
                                .withDeserializeState(reader.serialization ? reader.state : reader.null_map_state);
        settings.path.back().data = variant_data;
        if (reader.serialization)
            reader.serialization->enumerateStreams(settings, callback, variant_data);
        else
            reader.null_map_serialization->enumerateStreams(settings, callback, variant_data);
        settings.path.pop_back();
    }

    if (deserialize_state->shared_variant_serialization)
    {
        settings.path.push_back(Substream::DynamicData);
        auto variant_data = SubstreamData(deserialize_state->shared_variant_serialization)
                                .withType(ColumnDynamic::getSharedVariantDataType())
                                .withColumn(ColumnDynamic::getSharedVariantDataType()->createColumn())
                                .withSerializationInfo(data.serialization_info)
                                .withDeserializeState(deserialize_state->shared_variant_state);
        settings.path.back().data = variant_data;
        deserialize_state->shared_variant_serialization->enumerateStreams(settings, callback, variant_data);
        settings.path.pop_back();
    }
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
    const auto * dynamic_structure_state
        = checkAndGetState<SerializationDynamic::DeserializeBinaryBulkStateDynamicStructure>(dynamic_element_state->structure_state);
    const auto & variant_type = assert_cast<const DataTypeVariant &>(*dynamic_structure_state->variant_type);
    auto requested_type = DataTypeFactory::instance().get(dynamic_element_name);
    auto shared_variant_global_discr = variant_type.tryGetVariantDiscriminator(ColumnDynamic::getSharedVariantTypeName());
    chassert(shared_variant_global_discr.has_value());
    const auto & variants = variant_type.getVariants();

    auto add_variant_reader = [&](ColumnVariant::Discriminator discr)
    {
        const auto & matched_variant_name = variants[discr]->getName();
        auto & reader = dynamic_element_state->variant_readers.emplace_back();
        settings.path.push_back(Substream::DynamicData);
        if (is_null_map_subcolumn)
        {
            reader.type = variants[discr];
            reader.null_map_serialization = SerializationVariantElementNullMap::create(matched_variant_name, discr, variants.size());
            reader.null_map_serialization->deserializeBinaryBulkStatePrefix(settings, reader.null_map_state, cache);
        }
        else
        {
            reader.type = variants[discr];
            reader.reads_nested_subcolumn_directly = variants[discr]->equals(*requested_type);
            SerializationPtr variant_serialization = reader.reads_nested_subcolumn_directly
                ? nested_serialization
                : variants[discr]->getSerialization(serialization_info_settings);
            reader.serialization = SerializationVariantElement::create(variant_serialization, matched_variant_name, discr, variants.size());
            reader.serialization->deserializeBinaryBulkStatePrefix(settings, reader.state, cache);
            reader.null_map_serialization = SerializationVariantElementNullMap::create(matched_variant_name, discr, variants.size());
            reader.null_map_serialization->deserializeBinaryBulkStatePrefix(settings, reader.null_map_state, cache);
        }
        settings.path.pop_back();
    };

    /// The exact-variant fast path is valid only for types whose compatibility is plain equality.
    /// For types with dynamic subcolumns (`JSON`, `Dynamic` and containers of them), other variants
    /// can be compatible without being equal (e.g. plain `JSON` and `JSON(max_dynamic_paths=0)`),
    /// and compatible values can also live in the shared variant, so all of them must be read.
    if (auto exact_variant_discr = variant_type.tryGetVariantDiscriminator(dynamic_element_name);
        exact_variant_discr && exact_variant_discr != shared_variant_global_discr && !requested_type->hasDynamicSubcolumns())
    {
        add_variant_reader(*exact_variant_discr);
    }
    else
    {
        for (ColumnVariant::Discriminator discr = 0; discr != variants.size(); ++discr)
        {
            if (discr == *shared_variant_global_discr)
                continue;

            /// Read compatibility is path-local for both whole-type and nested subcolumn reads:
            /// the in-memory path (`DataTypeDynamic::getDynamicSubcolumnData`) uses the same
            /// predicate for `d.\`Map(String, JSON)\``, so the stored-path reader must see rows
            /// stored as `Map(String, JSON(a UInt64))` as well. Values of a compatible but not
            /// equal variant are converted to the requested type before insertion.
            if (areDynamicSubcolumnTypesCompatibleForRead(variants[discr], requested_type))
                add_variant_reader(discr);
        }

        /// SharedVariant can contain values compatible with the requested type when no exact variant exists.
        settings.path.push_back(Substream::DynamicData);
        dynamic_element_state->shared_variant_serialization = SerializationVariantElement::create(
            shared_variant_serialization,
            ColumnDynamic::getSharedVariantTypeName(),
            *shared_variant_global_discr,
            variants.size());
        dynamic_element_state->shared_variant_serialization->deserializeBinaryBulkStatePrefix(settings, dynamic_element_state->shared_variant_state, cache);
        settings.path.pop_back();
    }

    state = std::move(dynamic_element_state);
}

void SerializationDynamicElement::serializeBinaryBulkWithMultipleStreams(const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationDynamicElement");
}

void SerializationDynamicElement::deserializeBinaryBulkWithMultipleStreams(
    IColumn & result_column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    if (!state)
    {
        if (is_null_map_subcolumn)
        {
            auto & data = assert_cast<ColumnUInt8 &>(result_column).getData();
            data.resize_fill(data.size() + limit, 1);
        }

        return;
    }

    auto * dynamic_element_state = checkAndGetState<DeserializeBinaryBulkStateDynamicElement>(state);
    auto requested_type = DataTypeFactory::instance().get(dynamic_element_name);

    /// Deserialize every compatible variant of the current range into its own fresh columns.
    std::vector<DynamicElementVariantReaderData> variant_readers_data;
    variant_readers_data.reserve(dynamic_element_state->variant_readers.size());
    for (auto & reader : dynamic_element_state->variant_readers)
        variant_readers_data.push_back(
            deserializeVariantReader(reader, limit, !is_null_map_subcolumn, result_column, settings, cache));

    MutableColumnPtr shared_variant_result_column;
    auto shared_variant_null_map_column = ColumnUInt8::create();
    auto & shared_variant_result_null_map = shared_variant_null_map_column->getData();

    if (dynamic_element_state->shared_variant_serialization)
    {
        MutableColumnPtr variant_column = is_null_map_subcolumn ? nullptr : result_column.cloneEmpty();
        if (variant_column)
            variant_column->reserve(limit);

        /// Deserialize the shared variant for the current range into a fresh column.
        auto shared_variant_column
            = ColumnNullable::create(ColumnDynamic::getSharedVariantDataType()->createColumn(), ColumnUInt8::create());

        settings.path.push_back(Substream::DynamicData);
        dynamic_element_state->shared_variant_serialization->deserializeBinaryBulkWithMultipleStreams(
            *shared_variant_column, limit, settings, dynamic_element_state->shared_variant_state, cache);
        settings.path.pop_back();

        if (shared_variant_column->size() != limit)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Unexpected size of deserialized Dynamic shared variant column: {} instead of {}",
                shared_variant_column->size(),
                limit);

        shared_variant_result_null_map.reserve(limit);

        const auto & nullable_shared_variant = assert_cast<const ColumnNullable &>(*shared_variant_column);
        const auto & shared_null_map = nullable_shared_variant.getNullMapData();
        const auto & shared_variant = assert_cast<const ColumnString &>(nullable_shared_variant.getNestedColumn());
        const FormatSettings format_settings;
        for (size_t i = 0; i != shared_variant.size(); ++i)
        {
            if (!shared_null_map[i])
            {
                auto value = shared_variant.getDataAt(i);
                ReadBufferFromMemory buf(value);
                /// Reading already-stored shared-variant data: not limited by the input complexity guard.
                auto type = decodeDataType(buf);
                /// Same path-local read compatibility as in deserializeBinaryBulkStatePrefix.
                if (areDynamicSubcolumnTypesCompatibleForRead(type, requested_type))
                {
                    shared_variant_result_null_map.push_back(static_cast<UInt8>(0));
                    if (!is_null_map_subcolumn)
                    {
                        auto tmp_column = type->createColumn();
                        type->getDefaultSerialization()->deserializeBinary(*tmp_column, buf, format_settings);
                        if (nested_subcolumn.empty())
                        {
                            /// The stored type may declare a different path set than the requested
                            /// type (read compatibility is path-local), so its column layout can
                            /// differ. Convert the value to the requested type before insertion.
                            ColumnPtr value_column = std::move(tmp_column);
                            if (!type->equals(*requested_type))
                                value_column = castColumn({value_column, type, ""}, requested_type);
                            insertSourceValueIntoColumn(variant_column, *value_column, 0);
                        }
                        else
                        {
                            /// Compatibility for nested subcolumn reads is path-local, so the stored
                            /// type may declare a different path set than the requested type (e.g.
                            /// `JSON(a UInt64)` vs plain `JSON`). Convert the value to the requested
                            /// type first, then extract the requested subcolumn from it.
                            ColumnPtr value_column = std::move(tmp_column);
                            if (!type->equals(*requested_type))
                                value_column = castColumn({value_column, type, ""}, requested_type);
                            auto subcolumn = requested_type->getSubcolumn(nested_subcolumn, value_column);
                            insertSourceValueIntoColumn(variant_column, *subcolumn, 0);
                        }
                    }
                }
                else
                {
                    shared_variant_result_null_map.push_back(static_cast<UInt8>(1));
                    if (!is_null_map_subcolumn)
                        variant_column->insertDefault();
                }
            }
            else
            {
                shared_variant_result_null_map.push_back(static_cast<UInt8>(1));
                if (!is_null_map_subcolumn)
                    variant_column->insertDefault();
            }
        }

        shared_variant_result_column = std::move(variant_column);
    }
    else
    {
        shared_variant_result_null_map.resize_fill(limit, 1);
    }

    if (is_null_map_subcolumn)
    {
        auto & data = assert_cast<ColumnUInt8 &>(result_column).getData();
        data.reserve(data.size() + limit);
        for (size_t i = 0; i != limit; ++i)
        {
            UInt8 is_null = shared_variant_result_null_map[i];
            for (const auto & reader_data : variant_readers_data)
            {
                const auto & null_map = assert_cast<const ColumnUInt8 &>(*reader_data.null_map).getData();
                if (!null_map[i])
                {
                    is_null = 0;
                    break;
                }
            }

            data.push_back(is_null);
        }

        return;
    }

    if (result_column.empty() && result_column.hasDynamicStructure())
    {
        for (size_t reader_index = 0; reader_index != dynamic_element_state->variant_readers.size(); ++reader_index)
        {
            const auto & reader = dynamic_element_state->variant_readers[reader_index];
            const auto & reader_column = variant_readers_data[reader_index].column;
            if (reader.reads_nested_subcolumn_directly && reader_column && reader_column->hasDynamicStructure())
            {
                result_column.takeExactDynamicStructureFrom(*reader_column);
                break;
            }
        }
    }

    auto variant_column = result_column.cloneEmpty();
    variant_column->reserve(limit);
    std::vector<ColumnPtr> variant_reader_columns;
    variant_reader_columns.reserve(dynamic_element_state->variant_readers.size());
    for (size_t reader_index = 0; reader_index != dynamic_element_state->variant_readers.size(); ++reader_index)
    {
        const auto & reader = dynamic_element_state->variant_readers[reader_index];
        ColumnPtr reader_column = std::move(variant_readers_data[reader_index].column);
        if (reader.reads_nested_subcolumn_directly)
        {
            variant_reader_columns.push_back(std::move(reader_column));
        }
        else
        {
            auto reader_result_type = makeExtractedSubcolumnsNullableOrLowCardinalityNullableSafe(reader.type);
            /// Read compatibility is path-local, so the variant may declare a different path set
            /// than the requested type (e.g. `JSON(a UInt64)` vs plain `JSON`), making their
            /// column layouts differ. Convert the variant column to the requested type first
            /// (and for nested subcolumn reads, then extract the requested subcolumn from it).
            auto requested_result_type = makeExtractedSubcolumnsNullableOrLowCardinalityNullableSafe(requested_type);
            if (!reader_result_type->equals(*requested_result_type))
            {
                reader_column = castColumn({reader_column, reader_result_type, ""}, requested_result_type);
                reader_result_type = requested_result_type;
            }
            if (nested_subcolumn.empty())
                variant_reader_columns.push_back(std::move(reader_column));
            else
                variant_reader_columns.push_back(reader_result_type->getSubcolumn(nested_subcolumn, reader_column));
        }
    }

    for (size_t i = 0; i != limit; ++i)
    {
        bool inserted = false;
        for (size_t reader_index = 0; reader_index != dynamic_element_state->variant_readers.size(); ++reader_index)
        {
            const auto & null_map = assert_cast<const ColumnUInt8 &>(*variant_readers_data[reader_index].null_map).getData();
            if (!null_map[i])
            {
                insertSourceValueIntoColumn(variant_column, *variant_reader_columns[reader_index], i);
                inserted = true;
                break;
            }
        }

        if (!inserted && !shared_variant_result_null_map[i])
        {
            insertSourceValueIntoColumn(variant_column, *shared_variant_result_column, i);
            inserted = true;
        }

        if (!inserted)
            variant_column->insertDefault();
    }

    result_column.insertRangeFrom(*variant_column, 0, variant_column->size());
}

size_t SerializationDynamicElement::allocatedBytes() const
{
    return sizeof(*this) + dynamic_element_name.capacity() + nested_subcolumn.capacity();
}

}
