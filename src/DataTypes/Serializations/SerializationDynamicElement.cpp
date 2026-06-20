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
    ColumnPtr column;
    size_t column_size = 0;
    ColumnPtr null_map;
    size_t null_map_size = 0;
    bool reads_nested_subcolumn_directly = false;

    DynamicElementVariantReader clone() const
    {
        auto new_reader = *this;
        new_reader.state = state ? state->clone() : nullptr;
        new_reader.null_map_state = null_map_state ? null_map_state->clone() : nullptr;
        new_reader.column = nullptr;
        new_reader.column_size = 0;
        new_reader.null_map = nullptr;
        new_reader.null_map_size = 0;
        return new_reader;
    }
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

void deserializeVariantReader(
    DynamicElementVariantReader & reader,
    size_t rows_offset,
    size_t limit,
    bool read_value,
    const IColumn & result_column_sample,
    ISerialization::DeserializeBinaryBulkSettings & settings,
    ISerialization::SubstreamsCache * cache)
{
    settings.path.push_back(ISerialization::Substream::DynamicData);

    if (!reader.null_map)
    {
        reader.null_map = ColumnUInt8::create();
        reader.null_map_size = 0;
    }

    reader.null_map_serialization->deserializeBinaryBulkWithMultipleStreams(
        reader.null_map, rows_offset, limit, settings, reader.null_map_state, cache);

    if (reader.null_map->size() != reader.null_map_size + limit)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unexpected size of deserialized Dynamic variant null map: {} instead of {}",
            reader.null_map->size(),
            reader.null_map_size + limit);

    reader.null_map_size = reader.null_map->size();

    if (read_value)
    {
        if (!reader.column)
        {
            if (reader.reads_nested_subcolumn_directly)
                reader.column = result_column_sample.cloneEmpty();
            else
                reader.column = makeExtractedSubcolumnsNullableOrLowCardinalityNullableSafe(reader.type)->createColumn();
            reader.column_size = 0;
        }

        reader.serialization->deserializeBinaryBulkWithMultipleStreams(
            reader.column, rows_offset, limit, settings, reader.state, cache);

        if (reader.column->size() != reader.column_size + limit)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Unexpected size of deserialized Dynamic variant column: {} instead of {}",
                reader.column->size(),
                reader.column_size + limit);

        reader.column_size = reader.column->size();
    }

    settings.path.pop_back();
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
    ColumnPtr shared_variant;
    size_t shared_variant_size = 0;


    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateDynamicElement>(*this);
        new_state->structure_state = structure_state ? structure_state->clone() : nullptr;
        new_state->variant_readers.clear();
        new_state->variant_readers.reserve(variant_readers.size());
        for (const auto & reader : variant_readers)
            new_state->variant_readers.push_back(reader.clone());
        new_state->shared_variant_state = shared_variant_state ? shared_variant_state->clone() : nullptr;
        new_state->shared_variant = nullptr;
        new_state->shared_variant_size = 0;
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
        auto variant_data = SubstreamData(reader.serialization ? reader.serialization : reader.null_map_serialization)
                                .withType(reader.type)
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
            reader.null_map_serialization = SerializationVariantElementNullMap::create(matched_variant_name, discr);
            reader.null_map_serialization->deserializeBinaryBulkStatePrefix(settings, reader.null_map_state, cache);
        }
        else
        {
            reader.type = variants[discr];
            reader.reads_nested_subcolumn_directly = variants[discr]->equals(*requested_type);
            SerializationPtr variant_serialization = reader.reads_nested_subcolumn_directly
                ? nested_serialization
                : variants[discr]->getSerialization(serialization_info_settings);
            reader.serialization = SerializationVariantElement::create(variant_serialization, matched_variant_name, discr);
            reader.serialization->deserializeBinaryBulkStatePrefix(settings, reader.state, cache);
            reader.null_map_serialization = SerializationVariantElementNullMap::create(matched_variant_name, discr);
            reader.null_map_serialization->deserializeBinaryBulkStatePrefix(settings, reader.null_map_state, cache);
        }
        settings.path.pop_back();
    };

    if (auto exact_variant_discr = variant_type.tryGetVariantDiscriminator(dynamic_element_name);
        exact_variant_discr && exact_variant_discr != shared_variant_global_discr)
    {
        add_variant_reader(*exact_variant_discr);
    }
    else
    {
        for (ColumnVariant::Discriminator discr = 0; discr != variants.size(); ++discr)
        {
            if (discr == *shared_variant_global_discr)
                continue;

            const bool can_read_variant = nested_subcolumn.empty()
                ? areDynamicStorageTypesCompatible(variants[discr], requested_type)
                : areDynamicSubcolumnTypesCompatible(variants[discr], requested_type);
            if (can_read_variant)
                add_variant_reader(discr);
        }

        /// SharedVariant can contain values compatible with the requested type when no exact variant exists.
        settings.path.push_back(Substream::DynamicData);
        dynamic_element_state->shared_variant_serialization = SerializationVariantElement::create(
            shared_variant_serialization,
            ColumnDynamic::getSharedVariantTypeName(),
            *shared_variant_global_discr);
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
    if (result_column->empty())
    {
        for (auto & reader : dynamic_element_state->variant_readers)
        {
            reader.column = nullptr;
            reader.column_size = 0;
            reader.null_map = nullptr;
            reader.null_map_size = 0;
        }

        dynamic_element_state->shared_variant = nullptr;
        dynamic_element_state->shared_variant_size = 0;
    }

    auto requested_type = DataTypeFactory::instance().get(dynamic_element_name);

    for (auto & reader : dynamic_element_state->variant_readers)
        deserializeVariantReader(reader, rows_offset, limit, !is_null_map_subcolumn, *result_column, settings, cache);

    if (dynamic_element_state->shared_variant_serialization && !dynamic_element_state->shared_variant)
    {
        dynamic_element_state->shared_variant = makeNullable(ColumnDynamic::getSharedVariantDataType()->createColumn());
        dynamic_element_state->shared_variant_size = 0;
    }

    ColumnPtr shared_variant_result_column;
    ColumnPtr shared_variant_null_map_column = ColumnUInt8::create();
    auto & shared_variant_result_null_map = assert_cast<ColumnUInt8 &>(*shared_variant_null_map_column->assumeMutable()).getData();

    if (dynamic_element_state->shared_variant_serialization)
    {
        MutableColumnPtr variant_column = is_null_map_subcolumn ? nullptr : result_column->cloneEmpty();
        if (variant_column)
            variant_column->reserve(limit);

        settings.path.push_back(Substream::DynamicData);
        dynamic_element_state->shared_variant_serialization->deserializeBinaryBulkWithMultipleStreams(
            dynamic_element_state->shared_variant, rows_offset, limit, settings, dynamic_element_state->shared_variant_state, cache);
        size_t prev_shared_variant_size = dynamic_element_state->shared_variant_size;
        dynamic_element_state->shared_variant_size = dynamic_element_state->shared_variant->size();
        settings.path.pop_back();

        if (dynamic_element_state->shared_variant_size != prev_shared_variant_size + limit)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Unexpected size of deserialized Dynamic shared variant column: {} instead of {}",
                dynamic_element_state->shared_variant_size,
                prev_shared_variant_size + limit);

        shared_variant_result_null_map.reserve(limit);

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
                const bool can_read_shared_variant = nested_subcolumn.empty()
                    ? areDynamicStorageTypesCompatible(type, requested_type)
                    : areDynamicSubcolumnTypesCompatible(type, requested_type);
                if (can_read_shared_variant)
                {
                    shared_variant_result_null_map.push_back(static_cast<UInt8>(0));
                    if (!is_null_map_subcolumn)
                    {
                        auto tmp_column = type->createColumn();
                        type->getDefaultSerialization()->deserializeBinary(*tmp_column, buf, format_settings);
                        if (nested_subcolumn.empty())
                        {
                            insertSourceValueIntoColumn(variant_column, *tmp_column, 0);
                        }
                        else
                        {
                            auto subcolumn = type->getSubcolumn(nested_subcolumn, tmp_column->getPtr());
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
        auto mutable_column = result_column->assumeMutable();
        auto & data = assert_cast<ColumnUInt8 &>(*mutable_column).getData();
        data.reserve(data.size() + limit);
        for (size_t i = 0; i != limit; ++i)
        {
            UInt8 is_null = shared_variant_result_null_map[i];
            for (const auto & reader : dynamic_element_state->variant_readers)
            {
                const auto & null_map = assert_cast<const ColumnUInt8 &>(*reader.null_map).getData();
                if (!null_map[reader.null_map_size - limit + i])
                {
                    is_null = 0;
                    break;
                }
            }

            data.push_back(is_null);
        }

        return;
    }

    if (result_column->empty() && result_column->hasDynamicStructure())
    {
        for (const auto & reader : dynamic_element_state->variant_readers)
        {
            if (reader.reads_nested_subcolumn_directly && reader.column && reader.column->hasDynamicStructure())
            {
                result_column->assumeMutable()->takeExactDynamicStructureFrom(*reader.column);
                break;
            }
        }
    }

    auto variant_column = result_column->cloneEmpty();
    variant_column->reserve(limit);
    std::vector<ColumnPtr> variant_reader_columns;
    variant_reader_columns.reserve(dynamic_element_state->variant_readers.size());
    for (const auto & reader : dynamic_element_state->variant_readers)
    {
        if (nested_subcolumn.empty() || reader.reads_nested_subcolumn_directly)
        {
            variant_reader_columns.push_back(reader.column);
        }
        else
        {
            auto reader_result_type = makeExtractedSubcolumnsNullableOrLowCardinalityNullableSafe(reader.type);
            variant_reader_columns.push_back(reader_result_type->getSubcolumn(nested_subcolumn, reader.column));
        }
    }

    for (size_t i = 0; i != limit; ++i)
    {
        bool inserted = false;
        for (size_t reader_index = 0; reader_index != dynamic_element_state->variant_readers.size(); ++reader_index)
        {
            const auto & reader = dynamic_element_state->variant_readers[reader_index];
            const auto & null_map = assert_cast<const ColumnUInt8 &>(*reader.null_map).getData();
            size_t row = reader.null_map_size - limit + i;
            if (!null_map[row])
            {
                insertSourceValueIntoColumn(variant_column, *variant_reader_columns[reader_index], reader.column_size - limit + i);
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

    result_column->assumeMutable()->insertRangeFrom(*variant_column, 0, variant_column->size());
}

size_t SerializationDynamicElement::allocatedBytes() const
{
    return sizeof(*this) + dynamic_element_name.capacity() + nested_subcolumn.capacity();
}

}
