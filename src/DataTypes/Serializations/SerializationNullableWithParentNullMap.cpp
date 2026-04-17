#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationNullableWithParentNullMap.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace
{

struct ParentNullMapState : public ISerialization::DeserializeBinaryBulkState
{
    ColumnPtr accumulated_parent_null_map;
    ISerialization::DeserializeBinaryBulkStatePtr nested_state;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<ParentNullMapState>(*this);
        new_state->nested_state = nested_state ? nested_state->clone() : nullptr;
        return new_state;
    }
};

}

SerializationNullableWithParentNullMap::SerializationNullableWithParentNullMap(const SerializationPtr & nested_)
    : SerializationWrapper(nested_)
{
}

UInt128 SerializationNullableWithParentNullMap::getHash(const SerializationPtr & nested_)
{
    SipHash hash;
    hash.update("NullableWithParentNullMap");
    hash.update(nested_->getHash());
    return hash.get128();
}

SerializationPtr SerializationNullableWithParentNullMap::create(const SerializationPtr & nested_)
{
    if (!nested_->supportsPooling())
        return std::shared_ptr<ISerialization>(new SerializationNullableWithParentNullMap(nested_));
    return ISerialization::pooled(getHash(nested_), [&] { return new SerializationNullableWithParentNullMap(nested_); });
}

void SerializationNullableWithParentNullMap::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    SubstreamData nested_data = data;
    if (data.deserialize_state)
    {
        const auto * parent_state = checkAndGetState<ParentNullMapState>(data.deserialize_state);
        nested_data.deserialize_state = parent_state->nested_state;
    }

    settings.path.push_back(Substream::NullMap);
    settings.path.back().data
        = SubstreamData(SerializationNumber<UInt8>::create()).withType(std::make_shared<DataTypeUInt8>());
    callback(settings.path);
    settings.path.pop_back();

    /// Inner streams emitted by the nested serialization are placed under
    /// NullableElements so file name resolution matches the on-disk layout.
    settings.path.push_back(Substream::NullableElements);
    settings.path.back().data = nested_data;
    nested_serialization->enumerateStreams(settings, callback, nested_data);
    settings.path.pop_back();
}

void SerializationNullableWithParentNullMap::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    auto parent_null_map_state = std::make_shared<ParentNullMapState>();
    parent_null_map_state->accumulated_parent_null_map = ColumnUInt8::create();

    settings.path.push_back(Substream::NullableElements);
    nested_serialization->deserializeBinaryBulkStatePrefix(settings, parent_null_map_state->nested_state, cache);
    settings.path.pop_back();

    state = std::move(parent_null_map_state);
}

void SerializationNullableWithParentNullMap::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    /// Nested serialization produces a ColumnNullable with its own null map.
    /// Read it first, then OR in the parent's null map for the rows just read.
    auto * parent_null_map_state = assert_cast<ParentNullMapState *>(state.get());

    if (column->empty())
        parent_null_map_state->accumulated_parent_null_map = ColumnUInt8::create();

    size_t prev_size = column->size();

    settings.path.push_back(Substream::NullableElements);
    nested_serialization->deserializeBinaryBulkWithMultipleStreams(
        column, rows_offset, limit, settings, parent_null_map_state->nested_state, cache);
    settings.path.pop_back();

    size_t new_rows = column->size() - prev_size;
    if (new_rows == 0)
        return;

    /// Append the parent's null map for this range into `accumulated_parent_null_map`.
    settings.path.push_back(Substream::NullMap);
    if (insertDataFromSubstreamsCacheIfAny(cache, settings, parent_null_map_state->accumulated_parent_null_map))
    {
        /// Data was inserted from cache.
    }
    else if (auto * stream = settings.getter(settings.path))
    {
        size_t accumulated_parent_size_before = parent_null_map_state->accumulated_parent_null_map->size();
        auto mutable_accumulated_parent_null_map = IColumn::mutate(std::move(parent_null_map_state->accumulated_parent_null_map));
        SerializationNumber<UInt8>::create()->deserializeBinaryBulk(*mutable_accumulated_parent_null_map, *stream, rows_offset, limit, 0);
        parent_null_map_state->accumulated_parent_null_map = std::move(mutable_accumulated_parent_null_map);
        addColumnWithNumReadRowsToSubstreamsCache(
            cache,
            settings.path,
            parent_null_map_state->accumulated_parent_null_map,
            parent_null_map_state->accumulated_parent_null_map->size() - accumulated_parent_size_before);
    }
    settings.path.pop_back();

    const auto & accumulated_parent_null_map_data
        = assert_cast<const ColumnUInt8 &>(*parent_null_map_state->accumulated_parent_null_map).getData();
    chassert(accumulated_parent_null_map_data.size() >= prev_size + new_rows);

    auto mutable_column = column->assumeMutable();
    auto & column_nullable = assert_cast<ColumnNullable &>(*mutable_column);
    auto & inner_null_map_data = column_nullable.getNullMapData();

    for (size_t i = 0; i < new_rows; ++i)
        inner_null_map_data[prev_size + i] |= accumulated_parent_null_map_data[prev_size + i];

    column = std::move(mutable_column);
}

}
