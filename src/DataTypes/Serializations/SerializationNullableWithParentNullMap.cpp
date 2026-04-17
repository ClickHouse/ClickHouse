#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationNullableWithParentNullMap.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
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
    settings.path.push_back(Substream::NullMap);
    settings.path.back().data = SubstreamData(SerializationNumber<UInt8>::create()).withType(std::make_shared<DataTypeUInt8>());
    callback(settings.path);
    settings.path.pop_back();

    settings.path.push_back(Substream::NullableElements);
    settings.path.back().data = data;
    nested_serialization->enumerateStreams(settings, callback, data);
    settings.path.pop_back();
}

void SerializationNullableWithParentNullMap::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    settings.path.push_back(Substream::NullableElements);
    nested_serialization->deserializeBinaryBulkStatePrefix(settings, state, cache);
    settings.path.pop_back();
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
    size_t prev_size = column->size();

    settings.path.push_back(Substream::NullableElements);
    nested_serialization->deserializeBinaryBulkWithMultipleStreams(
        column, rows_offset, limit, settings, state, cache);
    settings.path.pop_back();

    size_t new_rows = column->size() - prev_size;
    if (new_rows == 0)
        return;

    ColumnPtr parent_null_map = ColumnUInt8::create();

    settings.path.push_back(Substream::NullMap);
    if (insertDataFromSubstreamsCacheIfAny(cache, settings, parent_null_map))
    {
        /// Data was inserted from cache.
    }
    else if (auto * stream = settings.getter(settings.path))
    {
        auto mutable_parent_null_map = IColumn::mutate(std::move(parent_null_map));
        SerializationNumber<UInt8>::create()->deserializeBinaryBulk(*mutable_parent_null_map, *stream, rows_offset, limit, 0);
        parent_null_map = std::move(mutable_parent_null_map);
        addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, parent_null_map, parent_null_map->size());
    }
    settings.path.pop_back();

    const auto & parent_null_map_data = assert_cast<const ColumnUInt8 &>(*parent_null_map).getData();

    if (parent_null_map_data.size() < new_rows)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Parent null map has fewer rows than the nested column of `Nullable(Tuple(...))` subcolumn "
            "(parent null map size = {}, required = {})",
            parent_null_map_data.size(),
            new_rows);

    size_t parent_offset = parent_null_map_data.size() - new_rows;

    auto mutable_column = column->assumeMutable();
    auto & column_nullable = assert_cast<ColumnNullable &>(*mutable_column);
    auto & inner_null_map_data = column_nullable.getNullMapData();

    for (size_t i = 0; i < new_rows; ++i)
        inner_null_map_data[prev_size + i] |= parent_null_map_data[parent_offset + i];

    column = std::move(mutable_column);
}

}
