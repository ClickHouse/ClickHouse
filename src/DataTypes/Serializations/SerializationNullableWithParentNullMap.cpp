#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/NullableUtils.h>
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

SerializationNullableWithParentNullMap::SerializationNullableWithParentNullMap(
    const SerializationPtr & nested_, const DataTypePtr & on_disk_type_)
    : SerializationWrapper(nested_)
    , on_disk_type(on_disk_type_)
{
}

UInt128 SerializationNullableWithParentNullMap::getHash(const SerializationPtr & nested_, const DataTypePtr & on_disk_type_)
{
    SipHash hash;
    hash.update("NullableWithParentNullMap");
    hash.update(nested_->getHash());
    /// The on-disk type decides how the deserialization buffer is built, so the two flavours must not share
    /// a pooled instance.
    hash.update(on_disk_type_ != nullptr);
    if (on_disk_type_)
    {
        auto on_disk_type_name = on_disk_type_->getName();
        hash.update(on_disk_type_name.size());
        hash.update(on_disk_type_name);
    }
    return hash.get128();
}

SerializationPtr SerializationNullableWithParentNullMap::create(const SerializationPtr & nested_, const DataTypePtr & on_disk_type_)
{
    if (!nested_->supportsPooling())
        return std::shared_ptr<ISerialization>(new SerializationNullableWithParentNullMap(nested_, on_disk_type_));
    return ISerialization::pooled(
        getHash(nested_, on_disk_type_), [&] { return new SerializationNullableWithParentNullMap(nested_, on_disk_type_); });
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
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    /// Read the parent's null map first, matching the substream order of `SerializationNullable`, so that
    /// streams are always read forward in Compact parts.
    ColumnPtr parent_null_map;
    size_t parent_num_read_rows = 0;

    settings.path.push_back(Substream::NullMap);
    if (auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
    {
        /// The cached column may contain rows from multiple ranges read into the same result block;
        /// the rows of the current range are at its tail.
        std::tie(parent_null_map, parent_num_read_rows) = *cached_column_with_num_read_rows;
    }
    else if (auto * stream = settings.getter(settings.path))
    {
        auto mutable_parent_null_map = ColumnUInt8::create();
        SerializationNumber<UInt8>::create()->deserializeBinaryBulk(*mutable_parent_null_map, *stream, limit, 0);
        parent_null_map = std::move(mutable_parent_null_map);
        parent_num_read_rows = parent_null_map->size();
        addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, parent_null_map, parent_num_read_rows);
    }
    settings.path.pop_back();

    /// Deserialize the nested element into a temporary per-range column instead of into the accumulated
    /// result. The nested serialization publishes this column's substreams into the shared substreams cache,
    /// where a reader of the whole parent column reuses them, so they must stay exactly as read from disk.
    /// The parent null map applied below is destructive (for Variant/Dynamic it physically drops the
    /// parent-NULL rows, for a non-nullable LowCardinality it promotes the dictionary), so it is applied to a
    /// private copy, never to `range_column` (nor to the result column whose newly appended range would
    /// otherwise be the published one).
    /// For a promoted non-nullable `LowCardinality(T)` the result column is `LowCardinality(Nullable(T))`,
    /// which is not the on-disk representation: build the temporary from the on-disk type so the published
    /// substreams are the ones a whole-column reader expects; the promotion happens on the private copy below.
    MutableColumnPtr range_column = on_disk_type ? on_disk_type->createColumn(*nested_serialization) : column.cloneEmpty();

    settings.path.push_back(Substream::NullableElements);
    nested_serialization->deserializeBinaryBulkWithMultipleStreams(*range_column, limit, settings, state, cache);
    settings.path.pop_back();

    size_t new_rows = range_column->size();
    if (new_rows == 0)
        return;

    if (parent_num_read_rows != new_rows)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Number of rows read from the parent null map of `Nullable(Tuple(...))` differs from the number of rows "
            "read for its subcolumn (parent null map rows = {}, subcolumn rows = {})",
            parent_num_read_rows,
            new_rows);

    const auto & parent_null_map_data = assert_cast<const ColumnUInt8 &>(*parent_null_map).getData();
    size_t parent_offset = parent_null_map_data.size() - parent_num_read_rows;

    /// Copy the range into an independent column whose substreams are freshly built by `insertRangeFrom` and
    /// therefore not shared with the substreams cache. Apply the destructive parent null map to that private
    /// copy (for a promoted non-nullable LowCardinality this also promotes it to `LowCardinality(Nullable(T))`),
    /// then append the result, leaving the published `range_column` untouched.
    auto extracted = range_column->cloneEmpty();
    extracted->insertRangeFrom(*range_column, 0, new_rows);
    applyParentNullMapToExtractedSubcolumn(*extracted, parent_null_map_data, /*column_offset=*/ 0, parent_offset);
    column.insertRangeFrom(*extracted, 0, extracted->size());
}

}
