#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
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
        SerializationNumber<UInt8>::create()->deserializeBinaryBulk(*mutable_parent_null_map, *stream, rows_offset, limit, 0);
        parent_null_map = std::move(mutable_parent_null_map);
        parent_num_read_rows = parent_null_map->size();
        addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, parent_null_map, parent_num_read_rows);
    }
    settings.path.pop_back();

    size_t prev_size = column->size();

    /// The nested column (or some of its substreams) may already be in the substreams cache from
    /// deserialization of another subcolumn, and a cached column can contain rows from multiple ranges.
    /// Force copying only the rows of the current range from the cache, so that the number of rows appended
    /// to `column` is exactly the size of the current range and the null representation modified below is
    /// owned by `column` rather than shared with a column produced by another subcolumn read.
    auto nested_settings = settings;
    nested_settings.insert_only_rows_in_current_range_from_substreams_cache = true;
    nested_settings.path.push_back(Substream::NullableElements);
    nested_serialization->deserializeBinaryBulkWithMultipleStreams(column, rows_offset, limit, nested_settings, state, cache);

    size_t new_rows = column->size() - prev_size;
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

    auto mutable_column = IColumn::mutate(std::move(column));
    applyParentNullMapToExtractedSubcolumn(mutable_column, parent_null_map_data, prev_size, parent_offset);
    column = std::move(mutable_column);
}

}
