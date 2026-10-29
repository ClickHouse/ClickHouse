#include <DataTypes/Serializations/SerializationMapKeyPresenceMerge.h>
#include <DataTypes/DataTypeMap.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

void SerializationMapKeyPresenceMerge::throwNoSerialization()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Text/binary serialization is not implemented for Map key presence merge");
}

SerializationMapKeyPresenceMerge::SerializationMapKeyPresenceMerge(const SerializationPtr & map_with_key_columns_serialization_)
    : map_with_key_columns_serialization(map_with_key_columns_serialization_)
{
}

SerializationPtr SerializationMapKeyPresenceMerge::create(const SerializationPtr & map_with_key_columns_serialization_)
{
    return std::shared_ptr<ISerialization>(new SerializationMapKeyPresenceMerge(map_with_key_columns_serialization_));
}

void SerializationMapKeyPresenceMerge::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    /// `m.keys_presence` is reconstructed from the full per-key layout, so it
    /// touches the same streams as a full-column read. Unwrap our deserialize
    /// state into the underlying map state so the map serialization sees the
    /// type it expects.
    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);

    SubstreamData map_data(map_with_key_columns_serialization);
    if (data.deserialize_state)
    {
        const auto * presence_state = checkAndGetState<DeserializeState>(data.deserialize_state);
        map_data.deserialize_state = presence_state->map_state;
    }
    key_columns.enumerateStreams(settings, callback, map_data);
}

void SerializationMapKeyPresenceMerge::serializeBinaryBulkStatePrefix(
    const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationMapKeyPresenceMerge");
}

void SerializationMapKeyPresenceMerge::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationMapKeyPresenceMerge");
}

void SerializationMapKeyPresenceMerge::serializeBinaryBulkWithMultipleStreams(
    const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationMapKeyPresenceMerge");
}

void SerializationMapKeyPresenceMerge::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    auto presence_state = std::make_shared<DeserializeState>();
    map_with_key_columns_serialization->deserializeBinaryBulkStatePrefix(settings, presence_state->map_state, cache);
    state = std::move(presence_state);
}

void SerializationMapKeyPresenceMerge::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto * presence_state = checkAndGetState<DeserializeState>(state);
    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);

    /// Reconstruct the full map and emit one presence bit per manifest key per
    /// row, in manifest order. Shares the correct, mark-aligned per-key read path.
    const auto & manifest = key_columns.getManifestFromState(presence_state->map_state);
    auto map_column = DataTypeMap(key_columns.getKeyType(), key_columns.getValueType()).createColumn();
    map_with_key_columns_serialization->deserializeBinaryBulkWithMultipleStreams(
        *map_column, limit, settings, presence_state->map_state, cache);

    auto & array = assert_cast<ColumnArray &>(column);
    auto & nested = assert_cast<ColumnUInt8 &>(array.getData());
    auto & offsets = array.getOffsets();

    const auto & map = assert_cast<const ColumnMap &>(*map_column);
    const auto & map_offsets = map.getNestedColumn().getOffsets();
    const auto & map_keys = map.getNestedData().getColumn(0);
    const size_t rows = map_offsets.size();
    const size_t key_count = manifest.keys.size();

    std::vector<Field> keys;
    keys.reserve(key_count);
    for (const auto & entry : manifest.keys)
        keys.push_back(entry.key);

    nested.getData().reserve(nested.size() + rows * key_count);
    offsets.reserve(offsets.size() + rows);
    for (size_t row = 0; row < rows; ++row)
    {
        const size_t begin = map_offsets[static_cast<ssize_t>(row) - 1];
        const size_t end = map_offsets[row];
        for (const auto & key : keys)
        {
            UInt8 present = 0;
            for (size_t pos = begin; pos < end; ++pos)
            {
                if (map_keys[pos] == key)
                {
                    present = 1;
                    break;
                }
            }
            nested.getData().push_back(present);
        }
        offsets.push_back(nested.size());
    }
}

}
