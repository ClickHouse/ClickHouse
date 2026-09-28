#include <DataTypes/Serializations/SerializationMapWithKeyColumnsSize.h>
#include <DataTypes/Serializations/SerializationMapWithKeyColumns.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

struct DeserializeBinaryBulkStateMapWithKeyColumnsSize : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr with_key_columns_state;
    /// Own per-key exists states so we never advance the shared map state's value
    /// streams (that would corrupt sibling `m['k']` reads that share the cached map state).
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> exists_states;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateMapWithKeyColumnsSize>(*this);
        new_state->with_key_columns_state = with_key_columns_state ? with_key_columns_state->clone() : nullptr;
        for (auto & s : new_state->exists_states)
            s = s ? s->clone() : nullptr;
        return new_state;
    }
};

void SerializationMapWithKeyColumnsSize::throwNoSerialization()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Text/binary serialization is not implemented for Map with_key_columns size subcolumn");
}

SerializationMapWithKeyColumnsSize::SerializationMapWithKeyColumnsSize(const SerializationPtr & map_with_key_columns_serialization_)
    : map_with_key_columns_serialization(map_with_key_columns_serialization_)
{
}

SerializationPtr SerializationMapWithKeyColumnsSize::create(const SerializationPtr & map_with_key_columns_serialization_)
{
    return std::shared_ptr<ISerialization>(new SerializationMapWithKeyColumnsSize(map_with_key_columns_serialization_));
}

void SerializationMapWithKeyColumnsSize::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    /// `length(m)` is derived from the full per-key layout (one presence bit per key),
    /// so enumerate exactly what that read touches. Unwrap our own deserialize state into
    /// the underlying map state so the map serialization sees the type it expects.
    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);

    SubstreamData map_data(map_with_key_columns_serialization);
    if (data.deserialize_state)
    {
        const auto * size_state = checkAndGetState<DeserializeBinaryBulkStateMapWithKeyColumnsSize>(data.deserialize_state);
        map_data.deserialize_state = size_state->with_key_columns_state;
    }
    key_columns.enumerateStreams(settings, callback, map_data);
}

void SerializationMapWithKeyColumnsSize::serializeBinaryBulkStatePrefix(
    const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationMapWithKeyColumnsSize");
}

void SerializationMapWithKeyColumnsSize::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationMapWithKeyColumnsSize");
}

void SerializationMapWithKeyColumnsSize::serializeBinaryBulkWithMultipleStreams(
    const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationMapWithKeyColumnsSize");
}

void SerializationMapWithKeyColumnsSize::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    auto size_state = std::make_shared<DeserializeBinaryBulkStateMapWithKeyColumnsSize>();
    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);

    /// Read the manifest (shared, cached) but prepare our OWN exists/overflow
    /// states so we never touch the value streams.
    map_with_key_columns_serialization->deserializeBinaryBulkStatePrefix(settings, size_state->with_key_columns_state, cache);
    const auto & manifest = key_columns.getManifestFromState(size_state->with_key_columns_state);

    size_state->exists_states.resize(manifest.keys.size());
    for (size_t i = 0; i < manifest.keys.size(); ++i)
    {
        const auto & entry = manifest.keys[i];
        if (entry.presence_kind == MapKeyPresenceKind::AlwaysPresent)
            continue;

        settings.path.push_back(ISerialization::Substream::MapKeyExists);
        settings.path.back().name_of_substream = key_columns.keyToStreamName(entry.key);
        key_columns.getExistsSerialization()->deserializeBinaryBulkStatePrefix(settings, size_state->exists_states[i], cache);
        settings.path.pop_back();
    }

    state = std::move(size_state);
}

void SerializationMapWithKeyColumnsSize::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto * size_state = checkAndGetState<DeserializeBinaryBulkStateMapWithKeyColumnsSize>(state);
    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);
    const auto & manifest = key_columns.getManifestFromState(size_state->with_key_columns_state);
    auto & data = assert_cast<ColumnUInt64 &>(column).getData();

    const size_t start = data.size();
    /// Sum presence bits per row across dedicated keys, reading only `.exists_`
    /// streams. This never advances value streams, so it is safe to co-read with
    /// `m['k']` subcolumns that share the cached map state.
    bool sized = false;
    size_t rows = limit;
    for (size_t i = 0; i < manifest.keys.size(); ++i)
    {
        const auto & entry = manifest.keys[i];

        if (entry.presence_kind == MapKeyPresenceKind::AlwaysPresent)
        {
            if (!sized)
            {
                data.resize_fill(start + limit, 0);
                sized = true;
            }
            for (size_t row = 0; row < limit; ++row)
                data[start + row] += 1;
            continue;
        }

        auto exists_column = ColumnUInt8::create();
        settings.path.push_back(ISerialization::Substream::MapKeyExists);
        settings.path.back().name_of_substream = key_columns.keyToStreamName(entry.key);
        if (!insertDataFromSubstreamsCacheIfAny(cache, settings, *exists_column))
        {
            key_columns.getExistsSerialization()->deserializeBinaryBulkWithMultipleStreams(
                *exists_column, limit, settings, size_state->exists_states[i], cache);
            addColumnWithNumReadRowsToSubstreamsCache(
                cache, settings.path, exists_column->getPtr(), exists_column->size());
        }
        settings.path.pop_back();

        const auto & bits = exists_column->getData();
        rows = bits.size();
        if (!sized)
        {
            data.resize_fill(start + rows, 0);
            sized = true;
        }
        for (size_t row = 0; row < rows; ++row)
            data[start + row] += bits[row];
    }

    if (!sized)
        data.resize_fill(start + limit, 0);
}

}
