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
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
}

struct DeserializeBinaryBulkStateMapWithKeyColumnsSize : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr with_key_columns_state;
    /// Own per-key exists states so we never advance the shared map state's value
    /// streams (that would corrupt sibling `m['k']` reads that share the cached map state).
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> exists_states;

    ISerialization::DeserializeBinaryBulkStatePtr cloneImpl(
        ISerialization::DeserializeBinaryBulkStateCloneContext & context) const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateMapWithKeyColumnsSize>(*this);
        new_state->with_key_columns_state = with_key_columns_state ? with_key_columns_state->clone(context) : nullptr;
        for (auto & s : new_state->exists_states)
            s = s ? s->clone(context) : nullptr;
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
    /// `length(m)` sums presence bits. Value streams are not read, so they must not be
    /// enumerated: MergeTree would open every `m.key_*` file before the first row.
    const auto & key_columns = assert_cast<const SerializationMapWithKeyColumns &>(*map_with_key_columns_serialization);

    MapKeyManifest manifest;
    if (data.deserialize_state)
    {
        const auto * size_state = checkAndGetState<DeserializeBinaryBulkStateMapWithKeyColumnsSize>(data.deserialize_state);
        manifest = key_columns.getManifestFromState(size_state->with_key_columns_state);
    }
    else if (settings.map_key_columns_manifest)
        manifest = *settings.map_key_columns_manifest;

    for (const auto & entry : manifest.keys)
    {
        if (entry.presence_kind != MapKeyPresenceKind::Tracked)
            continue;

        settings.path.push_back(SerializationMapWithKeyColumns::makeKeySubstream(
            ISerialization::Substream::MapKeyExists,
            key_columns.keyToStreamName(entry.key),
            manifest.stream_name_version));
        auto exists_data = SubstreamData(key_columns.getExistsSerialization()).withType(std::make_shared<DataTypeUInt8>());
        key_columns.getExistsSerialization()->enumerateStreams(settings, callback, exists_data);
        settings.path.pop_back();
    }
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

    /// `length(m)` loads the manifest and opens only `.exists_` streams. The full map prefix
    /// would prepare every key's value state and open those files.
    settings.path.push_back(ISerialization::Substream::MapKeysInfo);
    MapKeyManifest manifest;
    if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, settings.path))
    {
        if (const auto * cached = dynamic_cast<const SerializationMapWithKeyColumns::DeserializeBinaryBulkStateMapWithKeyColumns *>(cached_state.get()))
            manifest = cached->manifest;
        size_state->with_key_columns_state = std::move(cached_state);
    }
    else
    {
        if (!settings.map_key_columns_manifest)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Missing key_columns manifest for with_key_columns Map");

        manifest = *settings.map_key_columns_manifest;
        auto stored = std::make_shared<SerializationMapWithKeyColumns::DeserializeBinaryBulkStateMapWithKeyColumns>();
        stored->manifest = manifest;
        addToSubstreamsDeserializeStatesCache(cache, settings.path, stored);
        size_state->with_key_columns_state = std::move(stored);
    }
    settings.path.pop_back();

    size_state->exists_states.resize(manifest.keys.size());
    for (size_t i = 0; i < manifest.keys.size(); ++i)
    {
        const auto & entry = manifest.keys[i];
        if (entry.presence_kind == MapKeyPresenceKind::AlwaysPresent)
            continue;

        settings.path.push_back(SerializationMapWithKeyColumns::makeKeySubstream(
            ISerialization::Substream::MapKeyExists,
            key_columns.keyToStreamName(entry.key),
            manifest.stream_name_version));
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
    struct ExistsRead
    {
        bool always_present = false;
        ColumnUInt8::Container bits;
    };
    std::vector<ExistsRead> reads;
    reads.reserve(manifest.keys.size());

    for (size_t i = 0; i < manifest.keys.size(); ++i)
    {
        const auto & entry = manifest.keys[i];
        if (entry.presence_kind == MapKeyPresenceKind::AlwaysPresent)
        {
            ExistsRead read;
            read.always_present = true;
            reads.push_back(std::move(read));
            continue;
        }

        auto exists_column = ColumnUInt8::create();
        settings.path.push_back(SerializationMapWithKeyColumns::makeKeySubstream(
            ISerialization::Substream::MapKeyExists,
            key_columns.keyToStreamName(entry.key),
            manifest.stream_name_version));
        if (!insertDataFromSubstreamsCacheIfAny(cache, settings, *exists_column))
        {
            key_columns.getExistsSerialization()->deserializeBinaryBulkWithMultipleStreams(
                *exists_column, limit, settings, size_state->exists_states[i], cache);
            addColumnWithNumReadRowsToSubstreamsCache(
                cache, settings.path, exists_column->getPtr(), exists_column->size());
        }
        settings.path.pop_back();
        /// Copy: the column may already be shared through the substreams cache.
        ExistsRead read;
        const auto & stored = exists_column->getData();
        read.bits.assign(stored.begin(), stored.end());
        reads.push_back(std::move(read));
    }

    /// `AlwaysPresent` has no stream. A tracked stream's length is the row count; every other
    /// stream must match it. With no tracked stream, `limit` is the only count available.
    size_t rows = limit;
    bool saw_tracked = false;
    for (const auto & read : reads)
    {
        if (read.always_present)
            continue;

        if (!saw_tracked)
        {
            rows = read.bits.size();
            saw_tracked = true;
        }
        else if (read.bits.size() != rows)
        {
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Map key presence row count mismatch: {} and {}",
                read.bits.size(),
                rows);
        }
    }

    data.resize_fill(start + rows, 0);
    for (const auto & read : reads)
    {
        if (read.always_present)
        {
            for (size_t row = 0; row < rows; ++row)
                data[start + row] += 1;
        }
        else
        {
            for (size_t row = 0; row < rows; ++row)
                data[start + row] += read.bits[row];
        }
    }
}

}
