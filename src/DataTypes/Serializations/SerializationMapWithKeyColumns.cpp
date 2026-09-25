#include <DataTypes/Serializations/SerializationMapWithKeyColumns.h>
#include <DataTypes/Serializations/SerializationMapWithKeyColumnsKeysOrValues.h>
#include <DataTypes/Serializations/SerializationMapWithKeyColumnsSize.h>
#include <DataTypes/Serializations/SerializationMapPresence.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeMapHelpers.h>
#include <DataTypes/Serializations/SerializationMap.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <base/EnumReflection.h>

#include <set>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{

const IColumn & extractNestedColumn(const IColumn & column)
{
    return assert_cast<const ColumnMap &>(column).getNestedColumn();
}

IColumn & extractNestedColumn(IColumn & column)
{
    return assert_cast<ColumnMap &>(column).getNestedColumn();
}

} // namespace

void SerializationMapWithKeyColumns::writeManifest(WriteBuffer & ostr, const SerializationPtr & key_serialization, const MapKeyManifest & manifest)
{
    writeBinaryLittleEndian(static_cast<UInt8>(MapKeysInfoVersion::V1), ostr);
    writeBinaryLittleEndian(static_cast<UInt64>(manifest.keys.size()), ostr);

    FormatSettings format_settings;
    for (const auto & entry : manifest.keys)
    {
        key_serialization->serializeBinary(entry.key, ostr, format_settings);
        writeBinaryLittleEndian(static_cast<UInt8>(entry.presence_kind), ostr);
        writeBinaryLittleEndian(static_cast<UInt8>(entry.value_kind), ostr);
    }
}

MapKeyManifest SerializationMapWithKeyColumns::readManifest(ReadBuffer & istr, const SerializationPtr & key_serialization)
{
    MapKeyManifest manifest;
    try
    {
        UInt8 version = 0;
        readBinaryLittleEndian(version, istr);
        if (!magic_enum::enum_cast<MapKeysInfoVersion>(version))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown Map keys info serialization version: {}", static_cast<UInt32>(version));

        UInt64 key_count = 0;
        readBinaryLittleEndian(key_count, istr);

        FormatSettings format_settings;
        manifest.keys.reserve(key_count);
        for (UInt64 i = 0; i < key_count; ++i)
        {
            MapKeyManifestEntry entry;
            key_serialization->deserializeBinary(entry.key, istr, format_settings);

            UInt8 presence_kind = 0;
            readBinaryLittleEndian(presence_kind, istr);
            auto maybe_presence = magic_enum::enum_cast<MapKeyPresenceKind>(presence_kind);
            if (!maybe_presence)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown Map key presence kind: {}", static_cast<UInt32>(presence_kind));
            entry.presence_kind = *maybe_presence;

            UInt8 value_kind = 0;
            readBinaryLittleEndian(value_kind, istr);
            auto maybe_value = magic_enum::enum_cast<MapKeyValueKind>(value_kind);
            if (!maybe_value)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown Map key value kind: {}", static_cast<UInt32>(value_kind));
            entry.value_kind = *maybe_value;

            manifest.keys.push_back(std::move(entry));
        }
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::INCORRECT_DATA)
            throw;
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot read Map keys info: {}", e.message());
    }
    catch (...)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot read Map keys info: truncated or corrupted manifest");
    }

    return manifest;
}

SerializationMapWithKeyColumns::SerializationMapWithKeyColumns(
    const DataTypePtr & key_type_,
    const DataTypePtr & value_type_,
    const SerializationPtr & key_serialization_,
    const SerializationPtr & value_serialization_,
    const SerializationPtr & nested_serialization_)
    : key_type(key_type_)
    , value_type(value_type_)
    , key_serialization(key_serialization_)
    , value_serialization(value_serialization_)
    , nested_serialization(nested_serialization_)
    , basic_map_serialization(SerializationMap::create(
          key_serialization_, value_serialization_, nested_serialization_, MergeTreeMapSerializationVersion::BASIC))
{
}

UInt128 SerializationMapWithKeyColumns::getHash(
    const SerializationPtr & nested_,
    const DataTypePtr & key_type_,
    const DataTypePtr & value_type_)
{
    SipHash hash;
    hash.update("MapWithKeyColumns");
    hash.update(nested_->getHash());
    hash.update(key_type_->getName());
    hash.update(value_type_->getName());
    return hash.get128();
}

SerializationPtr SerializationMapWithKeyColumns::create(
    const DataTypePtr & key_type_,
    const DataTypePtr & value_type_,
    const SerializationPtr & key_serialization_,
    const SerializationPtr & value_serialization_,
    const SerializationPtr & nested_serialization_)
{
    if (!nested_serialization_->supportsPooling())
        return std::shared_ptr<ISerialization>(new SerializationMapWithKeyColumns(
            key_type_, value_type_, key_serialization_, value_serialization_, nested_serialization_));
    return ISerialization::pooled(
        getHash(nested_serialization_, key_type_, value_type_),
        [&] { return new SerializationMapWithKeyColumns(
            key_type_, value_type_, key_serialization_, value_serialization_, nested_serialization_); });
}

const MapKeyManifest & SerializationMapWithKeyColumns::getManifestFromState(const DeserializeBinaryBulkStatePtr & state) const
{
    return checkAndGetState<DeserializeBinaryBulkStateMapWithKeyColumns>(state)->manifest;
}

MapKeyManifest SerializationMapWithKeyColumns::collectManifestFromColumn(const IColumn & column)
{
    const auto & column_map = assert_cast<const ColumnMap &>(column);
    std::set<Field> unique_keys;

    if (const auto & stats = column_map.getStatistics())
    {
        for (const auto & key : stats->keys)
            unique_keys.insert(key);
    }

    const auto & keys_column = column_map.getNestedData().getColumn(0);
    for (size_t i = 0; i < keys_column.size(); ++i)
        unique_keys.insert(keys_column[i]);

    MapKeyManifest manifest;
    manifest.keys.reserve(unique_keys.size());
    for (const auto & key : unique_keys)
        manifest.keys.push_back(MapKeyManifestEntry{.key = key});
    return manifest;
}

std::vector<Field> SerializationMapWithKeyColumns::keysFromManifest(const MapKeyManifest & manifest)
{
    std::vector<Field> keys;
    keys.reserve(manifest.keys.size());
    for (const auto & entry : manifest.keys)
        keys.push_back(entry.key);
    return keys;
}

std::vector<SerializationMapWithKeyColumns::PivotedKeyColumn> SerializationMapWithKeyColumns::pivot(
    const IColumn & column,
    const DataTypePtr & key_type,
    const DataTypePtr & value_type,
    const std::vector<Field> & keys,
    size_t offset,
    size_t limit)
{
    const auto & column_map = assert_cast<const ColumnMap &>(column);
    const auto & nested_array = column_map.getNestedColumn();
    const auto & keys_column = column_map.getNestedData().getColumn(0);
    const auto & offsets = nested_array.getOffsets();
    const size_t column_size = column_map.size();

    if (offset > column_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pivot offset {} is greater than column size {}", offset, column_size);

    const size_t rows = limit && offset + limit < column_size ? limit : column_size - offset;

    std::vector<PivotedKeyColumn> result;
    result.reserve(keys.size());

    for (const auto & key : keys)
    {
        PivotedKeyColumn pivoted;
        pivoted.key = key;
        pivoted.values = value_type->createColumn();
        pivoted.presence.assign(rows, 0);

        auto key_column = key_type->createColumn();
        key_column->insert(key);
        extractKeyValueFromMap(*column_map.getNestedColumnPtr(), *key_column, *pivoted.values, offset, offset + rows);

        for (size_t row = 0; row < rows; ++row)
        {
            const size_t begin = offsets[static_cast<ssize_t>(offset + row) - 1];
            const size_t end = offsets[offset + row];
            for (size_t pos = begin; pos < end; ++pos)
            {
                if (keys_column.compareAt(pos, 0, *key_column, 0) == 0)
                {
                    pivoted.presence[row] = 1;
                    break;
                }
            }
        }

        result.push_back(std::move(pivoted));
    }

    return result;
}

String SerializationMapWithKeyColumns::keyToStreamName(const Field & key) const
{
    auto column = key_type->createColumn();
    column->insert(key);
    WriteBufferFromOwnString buf;
    key_serialization->serializeText(*column, 0, buf, {});
    return buf.str();
}

void SerializationMapWithKeyColumns::addMapKeyPath(SerializeBinaryBulkSettings & settings, const Field & key) const
{
    settings.path.push_back(Substream::MapKey);
    settings.path.back().name_of_substream = keyToStreamName(key);
}

void SerializationMapWithKeyColumns::addMapKeyPath(DeserializeBinaryBulkSettings & settings, const Field & key) const
{
    settings.path.push_back(Substream::MapKey);
    settings.path.back().name_of_substream = keyToStreamName(key);
}

void SerializationMapWithKeyColumns::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    const auto * with_key_columns_state = data.deserialize_state ? checkAndGetState<DeserializeBinaryBulkStateMapWithKeyColumns>(data.deserialize_state) : nullptr;
    MapKeyManifest manifest;
    if (with_key_columns_state)
        manifest = with_key_columns_state->manifest;
    else if (data.column)
        manifest = collectManifestFromColumn(*data.column);

    settings.path.push_back(Substream::MapKeysInfo);
    callback(settings.path);
    settings.path.pop_back();

    if (settings.enumerate_virtual_streams)
    {
        settings.path.push_back(Substream::ArraySizes);
        auto self = SerializationMapWithKeyColumns::create(key_type, value_type, key_serialization, value_serialization, nested_serialization);
        settings.path.back().data = SubstreamData(SerializationMapWithKeyColumnsSize::create(self))
            .withType(std::make_shared<DataTypeUInt64>());
        callback(settings.path);
        settings.path.pop_back();

        settings.path.push_back(Substream::ArrayElements);
        ++settings.array_level;

        settings.path.push_back(Substream::TupleElement);
        settings.path.back().name_of_substream = "keys";
        settings.path.back().data = SubstreamData(SerializationMapWithKeyColumnsKeysOrValues::create(self, true))
            .withType(std::make_shared<DataTypeArray>(key_type));
        callback(settings.path);
        settings.path.pop_back();

        settings.path.push_back(Substream::TupleElement);
        settings.path.back().name_of_substream = "values";
        settings.path.back().data = SubstreamData(SerializationMapWithKeyColumnsKeysOrValues::create(self, false))
            .withType(std::make_shared<DataTypeArray>(value_type));
        callback(settings.path);
        settings.path.pop_back();

        --settings.array_level;
        settings.path.pop_back();
    }

    for (const auto & entry : manifest.keys)
    {
        const auto key_name = keyToStreamName(entry.key);

        settings.path.push_back(Substream::MapKey);
        settings.path.back().name_of_substream = key_name;
        auto value_data = SubstreamData(value_serialization).withType(value_type);
        value_serialization->enumerateStreams(settings, callback, value_data);
        settings.path.pop_back();
    }

    /// Presence is one shared physical stream. Logical `exists_<text>` names are
    /// produced by `SerializationMapKeyPresence`, not by the full-column layout.
    if (!manifest.keys.empty())
    {
        settings.path.push_back(Substream::MapKeyPresence);
        callback(settings.path);
        settings.path.pop_back();
    }
}

void SerializationMapWithKeyColumns::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto with_key_columns_state = std::make_shared<SerializeBinaryBulkStateMapWithKeyColumns>();
    with_key_columns_state->manifest = collectManifestFromColumn(column);

    settings.path.push_back(Substream::MapKeysInfo);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing stream for Map keys info during serialization of binary bulk state prefix");

    writeManifest(*stream, key_serialization, with_key_columns_state->manifest);

    const auto keys = keysFromManifest(with_key_columns_state->manifest);
    auto pivoted = pivot(column, key_type, value_type, keys);

    with_key_columns_state->value_states.resize(keys.size());
    for (size_t i = 0; i < keys.size(); ++i)
    {
        addMapKeyPath(settings, keys[i]);
        value_serialization->serializeBinaryBulkStatePrefix(*pivoted[i].values, settings, with_key_columns_state->value_states[i]);
        settings.path.pop_back();
    }

    state = std::move(with_key_columns_state);
}

void SerializationMapWithKeyColumns::flushPendingPresence(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStateMapWithKeyColumns & state) const
{
    if (state.manifest.keys.empty())
    {
        state.pending_presence.clear();
        state.pending_rows = 0;
        return;
    }

    settings.path.push_back(Substream::MapKeyPresence);
    auto * presence_stream = settings.getter(settings.path);
    settings.path.pop_back();

    if (!presence_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing stream for Map key presence during serialization");

    /// A 0-row call still goes through the getter so Compact/Wide stream
    /// discovery records `key_presence`. Do not emit an empty block: suffix
    /// would otherwise write a second one after a completed granule.
    if (state.pending_rows > 0)
        MapKeyPresenceBlock::serialize(*presence_stream, state.pending_rows, state.pending_presence);

    state.pending_presence.clear();
    state.pending_rows = 0;
}

void SerializationMapWithKeyColumns::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    if (!state)
        return;

    auto * with_key_columns_state = checkAndGetState<SerializeBinaryBulkStateMapWithKeyColumns>(state);
    for (size_t i = 0; i < with_key_columns_state->manifest.keys.size(); ++i)
    {
        addMapKeyPath(settings, with_key_columns_state->manifest.keys[i].key);
        value_serialization->serializeBinaryBulkStateSuffix(settings, with_key_columns_state->value_states[i]);
        settings.path.pop_back();
    }

    if (with_key_columns_state->pending_rows > 0)
        flushPendingPresence(settings, *with_key_columns_state);
}

void SerializationMapWithKeyColumns::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    settings.path.push_back(Substream::MapKeysInfo);

    if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, settings.path))
    {
        state = std::move(cached_state);
        settings.path.pop_back();
        return;
    }

    auto * stream = settings.getter(settings.path);
    if (!stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Missing stream for Map keys info");

    auto with_key_columns_state = std::make_shared<DeserializeBinaryBulkStateMapWithKeyColumns>();
    with_key_columns_state->manifest = readManifest(*stream, key_serialization);
    settings.path.pop_back();

    with_key_columns_state->value_states.resize(with_key_columns_state->manifest.keys.size());
    for (size_t i = 0; i < with_key_columns_state->manifest.keys.size(); ++i)
    {
        addMapKeyPath(settings, with_key_columns_state->manifest.keys[i].key);
        value_serialization->deserializeBinaryBulkStatePrefix(settings, with_key_columns_state->value_states[i], cache);
        settings.path.pop_back();
    }

    settings.path.push_back(Substream::MapKeysInfo);
    addToSubstreamsDeserializeStatesCache(cache, settings.path, with_key_columns_state);
    settings.path.pop_back();
    state = std::move(with_key_columns_state);
}

void SerializationMapWithKeyColumns::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * with_key_columns_state = checkAndGetState<SerializeBinaryBulkStateMapWithKeyColumns>(state);
    const auto keys = keysFromManifest(with_key_columns_state->manifest);
    auto pivoted = pivot(column, key_type, value_type, keys, offset, limit);
    const size_t rows = keys.empty() ? 0 : pivoted.front().presence.size();

    for (size_t i = 0; i < pivoted.size(); ++i)
    {
        addMapKeyPath(settings, pivoted[i].key);
        value_serialization->serializeBinaryBulkWithMultipleStreams(
            *pivoted[i].values, 0, pivoted[i].values->size(), settings, with_key_columns_state->value_states[i]);
        settings.path.pop_back();
    }

    if (keys.empty())
        return;

    if (with_key_columns_state->pending_presence.empty())
        with_key_columns_state->pending_presence.resize(pivoted.size());
    else if (with_key_columns_state->pending_presence.size() != pivoted.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Map key presence key count changed from {} to {} inside a granule",
            with_key_columns_state->pending_presence.size(),
            pivoted.size());

    for (size_t i = 0; i < pivoted.size(); ++i)
        with_key_columns_state->pending_presence[i].insert(
            with_key_columns_state->pending_presence[i].end(),
            pivoted[i].presence.begin(),
            pivoted[i].presence.end());
    with_key_columns_state->pending_rows += rows;

    if (settings.granule_is_complete)
        flushPendingPresence(settings, *with_key_columns_state);
}

void SerializationMapWithKeyColumns::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    if (!state)
        return;

    auto * with_key_columns_state = checkAndGetState<DeserializeBinaryBulkStateMapWithKeyColumns>(state);
    auto & column_map = assert_cast<ColumnMap &>(column);
    const size_t key_count = with_key_columns_state->manifest.keys.size();

    /// Compact `.mrk4` keeps each key / presence stream in its own compressed
    /// block. The Compact full-column getter only seeks for subcolumns, so the
    /// nested value serialization (e.g. String size + data) must seek itself.
    auto original_getter = settings.getter;
    if (settings.seek_stream_to_current_mark_callback)
    {
        settings.getter = [&](const SubstreamPath & path) -> ReadBuffer *
        {
            settings.seek_stream_to_current_mark_callback(path);
            return original_getter(path);
        };
    }

    std::vector<std::vector<UInt8>> presence;
    if (key_count != 0)
    {
        settings.path.push_back(Substream::MapKeyPresence);
        auto * presence_stream = settings.getter(settings.path);
        settings.path.pop_back();

        if (!presence_stream)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Missing stream for Map key presence");

        MapKeyPresenceBlock::deserializeAll(*presence_stream, limit, key_count, presence);
    }

    std::vector<MutableColumnPtr> value_columns(key_count);
    for (size_t i = 0; i < key_count; ++i)
    {
        addMapKeyPath(settings, with_key_columns_state->manifest.keys[i].key);
        value_columns[i] = value_type->createColumn();
        value_serialization->deserializeBinaryBulkWithMultipleStreams(
            *value_columns[i], limit, settings, with_key_columns_state->value_states[i], cache);
        settings.path.pop_back();
    }

    settings.getter = std::move(original_getter);

    size_t rows = limit;
    if (!presence.empty())
        rows = presence.front().size();

    for (size_t i = 0; i < key_count; ++i)
    {
        if (value_columns[i]->size() != rows)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Map key value stream row count {} does not match presence rows {}",
                value_columns[i]->size(),
                rows);
    }

    auto & nested_column = column_map.getNestedColumn();
    auto & nested_data = column_map.getNestedData();
    auto & keys_column = nested_data.getColumn(0);
    auto & values_column = nested_data.getColumn(1);
    auto & offsets = nested_column.getOffsets();

    std::vector<MutableColumnPtr> key_holders(key_count);
    for (size_t i = 0; i < key_count; ++i)
    {
        key_holders[i] = key_type->createColumn();
        key_holders[i]->insert(with_key_columns_state->manifest.keys[i].key);
    }

    size_t total_pairs = 0;
    for (size_t i = 0; i < key_count; ++i)
    {
        for (UInt8 bit : presence[i])
            total_pairs += bit;
    }

    keys_column.reserve(keys_column.size() + total_pairs);
    values_column.reserve(values_column.size() + total_pairs);
    offsets.reserve(offsets.size() + rows);

    for (size_t row = 0; row < rows; ++row)
    {
        for (size_t i = 0; i < key_count; ++i)
        {
            if (!presence[i][row])
                continue;
            keys_column.insertFrom(*key_holders[i], 0);
            values_column.insertFrom(*value_columns[i], row);
        }
        offsets.push_back(keys_column.size());
    }
}

void SerializationMapWithKeyColumns::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    basic_map_serialization->serializeBinary(field, ostr, settings);
}

void SerializationMapWithKeyColumns::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    basic_map_serialization->deserializeBinary(field, istr, settings);
}

void SerializationMapWithKeyColumns::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested_serialization->serializeBinary(extractNestedColumn(column), row_num, ostr, settings);
}

void SerializationMapWithKeyColumns::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested_serialization->deserializeBinary(extractNestedColumn(column), istr, settings);
}

void SerializationMapWithKeyColumns::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    basic_map_serialization->serializeText(column, row_num, ostr, settings);
}

void SerializationMapWithKeyColumns::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    if (whole)
        basic_map_serialization->deserializeWholeText(column, istr, settings);
    else
        basic_map_serialization->deserializeTextEscaped(column, istr, settings);
}

bool SerializationMapWithKeyColumns::tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    if (whole)
        return basic_map_serialization->tryDeserializeWholeText(column, istr, settings);
    return basic_map_serialization->tryDeserializeTextEscaped(column, istr, settings);
}

}
