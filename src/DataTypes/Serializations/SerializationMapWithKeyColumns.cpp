#include <DataTypes/Serializations/SerializationMapWithKeyColumns.h>
#include <DataTypes/Serializations/SerializationMapWithKeyColumnsKeysOrValues.h>
#include <DataTypes/Serializations/SerializationMapWithKeyColumnsSize.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/FieldVisitorToString.h>
#include <DataTypes/DataTypeMapHelpers.h>
#include <DataTypes/Serializations/SerializationMap.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <base/EnumReflection.h>

#include <limits>
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

}

namespace
{

constexpr UInt64 KEY_COLUMNS_TEXT_VERSION = 1;

MapKeyManifest readKeyColumnsTextImpl(ReadBuffer & istr, const DataTypePtr & key_type)
{
    UInt64 version = 0;
    readText(version, istr);
    assertChar('\n', istr);
    if (version != KEY_COLUMNS_TEXT_VERSION)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown key_columns.txt version {}", version);

    UInt64 key_count = 0;
    readText(key_count, istr);
    assertChar('\n', istr);

    auto key_serialization = key_type->getDefaultSerialization();
    FormatSettings format_settings;
    MapKeyManifest manifest;
    manifest.keys.reserve(key_count);
    for (UInt64 i = 0; i < key_count; ++i)
    {
        UInt64 presence_kind = 0;
        readText(presence_kind, istr);
        assertChar('\t', istr);

        auto kind = magic_enum::enum_cast<MapKeyPresenceKind>(static_cast<UInt8>(presence_kind));
        if (!kind || presence_kind > std::numeric_limits<UInt8>::max())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown Map key presence kind {} in key_columns.txt", presence_kind);

        auto key_column = key_type->createColumn();
        key_serialization->deserializeTextEscaped(*key_column, istr, format_settings);
        assertChar('\n', istr);

        manifest.keys.push_back(MapKeyManifestEntry{.key = (*key_column)[0], .presence_kind = *kind});
    }

    if (!istr.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected trailing data in key_columns.txt");

    return manifest;
}

}

void SerializationMapWithKeyColumns::writeKeyColumnsText(WriteBuffer & ostr, const DataTypePtr & key_type, const MapKeyManifest & manifest)
{
    writeText(KEY_COLUMNS_TEXT_VERSION, ostr);
    writeChar('\n', ostr);
    writeText(manifest.keys.size(), ostr);
    writeChar('\n', ostr);

    auto key_serialization = key_type->getDefaultSerialization();
    FormatSettings format_settings;
    for (const auto & entry : manifest.keys)
    {
        writeText(static_cast<UInt8>(entry.presence_kind), ostr);
        writeChar('\t', ostr);

        auto key_column = key_type->createColumn();
        key_column->insert(entry.key);
        key_serialization->serializeTextEscaped(*key_column, 0, ostr, format_settings);
        writeChar('\n', ostr);
    }
}

MapKeyManifest SerializationMapWithKeyColumns::readKeyColumnsText(ReadBuffer & istr, const DataTypePtr & key_type)
{
    try
    {
        return readKeyColumnsTextImpl(istr, key_type);
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::INCORRECT_DATA)
            throw;
        throw Exception(ErrorCodes::INCORRECT_DATA, "Corrupt key_columns.txt: {}", e.message());
    }
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
    , exists_serialization(std::make_shared<DataTypeUInt8>()->getDefaultSerialization())
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

namespace
{

/// Distinct keys of `column` (a `ColumnMap`) that are not in `known`, in first-seen order.
std::vector<Field> collectFirstSeenKeys(const IColumn & column, const std::set<Field> & known)
{
    const auto & column_map = assert_cast<const ColumnMap &>(column);
    const auto & keys_column = column_map.getNestedData().getColumn(0);

    std::vector<Field> new_keys;
    std::set<Field> seen = known;
    for (size_t i = 0; i < keys_column.size(); ++i)
    {
        Field key = keys_column[i];
        if (seen.insert(key).second)
            new_keys.push_back(std::move(key));
    }
    return new_keys;
}

}

std::vector<Field> SerializationMapWithKeyColumns::collectNewKeys(const IColumn & column, const SerializeBinaryBulkState & state) const
{
    const auto & map_state = typeid_cast<const SerializeBinaryBulkStateMapWithKeyColumns &>(state);
    std::set<Field> known;
    for (const auto & key_state : map_state.keys)
        known.insert(key_state.key);
    return collectFirstSeenKeys(column, known);
}

std::vector<Field> SerializationMapWithKeyColumns::collectAllKeys(const IColumn & column) const
{
    return collectFirstSeenKeys(column, {});
}

void SerializationMapWithKeyColumns::addKeys(SerializeBinaryBulkStatePtr & state, const std::vector<Field> & keys) const
{
    auto * map_state = checkAndGetState<SerializeBinaryBulkStateMapWithKeyColumns>(state);
    for (const auto & key : keys)
    {
        if (map_state->key_index.contains(key))
            continue;
        KeyWriteState key_state;
        key_state.key = key;
        map_state->key_index.emplace(key, map_state->keys.size());
        map_state->keys.push_back(std::move(key_state));
    }
}

void SerializationMapWithKeyColumns::markKeysCopiedFromTemplate(SerializeBinaryBulkStatePtr & state, const std::vector<Field> & keys) const
{
    auto * map_state = checkAndGetState<SerializeBinaryBulkStateMapWithKeyColumns>(state);
    for (const auto & key : keys)
        map_state->copied_from_template.insert(key);
}

std::vector<Field> SerializationMapWithKeyColumns::getRegisteredKeys(const SerializeBinaryBulkState & state) const
{
    const auto & map_state = typeid_cast<const SerializeBinaryBulkStateMapWithKeyColumns &>(state);
    std::vector<Field> keys;
    keys.reserve(map_state.keys.size());
    for (const auto & key_state : map_state.keys)
        keys.push_back(key_state.key);
    return keys;
}

size_t SerializationMapWithKeyColumns::getRegisteredKeyCount(const SerializeBinaryBulkState & state) const
{
    return typeid_cast<const SerializeBinaryBulkStateMapWithKeyColumns &>(state).keys.size();
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

void SerializationMapWithKeyColumns::addMapExistsPath(SerializeBinaryBulkSettings & settings, const Field & key) const
{
    settings.path.push_back(Substream::MapKeyExists);
    settings.path.back().name_of_substream = keyToStreamName(key);
}

void SerializationMapWithKeyColumns::addMapExistsPath(DeserializeBinaryBulkSettings & settings, const Field & key) const
{
    settings.path.push_back(Substream::MapKeyExists);
    settings.path.back().name_of_substream = keyToStreamName(key);
}

void SerializationMapWithKeyColumns::enumerateKeyStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData &,
    const Field & key) const
{
    const auto key_name = keyToStreamName(key);

    settings.path.push_back(Substream::MapKey);
    settings.path.back().name_of_substream = key_name;
    /// The sample column is a `ColumnMap`. `V` enumerates from its own type.
    auto value_data = SubstreamData(value_serialization).withType(value_type);
    value_serialization->enumerateStreams(settings, callback, value_data);
    settings.path.pop_back();

    settings.path.push_back(Substream::MapKeyExists);
    settings.path.back().name_of_substream = key_name;
    auto exists_data = SubstreamData(exists_serialization).withType(std::make_shared<DataTypeUInt8>());
    exists_serialization->enumerateStreams(settings, callback, exists_data);
    settings.path.pop_back();
}

void SerializationMapWithKeyColumns::enumerateTemplateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData &) const
{
    settings.path.push_back(Substream::MapKeyValueTemplate);
    /// The sample column is a `ColumnMap`. `V` enumerates from its own type.
    auto value_data = SubstreamData(value_serialization).withType(value_type);
    value_serialization->enumerateStreams(settings, callback, value_data);
    settings.path.pop_back();

    settings.path.push_back(Substream::MapKeyExistsTemplate);
    auto exists_data = SubstreamData(exists_serialization).withType(std::make_shared<DataTypeUInt8>());
    exists_serialization->enumerateStreams(settings, callback, exists_data);
    settings.path.pop_back();
}

void SerializationMapWithKeyColumns::enumerateRegisteredKeyStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data,
    const SerializeBinaryBulkStatePtr & state) const
{
    if (!state)
        return;

    const auto & map_state = typeid_cast<const SerializeBinaryBulkStateMapWithKeyColumns &>(*state);
    for (const auto & key_state : map_state.keys)
        enumerateKeyStreams(settings, callback, data, key_state.key);
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

        /// One independent UInt8 presence stream per key. `AlwaysPresent` keys
        /// skip it (presence is implicitly all-1). Written through the standard
        /// nested bulk path, so it is granule/mark aligned.
        if (entry.presence_kind == MapKeyPresenceKind::Tracked)
        {
            settings.path.push_back(Substream::MapKeyExists);
            settings.path.back().name_of_substream = key_name;
            auto exists_data = SubstreamData(exists_serialization).withType(std::make_shared<DataTypeUInt8>());
            exists_serialization->enumerateStreams(settings, callback, exists_data);
            settings.path.pop_back();
        }
    }
}

void SerializationMapWithKeyColumns::serializeBinaryBulkStatePrefix(
    const IColumn & /*column*/,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto with_key_columns_state = std::make_shared<SerializeBinaryBulkStateMapWithKeyColumns>();

    if (settings.map_key_columns_frozen_keys)
    {
        /// Compact "frozen" mode: the whole part's key set is known before the first granule.
        /// Register exactly those keys and their per-key streams now; do not create template
        /// streams. Rows that miss a key are serialized as absent in place. The key list is
        /// written by the part writer into `<column>.key_columns.txt`, not from this suffix.
        with_key_columns_state->frozen = true;
        state = std::move(with_key_columns_state);
        addKeys(state, *settings.map_key_columns_frozen_keys);
        initializeKeyPrefixes(settings, state);
        auto * frozen_state = checkAndGetState<SerializeBinaryBulkStateMapWithKeyColumns>(state);
        frozen_state->prefix_written = true;
        return;
    }

    /// Wide path: keys are discovered block by block. Open the two template streams; late keys
    /// copy the templates' all-absent history when they first appear.
    {
        settings.path.push_back(Substream::MapKeyValueTemplate);
        auto empty_values = value_type->createColumn();
        value_serialization->serializeBinaryBulkStatePrefix(*empty_values, settings, with_key_columns_state->template_value_state);
        settings.path.pop_back();

        settings.path.push_back(Substream::MapKeyExistsTemplate);
        auto empty_exists = ColumnUInt8::create();
        exists_serialization->serializeBinaryBulkStatePrefix(*empty_exists, settings, with_key_columns_state->template_exists_state);
        settings.path.pop_back();
    }

    with_key_columns_state->prefix_written = true;
    state = std::move(with_key_columns_state);
}

void SerializationMapWithKeyColumns::initializeKeyPrefixes(
    SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    auto * map_state = checkAndGetState<SerializeBinaryBulkStateMapWithKeyColumns>(state);
    for (auto & key_state : map_state->keys)
    {
        if (key_state.value_state || map_state->copied_from_template.contains(key_state.key))
            continue;

        addMapKeyPath(settings, key_state.key);
        auto empty_values = value_type->createColumn();
        value_serialization->serializeBinaryBulkStatePrefix(*empty_values, settings, key_state.value_state);
        settings.path.pop_back();

        addMapExistsPath(settings, key_state.key);
        auto empty_exists = ColumnUInt8::create();
        exists_serialization->serializeBinaryBulkStatePrefix(*empty_exists, settings, key_state.exists_state);
        settings.path.pop_back();
    }
}

void SerializationMapWithKeyColumns::writeTemplateDefaults(
    size_t rows,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * map_state = checkAndGetState<SerializeBinaryBulkStateMapWithKeyColumns>(state);

    auto values = value_type->createColumn();
    values->insertManyDefaults(rows);
    settings.path.push_back(Substream::MapKeyValueTemplate);
    value_serialization->serializeBinaryBulkWithMultipleStreams(*values, 0, rows, settings, map_state->template_value_state);
    settings.path.pop_back();

    auto exists = ColumnUInt8::create();
    exists->getData().resize_fill(rows, 0);
    settings.path.push_back(Substream::MapKeyExistsTemplate);
    exists_serialization->serializeBinaryBulkWithMultipleStreams(*exists, 0, rows, settings, map_state->template_exists_state);
    settings.path.pop_back();
}

void SerializationMapWithKeyColumns::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    if (!state)
        return;

    auto * with_key_columns_state = checkAndGetState<SerializeBinaryBulkStateMapWithKeyColumns>(state);

    for (auto & key_state : with_key_columns_state->keys)
    {
        addMapKeyPath(settings, key_state.key);
        value_serialization->serializeBinaryBulkStateSuffix(settings, key_state.value_state);
        settings.path.pop_back();

        addMapExistsPath(settings, key_state.key);
        exists_serialization->serializeBinaryBulkStateSuffix(settings, key_state.exists_state);
        settings.path.pop_back();
    }

    if (with_key_columns_state->frozen)
    {
        /// No template streams. The part writer writes `<column>.key_columns.txt`.
        return;
    }

    settings.path.push_back(Substream::MapKeyValueTemplate);
    value_serialization->serializeBinaryBulkStateSuffix(settings, with_key_columns_state->template_value_state);
    settings.path.pop_back();

    settings.path.push_back(Substream::MapKeyExistsTemplate);
    exists_serialization->serializeBinaryBulkStateSuffix(settings, with_key_columns_state->template_exists_state);
    settings.path.pop_back();
}

void SerializationMapWithKeyColumns::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    settings.path.push_back(Substream::MapKeysInfo);

    if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, settings.path))
    {
        /// A single-key subcolumn reader publishes this manifest before per-key
        /// value/exists states exist. Reusing that state for a full-column read
        /// would index `value_states` / `exists_states` out of range or read 0
        /// presence rows. Only reuse directly when both are fully prepared.
        auto * cached = typeid_cast<DeserializeBinaryBulkStateMapWithKeyColumns *>(cached_state.get());
        const bool fully_prepared = cached
            && cached->value_states.size() == cached->manifest.keys.size()
            && cached->exists_states.size() == cached->manifest.keys.size();
        if (!cached || fully_prepared)
        {
            state = std::move(cached_state);
            settings.path.pop_back();
            return;
        }

        settings.path.pop_back();
        preparePerKeyDeserializeStates(*cached, settings, cache);
        state = std::move(cached_state);
        return;
    }

    if (!settings.map_key_columns_manifest)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Missing key_columns manifest for with_key_columns Map");

    auto with_key_columns_state = std::make_shared<DeserializeBinaryBulkStateMapWithKeyColumns>();
    with_key_columns_state->manifest = *settings.map_key_columns_manifest;
    settings.path.pop_back();

    preparePerKeyDeserializeStates(*with_key_columns_state, settings, cache);

    settings.path.push_back(Substream::MapKeysInfo);
    addToSubstreamsDeserializeStatesCache(cache, settings.path, with_key_columns_state);
    settings.path.pop_back();
    state = std::move(with_key_columns_state);
}

void SerializationMapWithKeyColumns::preparePerKeyDeserializeStates(
    DeserializeBinaryBulkStateMapWithKeyColumns & state,
    DeserializeBinaryBulkSettings & settings,
    SubstreamsDeserializeStatesCache * cache) const
{
    const size_t key_count = state.manifest.keys.size();
    state.value_states.resize(key_count);
    state.exists_states.resize(key_count);

    for (size_t i = 0; i < key_count; ++i)
    {
        const auto & entry = state.manifest.keys[i];

        addMapKeyPath(settings, entry.key);
        value_serialization->deserializeBinaryBulkStatePrefix(settings, state.value_states[i], cache);
        settings.path.pop_back();

        if (entry.presence_kind == MapKeyPresenceKind::Tracked)
        {
            addMapExistsPath(settings, entry.key);
            exists_serialization->deserializeBinaryBulkStatePrefix(settings, state.exists_states[i], cache);
            settings.path.pop_back();
        }
    }
}

void SerializationMapWithKeyColumns::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * with_key_columns_state = checkAndGetState<SerializeBinaryBulkStateMapWithKeyColumns>(state);

    std::vector<Field> keys;
    keys.reserve(with_key_columns_state->keys.size());
    for (const auto & key_state : with_key_columns_state->keys)
        keys.push_back(key_state.key);

    const auto & column_map = assert_cast<const ColumnMap &>(column);
    const size_t column_size = column_map.size();
    const size_t rows = limit && offset + limit < column_size ? limit : column_size - offset;

    /// Each registered key: dense value stream + per-row presence stream.
    auto pivoted = pivot(column, key_type, value_type, keys, offset, limit);
    for (size_t i = 0; i < pivoted.size(); ++i)
    {
        addMapKeyPath(settings, pivoted[i].key);
        value_serialization->serializeBinaryBulkWithMultipleStreams(
            *pivoted[i].values, 0, pivoted[i].values->size(), settings, with_key_columns_state->keys[i].value_state);
        settings.path.pop_back();

        auto exists_column = ColumnUInt8::create();
        auto & exists_data = exists_column->getData();
        exists_data.assign(pivoted[i].presence.begin(), pivoted[i].presence.end());
        addMapExistsPath(settings, pivoted[i].key);
        exists_serialization->serializeBinaryBulkWithMultipleStreams(
            *exists_column, 0, exists_column->size(), settings, with_key_columns_state->keys[i].exists_state);
        settings.path.pop_back();
    }

    with_key_columns_state->num_rows_written += rows;
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
    const auto & manifest = with_key_columns_state->manifest;
    const size_t key_count = manifest.keys.size();

    /// Compact `.mrk4` keeps each key / exists stream in its own compressed
    /// block. The Compact full-column getter only seeks for subcolumns, so the
    /// nested value serialization (e.g. String size + data) must seek itself.
    ///
    /// Only force a per-mark seek when we are NOT continuing a sequential read.
    /// With `continuous_reading` (e.g. several small blocks inside one granule)
    /// the streams must keep advancing from their current position; re-seeking to
    /// the granule's mark every block would re-read the granule from its start.
    auto original_getter = settings.getter;
    if (settings.seek_stream_to_current_mark_callback && !settings.continuous_reading)
    {
        settings.getter = [&](const SubstreamPath & path) -> ReadBuffer *
        {
            settings.seek_stream_to_current_mark_callback(path);
            return original_getter(path);
        };
    }

    /// Per-key dedicated streams: value + presence, both read through the standard
    /// nested bulk path so a `limit` spanning several granules is handled correctly.
    /// Each stream is materialized once and shared with sibling subcolumn reads
    /// (e.g. `m['k']`, `mapContains`) via `cache`, mirroring the Array/Nullable
    /// pattern, so no stream is ever advanced twice within one read.
    std::vector<MutableColumnPtr> value_columns(key_count);
    std::vector<PaddedPODArray<UInt8>> presence(key_count);
    for (size_t i = 0; i < key_count; ++i)
    {
        const auto & entry = manifest.keys[i];

        addMapKeyPath(settings, entry.key);
        value_columns[i] = value_type->createColumn();
        if (!insertDataFromSubstreamsCacheIfAny(cache, settings, *value_columns[i]))
        {
            const size_t prev_size = value_columns[i]->size();
            value_serialization->deserializeBinaryBulkWithMultipleStreams(
                *value_columns[i], limit, settings, with_key_columns_state->value_states[i], cache);
            addColumnWithNumReadRowsToSubstreamsCache(
                cache, settings.path, value_columns[i]->getPtr(), value_columns[i]->size() - prev_size);
        }
        settings.path.pop_back();

        const size_t value_rows = value_columns[i]->size();
        if (entry.presence_kind == MapKeyPresenceKind::AlwaysPresent)
        {
            presence[i].resize_fill(value_rows, 1);
        }
        else
        {
            auto exists_column = ColumnUInt8::create();
            addMapExistsPath(settings, entry.key);
            if (!insertDataFromSubstreamsCacheIfAny(cache, settings, *exists_column))
            {
                const size_t prev_size = exists_column->size();
                exists_serialization->deserializeBinaryBulkWithMultipleStreams(
                    *exists_column, limit, settings, with_key_columns_state->exists_states[i], cache);
                addColumnWithNumReadRowsToSubstreamsCache(
                    cache, settings.path, exists_column->getPtr(), exists_column->size() - prev_size);
            }
            settings.path.pop_back();
            /// Copy (do not move): the same column is now shared in `cache` and
            /// must stay intact for sibling reads (e.g. `length`, `mapContains`).
            presence[i].assign(exists_column->getData().begin(), exists_column->getData().end());
        }
    }

    settings.getter = std::move(original_getter);

    /// Determine the row count from whatever key stream we read.
    size_t rows = limit;
    if (key_count > 0)
        rows = presence[0].size();

    for (size_t i = 0; i < key_count; ++i)
    {
        if (value_columns[i]->size() != rows || presence[i].size() != rows)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Map key value/presence row count mismatch: value {}, presence {}, expected {}",
                value_columns[i]->size(), presence[i].size(), rows);
    }

    auto & nested_column = column_map.getNestedColumn();
    auto & nested_data = column_map.getNestedData();
    auto & keys_column = nested_data.getColumn(0);
    auto & values_column = nested_data.getColumn(1);
    auto & offsets = nested_column.getOffsets();

    if (key_count == 0)
    {
        offsets.reserve(offsets.size() + limit);
        for (size_t row = 0; row < limit; ++row)
            offsets.push_back(keys_column.size());
        return;
    }

    std::vector<MutableColumnPtr> key_holders(key_count);
    for (size_t i = 0; i < key_count; ++i)
    {
        key_holders[i] = key_type->createColumn();
        key_holders[i]->insert(manifest.keys[i].key);
    }

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
