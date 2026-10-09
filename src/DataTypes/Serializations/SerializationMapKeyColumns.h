#pragma once

#include <DataTypes/Serializations/SerializationWrapper.h>
#include <Core/Field.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <map>
#include <optional>
#include <set>
#include <vector>

namespace DB
{

/// Disk serialization for `map_serialization_version = 'with_key_columns'`.
/// The key set of a part is stored as a plain serialized `String` column in the
/// `m.keys` stream (`Substream::MapKeys`), sorted by byte order, deduplicated and
/// free of empty keys. Each key stores its value data in `m.values.<key>`
/// (`Substream::MapKeyValue`) and its presence bitmap in `m.exists.<key>`
/// (`Substream::MapKeyPresence`); the raw key bytes are the last path element.
/// Presence bitmap and value columns both hold one entry per row, so reading a
/// range of rows reads the same range of every key's streams and filters by
/// presence.
///
/// Text formats (and everything else not binary-bulk) delegate to the wrapped
/// ordinary Map serialization.
class SerializationMapKeyColumns final : public SerializationWrapper
{
public:
    static UInt128 getHash(
        const SerializationPtr & text_serialization_,
        const SerializationPtr & value_serialization_);

    static SerializationPtr create(
        const DataTypePtr & key_type_,
        const DataTypePtr & value_type_,
        const SerializationPtr & key_serialization_,
        const SerializationPtr & value_serialization_,
        const SerializationPtr & text_serialization_);

    const DataTypePtr & getKeyType() const { return key_type; }
    const DataTypePtr & getValueType() const { return value_type; }
    const SerializationPtr & getKeySerialization() const { return key_serialization; }
    const SerializationPtr & getValueSerialization() const { return value_serialization; }

    /// Whether `type` is a Map or holds Maps inside `Array`/`Tuple`, i.e. Maps that get
    /// the per-key layout with `map_serialization_version = 'with_key_columns'`.
    static bool hasMapsWithKeyColumns(const IDataType & type);

    /// Returns `column` (of `type`) with the keys of every `with_key_columns` Map in it
    /// (top level and inside `Array`/`Tuple`) sorted within each row, i.e. the value
    /// the per-key layout reads back. Computing sorting/partition keys and indices on this
    /// form keeps them consistent with the stored data. Returns `column` itself when
    /// there is nothing to reorder.
    static ColumnPtr sortMapKeys(const ColumnPtr & column, const IDataType & type);

    /// The key set of `column` alone (first-seen order), independent of `state`.
    /// Throws on duplicate keys within one row and on empty keys.
    std::vector<String> collectColumnKeys(const IColumn & column) const;

    /// Appends the keys of `column` (a Map at `path` inside its column) to the key
    /// set of that Map in `keys`. Throws on duplicate keys within one row and on empty keys.
    void collectKeys(const IColumn & column, const SubstreamPath & path, MapKeyColumnsKeys & keys) const;

    /// The fixed key set of the Map at `path` (empty when `keys` has none for it).
    static const std::vector<String> & getFixedKeys(const MapKeyColumnsKeys * keys, const SubstreamPath & path);

    void enumerateKeyStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data,
        const String & key,
        const DeserializeBinaryBulkStatePtr & value_state = nullptr,
        const DeserializeBinaryBulkStatePtr & presence_state = nullptr) const;

    /// The `m.keys` stream payload: a `varint` key count followed by the part's keys
    /// in `String` binary form, sorted by byte order, deduplicated, none of them
    /// empty. The count makes the payload self-delimiting, so a Map nested inside an
    /// Array/Tuple (whose outer serialization appends granule data to the same nested
    /// stream) reads back exactly the manifest. Reading rejects empty keys and keys
    /// that are not strictly increasing.
    static std::vector<String> readMapKeys(ReadBuffer & in);
    void writeMapKeys(WriteBuffer & out, const std::vector<String> & keys) const;

    void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const override;

    void serializeBinaryBulkStatePrefix(
        const IColumn & column,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * cache) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        IColumn & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

    struct SerializeState;
    struct DeserializeState;

    friend class SerializationMapKeyColumn;
    friend class SerializationMapKeyPresence;

private:
    SerializationMapKeyColumns(
        const DataTypePtr & key_type_,
        const DataTypePtr & value_type_,
        const SerializationPtr & key_serialization_,
        const SerializationPtr & value_serialization_,
        const SerializationPtr & text_serialization_);

    void enumeratePresenceStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data,
        const String & key,
        const DeserializeBinaryBulkStatePtr & presence_state) const;

    /// Reads the `m.keys` stream into `state->keys` (empty key set when the column
    /// is absent from this part). Shared by the whole-Map reader and the subcolumn
    /// serializers.
    void readMapKeysStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * cache) const;

    DataTypePtr key_type;
    DataTypePtr value_type;
    SerializationPtr key_serialization;
    /// The serialization of V behind `m.values.<key>`; its own substreams
    /// (e.g. `.null` for `Nullable`) nest beneath the per-key path.
    SerializationPtr value_serialization;
};

/// Reads or writes one key value (`m.values.<key>`) as the value type `V`. A key
/// absent from the part's key set produces type defaults without opening data files.
class SerializationMapKeyColumn final : public SerializationWrapper
{
public:
    /// Does not support pooling because it stores runtime data (the key).
    bool supportsPooling() const override { return false; }

    static SerializationPtr create(
        const SerializationPtr & value_serialization_,
        const SerializationPtr & map_serialization_,
        String key_,
        String key_subcolumn_name_);

    void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * cache) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        IColumn & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

    void serializeBinaryBulkStatePrefix(
        const IColumn & column,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

private:
    SerializationMapKeyColumn(
        const SerializationPtr & value_serialization_,
        const SerializationPtr & map_serialization_,
        String key_,
        String key_subcolumn_name_);

    SerializationPtr map_serialization;
    String key;
    String key_subcolumn_name;
};

/// Reads the `values` subcolumn (`Array(V)`) of a `with_key_columns` Map: reads the
/// whole Map and keeps its values.
class SerializationMapKeyColumnsValues final : public SerializationWrapper
{
public:
    explicit SerializationMapKeyColumnsValues(const SerializationPtr & map_serialization_) : SerializationWrapper(map_serialization_) {}

    bool supportsPooling() const override { return false; }

    void enumerateStreams(EnumerateStreamsSettings &, const StreamCallback &, const SubstreamData &) const override;
    void deserializeBinaryBulkWithMultipleStreams(
        IColumn &, size_t, DeserializeBinaryBulkSettings &, DeserializeBinaryBulkStatePtr &, SubstreamsCache *) const override;
};

/// Reads all present keys (`Array(K)`, the `keys` subcolumn) from the presence
/// streams only — value streams are never opened.
class SerializationMapKeyPresence final : public SerializationWrapper
{
public:
    explicit SerializationMapKeyPresence(SerializationPtr map_serialization_);

    bool supportsPooling() const override { return false; }

    void enumerateStreams(EnumerateStreamsSettings &, const StreamCallback &, const SubstreamData &) const override;
    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings &, DeserializeBinaryBulkStatePtr &, SubstreamsDeserializeStatesCache *) const override;
    void deserializeBinaryBulkWithMultipleStreams(
        IColumn &, size_t, DeserializeBinaryBulkSettings &, DeserializeBinaryBulkStatePtr &, SubstreamsCache *) const override;

private:
    SerializationPtr map_serialization;
};

}
