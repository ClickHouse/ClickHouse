#pragma once

#include <DataTypes/Serializations/SerializationWrapper.h>
#include <Columns/ColumnArray.h>
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

    /// The key set of `column` alone (first-seen order), independent of `state`.
    /// Throws on duplicate keys within one row and on empty keys.
    std::vector<String> collectColumnKeys(const IColumn & column) const;

    /// Keys of `column` that are not yet registered in `state`, first-seen order.
    /// Throws on duplicate keys within one row and on empty keys.
    std::vector<String> getMissingKeys(const IColumn & column, const SerializeBinaryBulkState & state) const;

    /// Same check for a column whose Maps are nested inside Array/Tuple: `state` is
    /// the top-level state (the Map's state is found inside it). Empty when the key
    /// set has not been fixed yet (nothing to check).
    std::vector<String> getMissingKeysNested(const IDataType & type, const IColumn & column, const SerializeBinaryBulkState & state) const;

    /// Register a pre-planned key set (e.g. the merge-planned union of the input
    /// parts' keys) in `state`, so every planned key gets its `m.values.<key>` /
    /// `m.exists.<key>` streams even when no written row happens to contain it.
    /// Must be called before any block data is written; keys discovered later in
    /// the written blocks must be a subset of the registered keys, otherwise the
    /// write is rejected by `getMissingKeys`.
    void addPlannedKeys(SerializeBinaryBulkStatePtr & state, const std::vector<String> & keys) const;

    /// Whether `state` already carries a registered key set (from `addPlannedKeys`
    /// or from an earlier block), i.e. the caller must not adopt the first block's keys.
    bool hasRegisteredKeys(const SerializeBinaryBulkState & state) const;

    /// Prepare a serialize state that carries a pre-declared key set. The writer
    /// discovers the part's keys before opening any of the column's streams and
    /// hands the state to `serializeBinaryBulkStatePrefix`, which adopts them.
    static SerializeBinaryBulkStatePtr createSeedKeysState(std::vector<String> keys);

    /// The union of keys of every Map nested inside `column` (for a `type` holding
    /// nested `with_key_columns` Maps, e.g. `Array(Map(...))`). Throws on duplicate
    /// keys within one map element and on empty keys.
    std::vector<String> collectNestedKeys(const IDataType & type, const IColumn & column) const;

    /// A seed state for the write side of a column of `type` whose nested
    /// `with_key_columns` Maps must use `keys` as their part-level key set. The
    /// nesting serializations (Array/Tuple/Named) forward the matching child state
    /// to the nested prefix via `forwardNestedSeedKeysState`.
    static SerializeBinaryBulkStatePtr createNestedSeedKeysState(const IDataType & type, std::vector<String> keys);
    /// The child of a nested seed state for the nested serialization (element
    /// `element` of a Tuple; the nested serialization of an Array/Named); null when
    /// `state` is not a nested seed state or the element carries no Map.
    static SerializeBinaryBulkStatePtr forwardNestedSeedKeysState(const SerializeBinaryBulkStatePtr & state, size_t element = 0);
    /// True while a seed state has not been adopted by `serializeBinaryBulkStatePrefix`
    /// (used by nesting serializations to avoid re-running the prefix on a state the
    /// seed chain still owns).
    static bool isSeedStatePending(const SerializeBinaryBulkState & state);
    /// Records the flattened positions mapping of a nested Map's outer slice (see
    /// `SerializeState::nested_positions`); called by `SerializationArray` when it
    /// extracts a granule slice of a column containing a nested `with_key_columns` Map.
    static void recordNestedPositions(SerializeBinaryBulkState & state, const ColumnArray::Offsets & offsets);

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
    std::vector<String> readMapKeys(ReadBuffer & in) const;
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

/// Reads a key's existence (`m.exists_<key>` as `UInt8`) or, without a requested
/// key, all present keys (`Array(K)`, the `keys` subcolumn), reading only the
/// presence streams — value streams are never opened.
class SerializationMapKeyPresence final : public SerializationWrapper
{
public:
    /// Does not support pooling because it stores runtime data (the requested key).
    bool supportsPooling() const override { return false; }

    SerializationMapKeyPresence(SerializationPtr map_serialization_, std::optional<String> key_);

    void enumerateStreams(EnumerateStreamsSettings &, const StreamCallback &, const SubstreamData &) const override;
    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings &, DeserializeBinaryBulkStatePtr &, SubstreamsDeserializeStatesCache *) const override;
    void deserializeBinaryBulkWithMultipleStreams(
        IColumn &, size_t, DeserializeBinaryBulkSettings &, DeserializeBinaryBulkStatePtr &, SubstreamsCache *) const override;

private:
    SerializationPtr map_serialization;
    std::optional<String> requested_key;
};

}
