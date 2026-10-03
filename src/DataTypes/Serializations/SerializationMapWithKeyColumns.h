#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SimpleTextSerialization.h>

#include <map>
#include <set>

namespace DB
{

enum class MapKeyPresenceKind : UInt8
{
    /// The key is present in every row, so no `.exists_<name>` stream is written.
    AlwaysPresent = 0,
    /// Presence varies by row; a per-key `.exists_<name>` UInt8 stream records it.
    Tracked = 1,
};

struct MapKeyManifestEntry
{
    Field key;
    MapKeyPresenceKind presence_kind = MapKeyPresenceKind::Tracked;
};

struct MapKeyManifest
{
    std::vector<MapKeyManifestEntry> keys;
};

class SerializationMapWithKeyColumns final : public SimpleTextSerialization
{
public:
    static UInt128 getHash(
        const SerializationPtr & nested_,
        const DataTypePtr & key_type_,
        const DataTypePtr & value_type_);
    static SerializationPtr create(
        const DataTypePtr & key_type_,
        const DataTypePtr & value_type_,
        const SerializationPtr & key_serialization_,
        const SerializationPtr & value_serialization_,
        const SerializationPtr & nested_serialization_);

    bool supportsPooling() const override { return nested_serialization->supportsPooling(); }

    const MapKeyManifest & getManifestFromState(const DeserializeBinaryBulkStatePtr & state) const;
    const DataTypePtr & getKeyType() const { return key_type; }
    const DataTypePtr & getValueType() const { return value_type; }
    const SerializationPtr & getKeySerialization() const { return key_serialization; }
    const SerializationPtr & getValueSerialization() const { return value_serialization; }
    String keyToStreamName(const Field & key) const;
    /// Alias mirroring the reference: the per-key subcolumn name for a key.
    String getKeySubcolumnName(const Field & key) const { return keyToStreamName(key); }

    static MapKeyManifest collectManifestFromColumn(const IColumn & column);
    /// Plain-text part file `<column>.key_columns.txt`.
    /// Line 1 is the format version, line 2 is the key count, then one line per key:
    /// `<presence_kind>\t<text-escaped key>`. `presence_kind` is the decimal value of
    /// `MapKeyPresenceKind`. The key uses that type's `serializeTextEscaped`, so tabs,
    /// newlines and non-UTF-8 bytes round-trip.
    static void writeKeyColumnsText(WriteBuffer & ostr, const DataTypePtr & key_type, const MapKeyManifest & manifest);
    static MapKeyManifest readKeyColumnsText(ReadBuffer & istr, const DataTypePtr & key_type);
    static std::vector<Field> keysFromManifest(const MapKeyManifest & manifest);

    struct PivotedKeyColumn
    {
        Field key;
        MutableColumnPtr values;
        std::vector<UInt8> presence;
    };

    /// Pivot a `ColumnMap` into one dense value column and presence bitmap per key.
    /// Duplicate keys in a row keep the first match, matching `arrayElement`.
    /// `limit = 0` means "to the end of the column".
    static std::vector<PivotedKeyColumn> pivot(
        const IColumn & column,
        const DataTypePtr & key_type,
        const DataTypePtr & value_type,
        const std::vector<Field> & keys,
        size_t offset = 0,
        size_t limit = 0);

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

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &, bool whole) const override;
    bool tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &, bool whole) const override;

    /// Per-key writer state. Value and presence (`.key_<name>` / `.exists_<name>`)
    /// both flow through the standard nested bulk path, so they are granule/mark
    /// aligned and `limit` may start or end mid-granule.
    struct KeyWriteState
    {
        Field key;
        MapKeyPresenceKind presence_kind = MapKeyPresenceKind::Tracked;
        SerializeBinaryBulkStatePtr value_state;
        SerializeBinaryBulkStatePtr exists_state;
    };

    struct SerializeBinaryBulkStateMapWithKeyColumns : public SerializeBinaryBulkState
    {
        /// Registered keys, discovered block by block on the Wide path or frozen up front on
        /// the Compact path.
        std::vector<KeyWriteState> keys;
        std::map<Field, size_t> key_index;
        /// Keys that were seeded from the templates (a late key on the Wide path). Their prefixes
        /// were already written by copying the template, so `initializeKeyPrefixes` skips them.
        std::set<Field> copied_from_template;
        /// Value/exists template streams' serialize state (Wide path only; null when frozen).
        SerializeBinaryBulkStatePtr template_value_state;
        SerializeBinaryBulkStatePtr template_exists_state;
        /// Exact number of rows written so far for the part. New late keys are absent from every
        /// one of these rows, which is what the copied template history encodes.
        size_t num_rows_written = 0;
        /// Frozen mode: keys were frozen before the first granule, there are no template streams,
        /// and the manifest is written once by the Compact writer (not from the suffix here).
        bool frozen = false;
        bool prefix_written = false;
    };

    struct DeserializeBinaryBulkStateMapWithKeyColumns : public DeserializeBinaryBulkState
    {
        MapKeyManifest manifest;
        std::vector<DeserializeBinaryBulkStatePtr> value_states;
        std::vector<DeserializeBinaryBulkStatePtr> exists_states;

        DeserializeBinaryBulkStatePtr clone() const override
        {
            return std::make_shared<DeserializeBinaryBulkStateMapWithKeyColumns>(*this);
        }
    };

    /// Keys from `column` that are not yet registered in `state`, in first-seen order.
    std::vector<Field> collectNewKeys(const IColumn & column, const SerializeBinaryBulkState & state) const;
    /// All distinct keys present in `column`, first-seen order. Used by the Compact writer to
    /// freeze the part-wide key set from the buffered block before writing the first granule.
    std::vector<Field> collectAllKeys(const IColumn & column) const;
    /// Register `keys` in `state`, opening their per-key value/exists streams lazily.
    void addKeys(SerializeBinaryBulkStatePtr & state, const std::vector<Field> & keys) const;
    void markKeysCopiedFromTemplate(SerializeBinaryBulkStatePtr & state, const std::vector<Field> & keys) const;
    std::vector<Field> getRegisteredKeys(const SerializeBinaryBulkState & state) const;
    size_t getRegisteredKeyCount(const SerializeBinaryBulkState & state) const;

    /// Initialize newly registered key streams (value + exists prefixes) before the writer
    /// records their first marks. Keys seeded from templates are skipped.
    void initializeKeyPrefixes(SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const;

    /// Streams of one key's `.key_<name>` value + `.exists_<name>` presence.
    void enumerateKeyStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data,
        const Field & key) const;

    /// Streams of the two template streams `.key_template` (value defaults) and
    /// `.exists_template` (UInt8 zeros).
    void enumerateTemplateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const;

    /// Streams of the keys registered in `state`. Writers must use this rather than
    /// `enumerateStreams`, which infers keys from a column. The key list itself is not a stream;
    /// it is the part file `<column>.key_columns.txt`.
    void enumerateRegisteredKeyStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data,
        const SerializeBinaryBulkStatePtr & state) const;

    /// Append `rows` all-default values to `.key_template` and `rows` zeros to `.exists_template`,
    /// keeping the templates row-aligned with the granules already written.
    void writeTemplateDefaults(
        size_t rows,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const;

private:
    SerializationMapWithKeyColumns(
        const DataTypePtr & key_type_,
        const DataTypePtr & value_type_,
        const SerializationPtr & key_serialization_,
        const SerializationPtr & value_serialization_,
        const SerializationPtr & nested_serialization_);

    void addMapKeyPath(SerializeBinaryBulkSettings & settings, const Field & key) const;
    void addMapKeyPath(DeserializeBinaryBulkSettings & settings, const Field & key) const;
    void addMapExistsPath(SerializeBinaryBulkSettings & settings, const Field & key) const;
    void addMapExistsPath(DeserializeBinaryBulkSettings & settings, const Field & key) const;
    void preparePerKeyDeserializeStates(
        DeserializeBinaryBulkStateMapWithKeyColumns & state,
        DeserializeBinaryBulkSettings & settings,
        SubstreamsDeserializeStatesCache * cache) const;

public:
    const SerializationPtr & getExistsSerialization() const { return exists_serialization; }
    const SerializationPtr & getBasicMapSerialization() const { return basic_map_serialization; }

private:
    DataTypePtr key_type;
    DataTypePtr value_type;
    SerializationPtr key_serialization;
    SerializationPtr value_serialization;
    SerializationPtr nested_serialization;
    /// Used only for the text/binary (non-bulk) serialization methods.
    SerializationPtr basic_map_serialization;
    /// UInt8 serialization for the per-key `.exists_<name>` presence streams and templates.
    SerializationPtr exists_serialization;
};

}
