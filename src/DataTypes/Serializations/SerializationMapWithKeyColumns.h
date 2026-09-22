#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SimpleTextSerialization.h>

namespace DB
{

enum class MapKeysInfoVersion : UInt8
{
    V1 = 0,
};

enum class MapKeyPresenceKind : UInt8
{
    AlwaysPresent = 0,
    Tracked = 1,
};

enum class MapKeyValueKind : UInt8
{
    Dense = 0,
    Sparse = 1,
};

struct MapKeyManifestEntry
{
    Field key;
    MapKeyPresenceKind presence_kind = MapKeyPresenceKind::Tracked;
    MapKeyValueKind value_kind = MapKeyValueKind::Dense;
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

    static MapKeyManifest collectManifestFromColumn(const IColumn & column);
    static void writeManifest(WriteBuffer & ostr, const SerializationPtr & key_serialization, const MapKeyManifest & manifest);
    static MapKeyManifest readManifest(ReadBuffer & istr, const SerializationPtr & key_serialization);
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

    struct SerializeBinaryBulkStateMapWithKeyColumns : public SerializeBinaryBulkState
    {
        MapKeyManifest manifest;
        std::vector<SerializeBinaryBulkStatePtr> value_states;
        std::vector<std::vector<UInt8>> pending_presence;
        size_t pending_rows = 0;
    };

    struct DeserializeBinaryBulkStateMapWithKeyColumns : public DeserializeBinaryBulkState
    {
        MapKeyManifest manifest;
        std::vector<DeserializeBinaryBulkStatePtr> value_states;

        DeserializeBinaryBulkStatePtr clone() const override
        {
            return std::make_shared<DeserializeBinaryBulkStateMapWithKeyColumns>(*this);
        }
    };

private:
    SerializationMapWithKeyColumns(
        const DataTypePtr & key_type_,
        const DataTypePtr & value_type_,
        const SerializationPtr & key_serialization_,
        const SerializationPtr & value_serialization_,
        const SerializationPtr & nested_serialization_);

    void addMapKeyPath(SerializeBinaryBulkSettings & settings, const Field & key) const;
    void addMapKeyPath(DeserializeBinaryBulkSettings & settings, const Field & key) const;
    void flushPendingPresence(SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStateMapWithKeyColumns & state) const;

    DataTypePtr key_type;
    DataTypePtr value_type;
    SerializationPtr key_serialization;
    SerializationPtr value_serialization;
    SerializationPtr nested_serialization;
    SerializationPtr basic_map_serialization;
};

}
