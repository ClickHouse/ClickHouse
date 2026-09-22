#pragma once

#include <DataTypes/Serializations/SerializationMapWithKeyColumns.h>
#include <DataTypes/Serializations/SimpleTextSerialization.h>

namespace DB
{

/// Reads or writes the shared `key_presence` stream as `Array(UInt8)`:
/// one UInt8 per manifest key, in that manifest's order.
///
/// Read uses the source part's `keys_info`. Write uses the caller-supplied
/// output union so `keys_info` and the presence block stay aligned.
class SerializationMapKeyPresenceMerge final : public SimpleTextSerialization
{
public:
    static SerializationPtr create(
        const SerializationPtr & map_with_key_columns_serialization_,
        MapKeyManifest write_manifest_ = {},
        bool for_write_ = false);

    bool supportsPooling() const override { return false; }

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

    void serializeBinary(const Field &, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeBinary(Field &, ReadBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void serializeBinary(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override { throwNoSerialization(); }
    bool tryDeserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override { throwNoSerialization(); }

    const MapKeyManifest & getWriteManifest() const { return write_manifest; }

    struct DeserializeState : public DeserializeBinaryBulkState
    {
        MapKeyManifest manifest;

        DeserializeBinaryBulkStatePtr clone() const override
        {
            return std::make_shared<DeserializeState>(*this);
        }
    };

    struct SerializeState : public SerializeBinaryBulkState
    {
        std::vector<std::vector<UInt8>> pending_presence;
        size_t pending_rows = 0;
    };

private:
    SerializationMapKeyPresenceMerge(
        const SerializationPtr & map_with_key_columns_serialization_,
        MapKeyManifest write_manifest_,
        bool for_write_);

    [[noreturn]] static void throwNoSerialization();
    void flushPendingPresence(SerializeBinaryBulkSettings & settings, SerializeState & state) const;

    SerializationPtr map_with_key_columns_serialization;
    MapKeyManifest write_manifest;
    bool for_write = false;
};

}
