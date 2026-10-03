#pragma once

#include <DataTypes/Serializations/SerializationMapWithKeyColumns.h>
#include <DataTypes/Serializations/SimpleTextSerialization.h>

namespace DB
{

/// Read-only view of `m.keys_presence` as `Array(UInt8)`: one UInt8 per manifest
/// key per row, in manifest order. Reconstructs presence from the per-key
/// `.exists_<name>` streams (via the full `with_key_columns` read path), so it
/// always agrees with a full `m` read.
class SerializationMapKeyPresenceMerge final : public SimpleTextSerialization
{
public:
    static SerializationPtr create(const SerializationPtr & map_with_key_columns_serialization_);

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

    struct DeserializeState : public DeserializeBinaryBulkState
    {
        DeserializeBinaryBulkStatePtr map_state;

        DeserializeBinaryBulkStatePtr clone() const override
        {
            auto new_state = std::make_shared<DeserializeState>(*this);
            new_state->map_state = map_state ? map_state->clone() : nullptr;
            return new_state;
        }
    };

private:
    explicit SerializationMapKeyPresenceMerge(const SerializationPtr & map_with_key_columns_serialization_);

    [[noreturn]] static void throwNoSerialization();

    SerializationPtr map_with_key_columns_serialization;
};

}
