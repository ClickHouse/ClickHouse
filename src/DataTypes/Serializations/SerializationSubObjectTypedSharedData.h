#pragma once

#include <DataTypes/Serializations/SerializationObjectTypedSharedData.h>
#include <DataTypes/Serializations/SimpleTextSerialization.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// Serialization of the SHARED typed paths under a sub-object prefix, read from the typed shared data. It requests its
/// paths on the structure state of the column and decodes them in one call straight into its columns; when another
/// reader of the column decoded the call first, it copies them from the substreams cache.
class SerializationSubObjectTypedSharedData final : public SimpleTextSerialization
{
public:
    using SharedTypedPaths = SerializationObjectTypedSharedData::SharedTypedPaths;

private:
    SerializationSubObjectTypedSharedData(
        const String & paths_prefix_, SharedTypedPaths paths_, size_t first_path_index_, size_t num_paths_);

public:
    static UInt128 getHash(const String & paths_prefix_, const SharedTypedPaths & paths_, size_t first_path_index_, size_t num_paths_);
    static SerializationPtr create(const String & paths_prefix_, SharedTypedPaths paths_, size_t first_path_index_, size_t num_paths_);

    size_t allocatedBytes() const override;
    bool supportsPooling() const override;

    void enumerateStreams(EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const override;

    void serializeBinaryBulkStatePrefix(
        const IColumn & column, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const override;

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

    void serializeBinary(const Field &, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeBinary(Field &, ReadBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void serializeBinary(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override { throwNoSerialization(); }
    bool tryDeserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override { throwNoSerialization(); }

private:
    [[noreturn]] static void throwNoSerialization()
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Serialization of the typed shared data of a sub-object is used only as a part of Object serialization");
    }

    String paths_prefix;
    /// The shared typed paths under the prefix, a slice of the sorted list starting at first_path_index.
    SharedTypedPaths paths;
    size_t first_path_index;
    /// Of all shared typed paths of the type.
    size_t num_paths;
};

}
