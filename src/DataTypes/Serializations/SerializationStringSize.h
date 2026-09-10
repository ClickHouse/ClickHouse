#pragma once

#include <Core/MergeTreeSerializationEnums.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationString.h>

namespace DB
{

/// Enables the `.size` subcolumn for string columns.
class SerializationStringSize final : public SerializationNumber<UInt64>
{
private:
    explicit SerializationStringSize(MergeTreeStringSerializationVersion version_);

public:
    static UInt128 getHash(MergeTreeStringSerializationVersion version_);

    static SerializationPtr create(MergeTreeStringSerializationVersion version_);

    size_t allocatedBytes() const override;

    void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * cache) const override;

private:
    MergeTreeStringSerializationVersion version;

    /// Helper to access base string serialization logic.
    SerializationPtr serialization_string;

    /// dispatch helpers for deserializeBinaryBulkWithMultipleStreams
    void deserializeBinaryBulkWithSizeStream(
        ColumnPtr & column,
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const;
    void deserializeBinaryBulkWithoutSizeStream(
        ColumnPtr & column,
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const;

    void deserializeWithStringData(
        ColumnPtr & column,
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStateStringWithoutSizeStream & string_state,
        SubstreamsCache * cache) const;

    void deserializeWithoutStringData(
        ColumnPtr & column, size_t rows_offset, size_t limit, DeserializeBinaryBulkSettings & settings, SubstreamsCache * cache) const;
};

}
