#pragma once
#include <DataTypes/Serializations/SerializationWrapper.h>

namespace DB
{

/// Serialization for reading a subcolumn extracted from a `Nullable(...)` column that cannot represent
/// NULL itself: `Tuple`, `Map`, `Array`, `AggregateFunction`. Its only job is to reproduce the
/// `Substream::NullableElements` path element, so the subcolumn's streams resolve to the file names the
/// whole column wrote. The parent null map is not applied because there is nowhere to put the NULLs;
/// those rows keep whatever the writer stored. The subcolumns that do carry NULL go through
/// `SerializationNullableWithParentNullMap` instead.
class SerializationNullableElements final : public SerializationWrapper
{
public:
    static SerializationPtr create(const SerializationPtr & nested_);

    void enumerateStreams(EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const override;

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

private:
    explicit SerializationNullableElements(const SerializationPtr & nested_);
    static UInt128 getHash(const SerializationPtr & nested_);
};

}
