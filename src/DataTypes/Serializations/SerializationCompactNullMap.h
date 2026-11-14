#pragma once

#include <Columns/ColumnsNumber.h>
#include <DataTypes/Serializations/SerializationNumber.h>

namespace DB
{

/// The compact serialization of the null map of `Nullable`.
///
/// The data is split into granules: each call of the serialization (a granule in MergeTree, a block in the
/// `Native` format) writes a granule. A granule starts with the number of rows as `VarUInt` and the format
/// as `UInt8`:
///   - `NO_NULLS`: there are no NULLs in the granule, no data follows;
///   - `ALL_NULLS`: all rows of the granule are NULL, no data follows;
///   - `PLAIN`: the null map of the granule follows, one byte per row.
///
/// A column without NULLs takes a few bytes per granule, and the reader does not have to create its null map.
/// It is used when `nullable_serialization_version` is `compact_null_map` in MergeTree,
/// and in the native protocol since `DBMS_MIN_REVISION_WITH_COMPACT_NULL_MAP_SERIALIZATION`.
///
/// This class is also the serialization of the `null` subcolumn of such `Nullable` columns.
class SerializationCompactNullMap final : public SerializationNumber<UInt8>
{
public:
    enum GranuleFormat : UInt8
    {
        NO_NULLS = 0,
        ALL_NULLS = 1,
        PLAIN = 2,
    };

    /// The state of reading the null map stream. It is shared by all the readers of the same stream
    /// (e.g. the `Nullable` column and its subcolumns) via the deserialization states cache.
    struct DeserializeBinaryBulkStateCompactNullMap : public ISerialization::DeserializeBinaryBulkState
    {
        size_t remaining_rows_in_granule = 0;
        GranuleFormat granule_format = NO_NULLS;

        ISerialization::DeserializeBinaryBulkStatePtr cloneImpl(ISerialization::DeserializeBinaryBulkStateCloneContext &) const override
        {
            return std::make_shared<DeserializeBinaryBulkStateCompactNullMap>(*this);
        }
    };

    static SerializationPtr create();

    /// Write a granule of the null map. `null_map` is nullptr if there are no NULLs.
    static void serializeGranule(const PaddedPODArray<UInt8> * null_map, size_t offset, size_t limit, WriteBuffer & ostr);

    /// Read up to `limit` rows of the null map. Returns the number of read rows and the read null map,
    /// which is nullptr if the read rows have no NULLs.
    static std::pair<size_t, ColumnUInt8::MutablePtr> deserializeRows(
        DeserializeBinaryBulkStateCompactNullMap & state, ReadBuffer & istr, size_t limit, bool continuous_reading);

    /// Get the state of the null map stream at `settings.path` from the cache, or create it.
    static DeserializeBinaryBulkStatePtr deserializeStatePrefix(
        DeserializeBinaryBulkSettings & settings, SubstreamsDeserializeStatesCache * cache);

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

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
    SerializationCompactNullMap() = default;
};

}
