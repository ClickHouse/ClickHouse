#pragma once
#include <DataTypes/Serializations/SerializationWrapper.h>

namespace DB
{

/// Serialization for reading an already-Nullable subcolumn extracted from a Nullable(Tuple(...)) column.
///
/// When MergeTree reads a subcolumn like `tup.s` from a column
/// `tup Nullable(Tuple(u UInt64, s Nullable(String)))`, the subcolumn's inner data streams live under
/// [NullableElements, TupleElement(name)] in the parent's stream hierarchy, and the parent's null map is
/// at [NullMap].
///
/// This class reads the parent's null map, then the inner ColumnNullable (which has its own null map),
/// and ORs the parent's null map into the inner one.
///
///   Substreams layout (base name "tup", element "s"):
///     [NullMap]                                        -> tup.null       (parent null map)
///     [NullableElements, TupleElement("s"), NullMap]   -> tup%2Es.null   (inner null map)
///     [NullableElements, TupleElement("s"), ...]       -> tup%2Es.*      (data)
class SerializationNullableWithParentNullMap final : public SerializationWrapper
{
public:
    static SerializationPtr create(const SerializationPtr & nested_);

    void enumerateStreams(EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * cache) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t rows_offset,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

private:
    explicit SerializationNullableWithParentNullMap(const SerializationPtr & nested_);
    static UInt128 getHash(const SerializationPtr & nested_);
};

}
