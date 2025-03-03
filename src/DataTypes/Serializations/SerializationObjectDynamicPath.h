#pragma once

#include <DataTypes/Serializations/SerializationWrapper.h>

namespace DB
{

/// Serialization of dynamic Object paths.
/// For example, if we have type JSON(a.b UInt32, b.c String) and data {"a" : {"b" : 42}, "b" : {"c" : "Hello}, "c" : {"d" : [1, 2, 3]}, "d" : 42}
/// this class will be responsible for reading dynamic paths 'c.d' and 'd' as subcolumns.
/// Typed paths 'a.b' and 'b.c' are serialized in SerializationObjectTypedPath.
class SerializationObjectDynamicPath final : public SerializationWrapper
{
public:
    SerializationObjectDynamicPath(const SerializationPtr & nested_, const String & path_, const String & path_subcolumn_, size_t max_dynamic_types_);

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
        ColumnPtr & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

private:
    String path;
    String path_subcolumn;
    SerializationPtr dynamic_serialization;
    SerializationPtr shared_data_serialization;
    size_t max_dynamic_types;
};

}
