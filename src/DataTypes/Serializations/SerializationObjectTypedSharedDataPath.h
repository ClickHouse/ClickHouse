#pragma once

#include <DataTypes/Serializations/SerializationObjectTypedSharedData.h>
#include <DataTypes/Serializations/SerializationWrapper.h>

namespace DB
{

/// Serialization of one SHARED typed path of Object, or of a subcolumn of it, read from the typed shared data of a
/// part. The prefix registers the path in the structure state shared by the readers of the column; the data call
/// takes the call's rows of the path from the substreams cache or, as the first reader of the call, decodes every
/// requested path (SerializationObjectTypedSharedData::deserializeRequestedPaths). A subcolumn is extracted from the
/// decoded path.
class SerializationObjectTypedSharedDataPath final : public SerializationWrapper
{
private:
    SerializationObjectTypedSharedDataPath(
        const SerializationPtr & nested_,
        const SerializationObjectTypedSharedData::SharedTypedPath & path_,
        size_t path_index_,
        size_t num_paths_,
        const String & subcolumn_name_);

public:
    static UInt128 getHash(
        const SerializationPtr & nested_,
        const SerializationObjectTypedSharedData::SharedTypedPath & path_,
        size_t path_index_,
        size_t num_paths_,
        const String & subcolumn_name_);

    static SerializationPtr create(
        const SerializationPtr & nested_,
        const SerializationObjectTypedSharedData::SharedTypedPath & path_,
        size_t path_index_,
        size_t num_paths_,
        const String & subcolumn_name_);

    size_t allocatedBytes() const override;
    bool supportsPooling() const override { return SerializationWrapper::supportsPooling() && path.serialization->supportsPooling(); }

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

private:
    /// The whole path, which is what the typed shared data stores; a subcolumn is extracted from it.
    SerializationObjectTypedSharedData::SharedTypedPath path;
    /// Position of the path among the shared typed paths sorted by name, and their number.
    size_t path_index;
    size_t num_paths;
    /// Empty when the whole path is read.
    String subcolumn_name;
};

}
