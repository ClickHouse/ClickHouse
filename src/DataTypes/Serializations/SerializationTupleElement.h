#pragma once

#include <DataTypes/Serializations/SerializationWrapper.h>

namespace DB
{

class SerializationTupleElement final : public SerializationWrapper
{
private:
    String name;
    bool escape_delimiter;

public:
    SerializationTupleElement(const SerializationPtr & nested_, const String & name_, bool escape_delimiter_ = true)
        : SerializationWrapper(nested_)
        , name(name_), escape_delimiter(escape_delimiter_)
    {
    }

    const String & getElementName() const { return name; }

    void enumerateStreams(
        const StreamCallback & callback,
        SubstreamPath & path) const override;

    void serializeBinaryBulkStatePrefix(
         SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state) const override;

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
    void addToPath(SubstreamPath & path) const;
};

}
