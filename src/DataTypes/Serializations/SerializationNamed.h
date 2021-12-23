#pragma once

#include <DataTypes/Serializations/SerializationWrapper.h>

namespace DB
{

class SerializationNamed final : public SerializationWrapper
{
private:
    String name;
    bool escape_delimiter;

public:
    SerializationNamed(const SerializationPtr & nested_, const String & name_, bool escape_delimiter_ = true)
        : SerializationWrapper(nested_)
        , name(name_), escape_delimiter(escape_delimiter_)
    {
    }

    const String & getElementName() const { return name; }

    void enumerateStreams(
        SubstreamPath & path,
        const StreamCallback & callback,
        DataTypePtr type,
        ColumnPtr column) const override;

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
    struct SubcolumnCreator : public ISubcolumnCreator
    {
        const String name;
        const bool escape_delimiter;

        SubcolumnCreator(const String & name_, bool escape_delimiter_)
            : name(name_), escape_delimiter(escape_delimiter_) {}

        DataTypePtr create(const DataTypePtr & prev) const override { return prev; }
        ColumnPtr create(const ColumnPtr & prev) const override { return prev; }
        SerializationPtr create(const SerializationPtr & prev) const override
        {
            return std::make_shared<SerializationNamed>(prev, name, escape_delimiter);
        }
    };

    void addToPath(SubstreamPath & path) const;
};

}
