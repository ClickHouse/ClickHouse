#pragma once

#include <DataTypes/Serializations/SerializationWrapper.h>
#include <DataTypes/Serializations/SimpleTextSerialization.h>
#include <base/FnTraits.h>


namespace DB
{

class SerializationMap final : public SimpleTextSerialization
{
private:
    SerializationPtr key;
    SerializationPtr value;

    /// 'nested' is an Array(Tuple(key_type, value_type))
    SerializationPtr nested;
    const size_t num_shards;

public:
    SerializationMap(
        const SerializationPtr & key_,
        const SerializationPtr & value_,
        const SerializationPtr & nested_,
        size_t num_shards_);

    size_t getNumShards() const { return num_shards; }

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &, bool whole) const override;
    bool tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &, bool whole) const override;
    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextJSONPretty(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const override;
    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

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
    struct SubcolumnCreator : public ISubcolumnCreator
    {
        const String shard_name;
        const size_t num_shards;

        SubcolumnCreator(const String & shard_name_, size_t num_shards_)
            :  shard_name(shard_name_), num_shards(num_shards_)
        {
        }

        void create(SubstreamData & data, std::string_view name) const override;
    };

    template <typename KeyWriter, typename ValueWriter>
    void serializeTextImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, KeyWriter && key_writer, ValueWriter && value_writer) const;

    template <typename ReturnType = void, typename Reader>
    ReturnType deserializeTextImpl(IColumn & column, ReadBuffer & istr, Reader && reader) const;
};

class SerializationMapSubcolumn : public SerializationWrapper
{
protected:
    const size_t num_shards;

public:
    SerializationMapSubcolumn(SerializationPtr nested_, size_t num_shards_);

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

protected:
    static void assertSettings(const ISerialization::SerializeBinaryBulkSettings & settings);
};

class SerializationMapKeysValues : public SerializationMapSubcolumn
{
public:
    SerializationMapKeysValues(SerializationPtr nested_, size_t num_shards_);

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;
};

class SerializationMapSize : public SerializationMapSubcolumn
{
public:
    SerializationMapSize(size_t num_shards_, const String & subcolumn_name_);

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;
};

}
