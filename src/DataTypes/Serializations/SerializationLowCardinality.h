#pragma once

#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

class SerializationLowCardinality final : public ISerialization
{
private:
    DataTypePtr dictionary_type;
    /// Serialization of the nested column.
    SerializationPtr nested_serialization;
    /// Serialization of the dictionary type.
    /// It can differ from the nested serialization for nullable types.
    SerializationPtr dict_inner_serialization;

    explicit SerializationLowCardinality(const DataTypePtr & dictionary_type);

public:
    static UInt128 getHash(const DataTypePtr & dictionary_type);
    static SerializationPtr create(const DataTypePtr & dictionary_type);

    bool supportsPooling() const override { return dict_inner_serialization->supportsPooling(); }

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
            IColumn & column,
            size_t limit,
            DeserializeBinaryBulkSettings & settings,
            DeserializeBinaryBulkStatePtr & state,
            SubstreamsCache * cache) const override;

    /// Reads the whole `DictionaryKeys` stream of a top-level `LowCardinality` column of a `Wide` part.
    /// If the stream ends with the marker written with `low_cardinality_write_complete_dictionaries_marker`,
    /// returns a `LowCardinality` column per dictionary, which refers to the dictionary as it is, without copying
    /// or hashing its keys again. Together they have every value of the column (`NULL` and the default value only
    /// if the rows use them). Otherwise returns `std::nullopt`: the column can have keys that are not
    /// in the dictionaries, or the part was written without the marker.
    std::optional<Columns> deserializeCompleteDictionaries(ReadBuffer & keys_stream) const;

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextRaw(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeTextRaw(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

private:
    template <typename ... Params>
    using SerializeFunctionPtr = void (ISerialization::*)(const IColumn &, size_t, Params ...) const;

    template <typename... Params, typename... Args>
    void serializeImpl(const IColumn & column, size_t row_num, SerializeFunctionPtr<Params...> func, Args &&... args) const;

    template <typename ... Params>
    using DeserializeFunctionPtr = void (ISerialization::*)(IColumn &, Params ...) const;

    template <typename ... Params, typename... Args>
    void deserializeImpl(IColumn & column, DeserializeFunctionPtr<Params...> func, Args &&... args) const;

    template <typename ... Params>
    using TryDeserializeFunctionPtr = bool (ISerialization::*)(IColumn &, Params ...) const;

    template <typename ... Params, typename... Args>
    bool tryDeserializeImpl(IColumn & column, TryDeserializeFunctionPtr<Params...> func, Args &&... args) const;
};

}
