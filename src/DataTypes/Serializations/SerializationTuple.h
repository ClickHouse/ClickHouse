#pragma once

#include <DataTypes/Serializations/SimpleTextSerialization.h>
#include <DataTypes/Serializations/SerializationNamed.h>

namespace DB
{

class SerializationTuple final : public SimpleTextSerialization
{
public:
    using ElementSerializationPtr = std::shared_ptr<const SerializationNamed>;
    using ElementSerializations = std::vector<ElementSerializationPtr>;

    SerializationTuple(const ElementSerializations & elems_, bool have_explicit_names_)
        : elems(elems_), have_explicit_names(have_explicit_names_)
    {
    }

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &, bool whole) const override;
    bool tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &, bool whole) const override;
    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextJSONPretty(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const override;
    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;

    /// Tuples in CSV format will be serialized as separate columns (that is, losing their nesting in the tuple).
    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    /** Each sub-column in a tuple is serialized in separate stream.
      */
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

    const ElementSerializations & getElementsSerializations() const { return elems; }

private:
    ElementSerializations elems;
    bool have_explicit_names;

    size_t getPositionByName(const String & name) const;

    template <typename ReturnType = void>
    ReturnType deserializeTextImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const;

    template <typename ReturnType>
    ReturnType deserializeTupleJSONImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, auto && deserialize_element) const;

    template <typename ReturnType>
    ReturnType deserializeTextJSONImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const;

    template <typename ReturnType = void>
    ReturnType deserializeTextCSVImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const;
};

}
