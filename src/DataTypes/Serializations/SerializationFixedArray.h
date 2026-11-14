#pragma once

#include <DataTypes/Serializations/SimpleTextSerialization.h>

namespace DB
{

class SerializationArray;

/** Serialization of `FixedArray(N, T)`.
  *
  * The binary formats contain only the elements, because their number is known from the type:
  * a row is `N` consecutive values of `T`, and the bulk serialization is the bulk serialization of the
  * elements, without the stream of sizes.
  *
  * The text formats are the same as for `Array`, with a check that the number of elements is `N`.
  */
class SerializationFixedArray final : public SimpleTextSerialization
{
private:
    SerializationPtr nested;
    /// The text formats are delegated to the serialization of `Array`, which supports `FixedArray` columns.
    std::shared_ptr<const SerializationArray> array_serialization;
    size_t n;
    bool nested_supports_pooling;

    SerializationFixedArray(const SerializationPtr & nested_, size_t n_);

public:
    static UInt128 getHash(const SerializationPtr & nested_, size_t n_);
    static SerializationPtr create(const SerializationPtr & nested_, size_t n_);
    bool supportsPooling() const override { return nested_supports_pooling; }

    const SerializationPtr & getNestedSerialization() const { return nested; }
    size_t getArraySize() const { return n; }

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeForHashCalculation(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const override;
    bool tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeTextJSONPretty(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const override;

    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeTextHive(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

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

    /// Wraps subcolumns of the elements into `FixedArray` of the same size.
    struct SubcolumnCreator : public ISubcolumnCreator
    {
        const size_t n;

        explicit SubcolumnCreator(size_t n_) : n(n_) {}

        DataTypePtr create(const DataTypePtr & prev) const override;
        SerializationPtr create(const SerializationPtr & prev, const DataTypePtr &) const override;
        ColumnPtr create(const ColumnPtr & prev) const override;
    };
};

}
