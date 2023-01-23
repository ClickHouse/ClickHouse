#pragma once

#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

class ColumnSparse;

/** Serialization for sparse representation.
 *  Only '{serialize,deserialize}BinaryBulk' makes sense.
 *  Format:
 *    Values and offsets are written to separate substreams.
 *    There are written only non-default values.
 *
 *    Offsets have position independent format: as i-th offset there
 *    is written number of default values, that precedes the i-th non-default value.
 *    Offsets are written in VarInt encoding.
 *    Additionally at the end of every call of 'serializeBinaryBulkWithMultipleStreams'
 *    there is written number of default values in the suffix of part of column,
 *    that we currently writing. This value also marked with a flag, that means the end of portion of data.
 *    This value is used, e.g. to allow independent reading of granules in MergeTree.
 *
 * Sparse serialization of Nullable columns is supported too.
 * In that case NULL  is used as implicit default value,
 * null map is not written but can be restored from offsets.
 */
class SerializationSparse final : public ISerialization
{
public:
    explicit SerializationSparse(const SerializationPtr & nested_);

    Kind getKind() const override { return Kind::SPARSE; }

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
        DeserializeBinaryBulkStatePtr & state) const override;

    /// Allows to write ColumnSparse and full columns in sparse serialization.
    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    /// Allows to read only ColumnSparse.
    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const override;

    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;

private:
    const SerializationPtr & getSerializationForMultipleStreams() const
    {
        return nested_non_nullable ? nested_non_nullable : nested;
    }

    /// Deserialized values of ColumnSparse.
    void deserializeValuesWithMultipleStreams(
        ColumnPtr & values,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state) const;

    /// Serializes ColumnSparse in sparse serialization.
    void serializeWithMultipleStreamsSparse(
        const ColumnSparse & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const;

    /// Serializes full column in sparse serialization.
    void serializeWithMultipleStreamsGeneric(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const;

    /// Serializes non-default values of sparse column.
    void serializeValuesWithMultipleStreams(
        const IColumn & values,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const;

    struct SubcolumnCreator : public ISubcolumnCreator
    {
        const ColumnPtr offsets;
        const size_t size;

        SubcolumnCreator(const ColumnPtr & offsets_, size_t size_)
            : offsets(offsets_), size(size_) {}

        DataTypePtr create(const DataTypePtr & prev) const override;
        SerializationPtr create(const SerializationPtr & prev) const override;
        ColumnPtr create(const ColumnPtr & prev) const override;
    };

    struct NullMapSubcolumnCreator : public ISubcolumnCreator
    {
        const ColumnPtr offsets;
        const size_t size;

        NullMapSubcolumnCreator(const ColumnPtr & offsets_, size_t size_)
            : offsets(offsets_), size(size_) {}

        DataTypePtr create(const DataTypePtr & prev) const override;
        SerializationPtr create(const SerializationPtr & prev) const override;
        ColumnPtr create(const ColumnPtr & prev) const override;
    };

    /// Serialization of nested column.
    SerializationPtr nested;

    /// Serialization of nested non-nullable column.
    /// If @nested is not Nullable then nullptr.
    SerializationPtr nested_non_nullable;
};


/// Special serialization that allows to read ".null" (which represents null map)
/// subcolumn from Nullable column written in Sparse serialziaton.
/// It modifies only 'enumerateStreams' and 'deserializeBinaryBulkWithMultipleStreams'
/// methods and builds a null map in full serialization from offset of Sparse column.
class SerializationSparseNullMap final : public SerializationNumber<UInt8>
{
public:
    using Base = SerializationNumber<UInt8>;

    void serializeBinaryBulkStatePrefix(
        const IColumn & column,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        ColumnPtr & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

private:
    static void assertSettings(const SerializeBinaryBulkSettings & settings);
};

}
