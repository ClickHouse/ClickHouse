#pragma once

#include <Core/Types.h>
#include <Common/PODArray_fwd.h>
#include <DataTypes/Serializations/SimpleTextSerialization.h>
#include <base/TypeName.h>

namespace DB
{

template <typename T>
class ColumnVector;

template <typename T>
class SerializationNumber : public SimpleTextSerialization
{
    static_assert(is_arithmetic_v<T>);

protected:
    SerializationNumber() = default;

public:
    using FieldType = T;
    using ColumnType = ColumnVector<T>;

    static UInt128 getHash();
    static SerializationPtr create();

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const final;
    bool tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const final;
    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeTextHive(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;

    /** Format is platform-dependent. */
    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const final;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const final;

    /// Bulk (de)serialization straight from/into a raw value container. Reused by the IColumn
    /// overloads above and by callers that keep the values outside a ColumnVector - for example the
    /// offsets of a String column, which are sent as-is over the native protocol.
    static void serializeBinaryBulk(const PaddedPODArray<T> & x, WriteBuffer & ostr, size_t offset, size_t limit);
    static void deserializeBinaryBulk(PaddedPODArray<T> & x, ReadBuffer & istr, size_t limit);
};

}
