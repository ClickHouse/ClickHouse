#pragma once

#include <Core/Types.h>
#include <DataTypes/Serializations/SimpleTextSerialization.h>

namespace DB
{

template <typename T>
class ColumnVector;

template <typename T>
class SerializationNumber : public SimpleTextSerialization
{
    static_assert(is_arithmetic_v<T>);

public:
    using FieldType = T;
    using ColumnType = ColumnVector<T>;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const override;
    bool tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const override;
    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    bool tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    bool tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    /** Format is platform-dependent. */
    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;
};

}
