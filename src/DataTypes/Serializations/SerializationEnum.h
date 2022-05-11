#pragma once

#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/EnumValues.h>

namespace DB
{

template <typename Type>
class SerializationEnum : public SerializationNumber<Type>, public EnumValues<Type>
{
public:
    using typename SerializationNumber<Type>::FieldType;
    using typename SerializationNumber<Type>::ColumnType;
    using typename EnumValues<Type>::Values;

    SerializationEnum(const Values & values_) : EnumValues<Type>(values_) {}

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    FieldType readValue(ReadBuffer & istr) const
    {
        FieldType x;
        readText(x, istr);
        return this->findByValue(x)->first;
    }
};

}
