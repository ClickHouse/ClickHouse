#pragma once

#include <DataTypes/Serializations/SerializationCustomSimpleText.h>

namespace DB
{

class SerializationIPv4 final : public SerializationCustomSimpleText
{
public:
    SerializationIPv4(const SerializationPtr & nested_);

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
};

class SerializationIPv6 : public SerializationCustomSimpleText
{
public:
    SerializationIPv6(const SerializationPtr & nested_);

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
};

}
