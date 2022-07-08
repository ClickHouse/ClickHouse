#pragma once

#include <DataTypes/Serializations/SerializationCustomSimpleText.h>

namespace DB
{

class SerializationBase58 final : public SerializationCustomSimpleText
{
public:
    explicit SerializationBase58(const SerializationPtr & nested_);

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const override;
};

}
