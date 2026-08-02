#pragma once

#include <DataTypes/Serializations/ISerialization.h>

#include <DataTypes/DataTypeInterval.h>
#include <Formats/FormatSettings.h>
#include <Common/IntervalKind.h>


namespace DB
{

class SerializationInterval : public SerializationNumber<typename DataTypeInterval::FieldType>
{
public:
    explicit SerializationInterval(IntervalKind kind_);
    void serializeText(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void serializeTextJSON(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void serializeTextCSV(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void serializeTextQuoted(const IColumn & column, size_t row, WriteBuffer & ostr, const FormatSettings & settings) const override;
private:
    using Base = SerializationNumber<typename DataTypeInterval::FieldType>;
    IntervalKind interval_kind;
};

}
