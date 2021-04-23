#pragma once

#include <DataTypes/Serializations/SimpleTextSerialization.h>

namespace DB
{

template <typename Parser>
class SerializationObject : public SimpleTextSerialization
{
public:
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

    /** Format is platform-dependent. */
    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;
    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

private:
    mutable Parser parser;
};

SerializationPtr getObjectSerialization(const String & schema_format);

}
