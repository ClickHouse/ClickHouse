#pragma once

#include <DataTypes/Serializations/SimpleTextSerialization.h>
#include <Columns/ColumnDecimal.h>

namespace DB
{

template <typename T>
class SerializationDecimalBase : public SimpleTextSerialization
{
protected:
    const UInt32 precision;
    const UInt32 scale;

public:
    using FieldType = T;
    using ColumnType = ColumnDecimal<T>;

    SerializationDecimalBase(UInt32 precision_, UInt32 scale_)
        : precision(precision_), scale(scale_) {}

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;

    void deserializeBinary(Field & field, ReadBuffer & istr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;
};

}
