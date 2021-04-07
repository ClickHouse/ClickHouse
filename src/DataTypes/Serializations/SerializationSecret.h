#pragma once

#include <DataTypes/Serializations/SerializationNothing.h>

namespace DB
{

class SerializationSecret final : public SerializationNothing
{
public:
    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double /*avg_value_size_hint*/) const override;

};

}
