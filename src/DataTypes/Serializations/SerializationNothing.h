#pragma once

#include <DataTypes/Serializations/SimpleTextSerialization.h>

namespace DB
{

class SerializationNothing : public SimpleTextSerialization
{
public:
    /// These methods are used to serde empty tuples: Tuple(Nothing).
    void serializeBinary(const Field &, WriteBuffer &, const FormatSettings &) const override {}
    void deserializeBinary(Field &, ReadBuffer &, const FormatSettings &) const override;
    void serializeBinary(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override {}
    void deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const override;
    void serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override {}
    void deserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override;

    /// These methods read and write zero bytes just to allow to figure out size of column.
    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;
};

}
