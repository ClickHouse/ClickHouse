#pragma once

#include <DataTypes/Serializations/SimpleTextSerialization.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class SerializationNothing : public SimpleTextSerialization
{
private:
    [[noreturn]] static void throwNoSerialization()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization is not implemented for type Nothing");
    }
public:
    void serializeBinary(const Field &, WriteBuffer &, const FormatSettings &) const override                       { throwNoSerialization(); }
    void deserializeBinary(Field &, ReadBuffer &, const FormatSettings &) const override                            { throwNoSerialization(); }
    void serializeBinary(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override             { throwNoSerialization(); }
    void deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const override                          { throwNoSerialization(); }
    void serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override    { throwNoSerialization(); }
    bool tryDeserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override    { throwNoSerialization(); }

    /// These methods read and write zero bytes just to allow to figure out size of column.
    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;
};

}
