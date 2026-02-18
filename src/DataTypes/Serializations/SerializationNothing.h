#pragma once

#include <DataTypes/Serializations/SimpleTextSerialization.h>
#include <DataTypes/Serializations/SerializationObjectPool.h>

namespace DB
{

class SerializationNothing : public SimpleTextSerialization
{
private:
    [[noreturn]] static void throwNoSerialization();
    SerializationNothing() = default;

public:
    static SerializationPtr create()
    {
        auto ptr = SerializationPtr(new SerializationNothing());
        return SerializationObjectPool::instance().getOrCreate(ptr->getName(), std::move(ptr));
    }

    ~SerializationNothing() override;

    String getName() const override;

    void serializeBinary(const Field &, WriteBuffer &, const FormatSettings &) const override                       { throwNoSerialization(); }
    void deserializeBinary(Field &, ReadBuffer &, const FormatSettings &) const override                            { throwNoSerialization(); }
    void serializeBinary(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override             { throwNoSerialization(); }
    void deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const override                          { throwNoSerialization(); }
    void serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override    { throwNoSerialization(); }
    bool tryDeserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override    { throwNoSerialization(); }

    /// These methods read and write zero bytes just to allow to figure out size of column.
    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double avg_value_size_hint) const override;
};

}
