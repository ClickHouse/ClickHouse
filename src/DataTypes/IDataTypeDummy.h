#pragma once

#include <DataTypes/DataTypeWithSimpleSerialization.h>
#include <Core/Field.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/** The base class for data types that do not support serialization and deserialization,
  *  but arise only as an intermediate result of the calculations.
  *
  * That is, this class is used just to distinguish the corresponding data type from the others.
  */
class IDataTypeDummy : public DataTypeWithSimpleSerialization
{
private:
    [[noreturn]] void throwNoSerialization() const
    {
        throw Exception("Serialization is not implemented for data type " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

public:
    void serializeBinary(const Field &, WriteBuffer &) const override                       { throwNoSerialization(); }
    void deserializeBinary(Field &, ReadBuffer &) const override                            { throwNoSerialization(); }
    void serializeBinary(const IColumn &, size_t, WriteBuffer &) const override             { throwNoSerialization(); }
    void deserializeBinary(IColumn &, ReadBuffer &) const override                          { throwNoSerialization(); }
    void serializeBinaryBulk(const IColumn &, WriteBuffer &, size_t, size_t) const override { throwNoSerialization(); }
    void deserializeBinaryBulk(IColumn &, ReadBuffer &, size_t, double) const override      { throwNoSerialization(); }
    void serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeText(IColumn &, ReadBuffer &, const FormatSettings &) const override    { throwNoSerialization(); }
    void serializeProtobuf(const IColumn &, size_t, ProtobufWriter &, size_t &) const override { throwNoSerialization(); }
    void deserializeProtobuf(IColumn &, ProtobufReader &, bool, bool &) const override      { throwNoSerialization(); }

    MutableColumnPtr createColumn() const override
    {
        throw Exception("Method createColumn() is not implemented for data type " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    Field getDefault() const override
    {
        throw Exception("Method getDefault() is not implemented for data type " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertDefaultInto(IColumn &) const override
    {
        throw Exception("Method insertDefaultInto() is not implemented for data type " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    bool haveSubtypes() const override { return false; }
    bool cannotBeStoredInTables() const override { return true; }
};

}
