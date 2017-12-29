#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

/** The base class for data types that do not support serialization and deserialization,
  *  but arise only as an intermediate result of the calculations.
  *
  * That is, this class is used just to distinguish the corresponding data type from the others.
  */
class IDataTypeDummy : public IDataType
{
private:
    void throwNoSerialization() const
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
    void serializeText(const IColumn &, size_t, WriteBuffer &) const override               { throwNoSerialization(); }
    void serializeTextEscaped(const IColumn &, size_t, WriteBuffer &) const override        { throwNoSerialization(); }
    void deserializeTextEscaped(IColumn &, ReadBuffer &) const override                     { throwNoSerialization(); }
    void serializeTextQuoted(const IColumn &, size_t, WriteBuffer &) const override         { throwNoSerialization(); }
    void deserializeTextQuoted(IColumn &, ReadBuffer &) const override                      { throwNoSerialization(); }
    void serializeTextJSON(const IColumn &, size_t, WriteBuffer &, const FormatSettingsJSON &) const override { throwNoSerialization(); }
    void deserializeTextJSON(IColumn &, ReadBuffer &) const override                        { throwNoSerialization(); }
    void serializeTextCSV(const IColumn &, size_t, WriteBuffer &) const override            { throwNoSerialization(); }
    void deserializeTextCSV(IColumn &, ReadBuffer &, const char) const override             { throwNoSerialization(); }

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

