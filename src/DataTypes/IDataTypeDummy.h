#pragma once

#include <DataTypes/IDataType.h>
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
class IDataTypeDummy : public IDataType
{
private:
    [[noreturn]] void throwNoSerialization() const
    {
        throw Exception("Serialization is not implemented for data type " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

public:
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

    SerializationPtr doGetDefaultSerialization() const override { throwNoSerialization(); }
};

}
