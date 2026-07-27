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
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization is not implemented for data type {}", getName());
    }

public:
    MutableColumnPtr createColumn() const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method createColumn is not implemented for data type {}", getName());
    }

    Field getDefault() const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getDefault is not implemented for data type {}", getName());
    }

    void insertDefaultInto(IColumn &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method insertDefaultInto is not implemented for data type {}", getName());
    }

    bool haveSubtypes() const override { return false; }
    bool cannotBeStoredInTables() const override { return true; }

    void updateHashImpl(SipHash &) const override {}

    SerializationPtr doGetDefaultSerialization() const override { throwNoSerialization(); }
};

}
