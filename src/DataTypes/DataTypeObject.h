#pragma once

#include <DataTypes/IDataType.h>
#include <Core/Field.h>
#include <Columns/ColumnObject.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class DataTypeObject : public IDataType
{
private:
    String schema_format;
    SerializationPtr default_serialization;

public:
    DataTypeObject(const String & schema_format_);

    const char * getFamilyName() const override { return "Object"; }
    String doGetName() const override;
    TypeIndex getTypeId() const override { return TypeIndex::Nothing; }

    MutableColumnPtr createColumn() const override { return ColumnObject::create(); }

    Field getDefault() const override
    {
        throw Exception("Method getDefault() is not implemented for data type " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    bool haveSubtypes() const override { return false; }
    bool equals(const IDataType & rhs) const override;
    bool isParametric() const override { return true; }

    SerializationPtr doGetDefaultSerialization() const override;
};

}
