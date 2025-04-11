#pragma once

#include <DataTypes/IDataType.h>
#include <Core/Field.h>
#include <Columns/ColumnObjectDeprecated.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class DataTypeObjectDeprecated : public IDataType
{
private:
    String schema_format;
    bool is_nullable;

public:
    DataTypeObjectDeprecated(const String & schema_format_, bool is_nullable_);

    const char * getFamilyName() const override { return "Object"; }
    String doGetName() const override;
    TypeIndex getTypeId() const override { return TypeIndex::ObjectDeprecated; }

    MutableColumnPtr createColumn() const override { return ColumnObjectDeprecated::create(is_nullable); }

    Field getDefault() const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getDefault() is not implemented for data type {}", getName());
    }

    bool haveSubtypes() const override { return false; }
    bool equals(const IDataType & rhs) const override;
    bool isParametric() const override { return true; }
    bool hasDynamicSubcolumnsDeprecated() const override { return true; }

    SerializationPtr doGetDefaultSerialization() const override;

    bool hasNullableSubcolumns() const { return is_nullable; }

    const String & getSchemaFormat() const { return schema_format; }
};

}
