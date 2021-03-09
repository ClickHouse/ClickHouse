#pragma once

#include <Common/UInt128.h>
#include <DataTypes/DataTypeNumberBase.h>
#include <DataTypes/IDataType.h>

namespace DB
{

class DataTypeUUID final : public DataTypeNumberBase<UInt128>
{

public:
    const char * getFamilyName() const override { return "UUID"; }
    TypeIndex getTypeId() const override { return TypeIndex::UUID; }

    bool equals(const IDataType & rhs) const override;

    bool canBeUsedInBitOperations() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
    bool canBeInsideLowCardinality() const override { return false; }

    bool canBePromoted() const override { return false; }

    SerializationPtr doGetDefaultSerialization() const override;
};

}
