#pragma once

#include <type_traits>
#include <Core/Field.h>
#include <DataTypes/DataTypeNumberBase.h>


namespace DB
{

template <typename T>
class DataTypeNumber final : public DataTypeNumberBase<T>
{
    bool equals(const IDataType & rhs) const override { return typeid(rhs) == typeid(*this); }

    bool canBeUsedAsVersion() const override { return true; }
    bool isSummable() const override { return true; }
    bool canBeUsedInBitOperations() const override { return true; }
    bool canBeUsedInBooleanContext() const override { return true; }
    bool canBeInsideNullable() const override { return true; }

    bool canBePromoted() const override { return true; }
    DataTypePtr promoteNumericType() const override
    {
        using PromotedType = DataTypeNumber<NearestFieldType<T>>;
        return std::make_shared<PromotedType>();
    }
};

using DataTypeUInt8 = DataTypeNumber<UInt8>;
using DataTypeUInt16 = DataTypeNumber<UInt16>;
using DataTypeUInt32 = DataTypeNumber<UInt32>;
using DataTypeUInt64 = DataTypeNumber<UInt64>;
using DataTypeInt8 = DataTypeNumber<Int8>;
using DataTypeInt16 = DataTypeNumber<Int16>;
using DataTypeInt32 = DataTypeNumber<Int32>;
using DataTypeInt64 = DataTypeNumber<Int64>;
using DataTypeFloat32 = DataTypeNumber<Float32>;
using DataTypeFloat64 = DataTypeNumber<Float64>;

using DataTypeInt128 = DataTypeNumber<Int128>;
using DataTypeUInt256 = DataTypeNumber<UInt256>;
using DataTypeInt256 = DataTypeNumber<Int256>;

}
