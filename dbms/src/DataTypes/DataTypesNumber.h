#pragma once

#include <type_traits>
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
    bool isUnsignedInteger() const override { return isInteger() && std::is_unsigned_v<T>; }
    bool canBeUsedInBooleanContext() const override { return true; }
    bool isNumber() const override { return true; }
    bool isInteger() const override { return std::is_integral_v<T>; }
    bool canBeInsideNullable() const override { return true; }
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

}
