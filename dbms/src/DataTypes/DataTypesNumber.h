#pragma once

#include <DataTypes/DataTypeNumberBase.h>
#include <DataTypes/IDataTypeDummy.h>


namespace DB
{

template <typename T>
class DataTypeNumber final : public DataTypeNumberBase<T>
{
    DataTypePtr clone() const override { return std::make_shared<DataTypeNumber<T>>(); }
};

using DataTypeUInt8 = DataTypeNumber<UInt8>;
using DataTypeUInt16 = DataTypeNumber<UInt16>;
using DataTypeUInt32 = DataTypeNumber<UInt32>;
using DataTypeUInt64 = DataTypeNumber<UInt64>;
using DataTypeUInt128 = DataTypeNumber<UInt128>;
using DataTypeInt8 = DataTypeNumber<Int8>;
using DataTypeInt16 = DataTypeNumber<Int16>;
using DataTypeInt32 = DataTypeNumber<Int32>;
using DataTypeInt64 = DataTypeNumber<Int64>;
using DataTypeFloat32 = DataTypeNumber<Float32>;
using DataTypeFloat64 = DataTypeNumber<Float64>;


/// Used only to indicate error case in calculations on data types.

template <>
class DataTypeNumber<void> final : public IDataTypeDummy
{
public:
    using FieldType = void;

    std::string getName() const override { return "Void"; }
    DataTypePtr clone() const override { return std::make_shared<DataTypeNumber<void>>(); }
};

using DataTypeVoid = DataTypeNumber<void>;

template <>
class DataTypeNumber<Null> final : public IDataTypeDummy
{
public:
    using FieldType = Null;

    std::string getName() const override { return "Null"; }
    DataTypePtr clone() const override { return std::make_shared<DataTypeNumber<Null>>(); }
};

}
