#pragma once

#include <type_traits>
#include <Core/Field.h>
#include <DataTypes/DataTypeNumberBase.h>
#include <DataTypes/Serializations/SerializationNumber.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename T>
class DataTypeNumber final : public DataTypeNumberBase<T>
{
public:
    DataTypeNumber() = default;

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

    SerializationPtr doGetDefaultSerialization() const override
    {
        return std::make_shared<SerializationNumber<T>>();
    }

    /// Special constructor for unsigned integers that can also fit into signed integer.
    /// It's used for better type inference from fields.
    /// See getLeastSupertype.cpp::convertUInt64toInt64IfPossible and FieldToDataType.cpp
    explicit DataTypeNumber(bool unsigned_can_be_signed_) : DataTypeNumberBase<T>(), unsigned_can_be_signed(unsigned_can_be_signed_)
    {
        if constexpr (std::is_signed_v<T>)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "DataTypeNumber constructor with bool argument should not be used with signed integers");
    }

    bool canUnsignedBeSigned() const { return unsigned_can_be_signed; }

private:
    bool unsigned_can_be_signed = false;
};

extern template class DataTypeNumber<UInt8>;
extern template class DataTypeNumber<UInt16>;
extern template class DataTypeNumber<UInt32>;
extern template class DataTypeNumber<UInt64>;
extern template class DataTypeNumber<Int8>;
extern template class DataTypeNumber<Int16>;
extern template class DataTypeNumber<Int32>;
extern template class DataTypeNumber<Int64>;
extern template class DataTypeNumber<Float32>;
extern template class DataTypeNumber<Float64>;

extern template class DataTypeNumber<UInt128>;
extern template class DataTypeNumber<Int128>;
extern template class DataTypeNumber<UInt256>;
extern template class DataTypeNumber<Int256>;

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

using DataTypeUInt128 = DataTypeNumber<UInt128>;
using DataTypeInt128 = DataTypeNumber<Int128>;
using DataTypeUInt256 = DataTypeNumber<UInt256>;
using DataTypeInt256 = DataTypeNumber<Int256>;

bool isUInt64ThatCanBeInt64(const DataTypePtr & type);

}
