#pragma once

#include <base/TypeName.h>
#include <Core/TypeId.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationNumber.h>


namespace DB
{

/** Implements part of the IDataType interface, common to all numbers and for Date and DateTime.
  */
template <typename T>
class DataTypeNumberBase : public IDataType
{
    static_assert(is_arithmetic_v<T>);

public:
    static constexpr bool is_parametric = false;
    static constexpr auto family_name = TypeName<T>;
    static constexpr auto type_id = TypeToTypeIndex<T>;

    using FieldType = T;
    using ColumnType = ColumnVector<T>;

    const char * getFamilyName() const override { return TypeName<T>.data(); }
    TypeIndex getTypeId() const override { return TypeToTypeIndex<T>; }

    Field getDefault() const override;

    MutableColumnPtr createColumn() const override;

    bool isParametric() const override { return false; }
    bool haveSubtypes() const override { return false; }

    bool shouldAlignRightInPrettyFormats() const override
    {
        /// Just a number, without customizations. Counterexample: IPv4.
        return !custom_serialization;
    }

    bool textCanContainOnlyValidUTF8() const override { return true; }
    bool isComparable() const override { return true; }
    bool isValueRepresentedByNumber() const override { return true; }
    bool isValueRepresentedByInteger() const override;
    bool isValueRepresentedByUnsignedInteger() const override;
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool haveMaximumSizeOfValue() const override { return true; }
    size_t getSizeOfValueInMemory() const override { return sizeof(T); }
    bool isCategorial() const override { return isValueRepresentedByInteger(); }
    bool canBeInsideLowCardinality() const override { return true; }

    SerializationPtr doGetDefaultSerialization() const override { return std::make_shared<SerializationNumber<T>>(); }
};

/// Prevent implicit template instantiation of DataTypeNumberBase for common numeric types

extern template class DataTypeNumberBase<UInt8>;
extern template class DataTypeNumberBase<UInt16>;
extern template class DataTypeNumberBase<UInt32>;
extern template class DataTypeNumberBase<UInt64>;
extern template class DataTypeNumberBase<UInt128>;
extern template class DataTypeNumberBase<UInt256>;
extern template class DataTypeNumberBase<Int16>;
extern template class DataTypeNumberBase<Int8>;
extern template class DataTypeNumberBase<Int32>;
extern template class DataTypeNumberBase<Int64>;
extern template class DataTypeNumberBase<Int128>;
extern template class DataTypeNumberBase<Int256>;
extern template class DataTypeNumberBase<Float32>;
extern template class DataTypeNumberBase<Float64>;

}
