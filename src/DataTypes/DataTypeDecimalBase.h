#pragma once

#include <cmath>
#include <type_traits>

#include <Columns/ColumnDecimal.h>
#include <Core/TypeId.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Common/SipHash.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

bool decimalCheckComparisonOverflow(ContextPtr context);
bool decimalCheckArithmeticOverflow(ContextPtr context);

inline UInt32 leastDecimalPrecisionFor(TypeIndex int_type)
{
    switch (int_type)
    {
        case TypeIndex::Int8: [[fallthrough]];
        case TypeIndex::UInt8:
            return 3;
        case TypeIndex::Int16: [[fallthrough]];
        case TypeIndex::UInt16:
            return 5;
        case TypeIndex::Int32: [[fallthrough]];
        case TypeIndex::UInt32:
            return 10;
        case TypeIndex::Int64:
            return 19;
        case TypeIndex::UInt64:
            return 20;
        default:
            break;
    }
    return 0;
}

/// Base class for decimals, like Decimal(P, S), where P is precision, S is scale.
/// Maximum precisions for underlying types are:
/// Int32    9
/// Int64   18
/// Int128  38
/// Int256  76
/// Operation between two decimals leads to Decimal(P, S), where
///     P is one of (9, 18, 38, 76); equals to the maximum precision for the biggest underlying type of operands.
///     S is maximum scale of operands. The allowed valuas are [0, precision]
template <is_decimal T>
class DataTypeDecimalBase : public IDataType
{
public:
    using FieldType = T;
    using ColumnType = ColumnDecimal<T>;
    static constexpr auto type_id = TypeToTypeIndex<T>;

    static constexpr bool is_parametric = true;

    static constexpr size_t maxPrecision();

    DataTypeDecimalBase(UInt32 precision_, UInt32 scale_)
    :   precision(precision_),
        scale(scale_)
    {
        if (unlikely(precision < 1 || precision > maxPrecision()))
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                            "Precision {} is out of bounds (precision range: [1, {}])",
                            std::to_string(precision), maxPrecision());
        if (unlikely(scale > maxPrecision()))
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Scale {} is out of bounds (max scale: {})",
                            std::to_string(scale), maxPrecision());
    }

    TypeIndex getTypeId() const override { return TypeToTypeIndex<T>; }

    Field getDefault() const override;
    MutableColumnPtr createColumn() const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return false; }
    bool shouldAlignRightInPrettyFormats() const override { return true; }
    bool textCanContainOnlyValidUTF8() const override { return true; }
    bool isComparable() const override { return true; }
    bool isValueRepresentedByNumber() const override { return true; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool haveMaximumSizeOfValue() const override { return true; }
    size_t getSizeOfValueInMemory() const override { return sizeof(T); }

    bool isSummable() const override { return true; }
    bool canBeUsedInBooleanContext() const override { return true; }
    bool canBeInsideNullable() const override { return true; }

    /// Decimal specific

    UInt32 getPrecision() const { return precision; }
    UInt32 getScale() const { return scale; }
    T getScaleMultiplier() const { return getScaleMultiplier(scale); }

    T wholePart(T x) const;
    T fractionalPart(T x) const;

    T maxWholeValue() const { return getScaleMultiplier(precision - scale) - T(1); }

    template <typename U>
    bool canStoreWhole(U x) const
    {
        static_assert(is_signed_v<typename T::NativeType>);
        T max = maxWholeValue();
        if constexpr (is_signed_v<U>)
            return -max.value <= x && x <= max.value;
        else
            return x <= static_cast<make_unsigned_t<typename T::NativeType>>(max.value);
    }

    /// @returns multiplier for U to become T with correct scale
    template <typename U>
    T scaleFactorFor(const DataTypeDecimalBase<U> & x, bool) const
    {
        if (getScale() < x.getScale())
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Decimal result's scale is less than argument's one");
        UInt32 scale_delta = getScale() - x.getScale(); /// scale_delta >= 0
        return getScaleMultiplier(scale_delta);
    }

    template <typename U>
    T scaleFactorFor(const DataTypeNumber<U> & , bool is_multiply_or_divisor) const
    {
        if (is_multiply_or_divisor)
            return T(1);
        return getScaleMultiplier();
    }

    static T getScaleMultiplier(UInt32 scale);

    void updateHashImpl(SipHash & hash) const override
    {
        hash.update(precision);
        hash.update(scale);
    }

protected:
    const UInt32 precision;
    const UInt32 scale;
};


template <typename T>
inline const DataTypeDecimalBase<T> * checkDecimalBase(const IDataType & data_type)
{
    if (isColumnedAsDecimalT<T>(data_type))
        return static_cast<const DataTypeDecimalBase<T> *>(&data_type);

    return nullptr;
}

template <> constexpr size_t DataTypeDecimalBase<Decimal32>::maxPrecision() { return 9; };
template <> constexpr size_t DataTypeDecimalBase<Decimal64>::maxPrecision() { return 18; };
template <> constexpr size_t DataTypeDecimalBase<DateTime64>::maxPrecision() { return 18; };
template <> constexpr size_t DataTypeDecimalBase<Time64>::maxPrecision() { return 18; };
template <> constexpr size_t DataTypeDecimalBase<Decimal128>::maxPrecision() { return 38; };
template <> constexpr size_t DataTypeDecimalBase<Decimal256>::maxPrecision() { return 76; };

extern template class DataTypeDecimalBase<Decimal32>;
extern template class DataTypeDecimalBase<Decimal64>;
extern template class DataTypeDecimalBase<DateTime64>;
extern template class DataTypeDecimalBase<Time64>;
extern template class DataTypeDecimalBase<Decimal128>;
extern template class DataTypeDecimalBase<Decimal256>;

template <template <typename> typename DecimalType>
inline DataTypePtr createDecimal(UInt64 precision_value, UInt64 scale_value)
{
    if (precision_value < 1 || precision_value > DataTypeDecimalBase<Decimal256>::maxPrecision())
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Wrong precision: it must be between {} and {}, got {}",
                        1, DataTypeDecimalBase<Decimal256>::maxPrecision(), precision_value);

    if (scale_value > precision_value)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Negative scales and scales larger than precision are not supported");

    if (precision_value <= DataTypeDecimalBase<Decimal32>::maxPrecision())
        return std::make_shared<DecimalType<Decimal32>>(precision_value, scale_value);
    if (precision_value <= DataTypeDecimalBase<Decimal64>::maxPrecision())
        return std::make_shared<DecimalType<Decimal64>>(precision_value, scale_value);
    if (precision_value <= DataTypeDecimalBase<Decimal128>::maxPrecision())
        return std::make_shared<DecimalType<Decimal128>>(precision_value, scale_value);
    return std::make_shared<DecimalType<Decimal256>>(precision_value, scale_value);
}

}
