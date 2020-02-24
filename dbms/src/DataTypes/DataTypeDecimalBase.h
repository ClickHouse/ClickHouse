#pragma once
#include <cmath>

#include <common/likely.h>
#include <Columns/ColumnDecimal.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeWithSimpleSerialization.h>

#include <type_traits>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int CANNOT_CONVERT_TYPE;
    extern const int DECIMAL_OVERFLOW;
}

class Context;
bool decimalCheckComparisonOverflow(const Context & context);
bool decimalCheckArithmeticOverflow(const Context & context);

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
/// Operation between two decimals leads to Decimal(P, S), where
///     P is one of (9, 18, 38); equals to the maximum precision for the biggest underlying type of operands.
///     S is maximum scale of operands. The allowed valuas are [0, precision]
template <typename T>
class DataTypeDecimalBase : public DataTypeWithSimpleSerialization
{
    static_assert(IsDecimalNumber<T>);

public:
    using FieldType = T;
    using ColumnType = ColumnDecimal<T>;

    static constexpr bool is_parametric = true;

    static constexpr size_t maxPrecision() { return DecimalUtils::maxPrecision<T>(); }

    DataTypeDecimalBase(UInt32 precision_, UInt32 scale_)
    :   precision(precision_),
        scale(scale_)
    {
        if (unlikely(precision < 1 || precision > maxPrecision()))
            throw Exception("Precision " + std::to_string(precision) + " is out of bounds", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        if (unlikely(scale < 0 || static_cast<UInt32>(scale) > maxPrecision()))
            throw Exception("Scale " + std::to_string(scale) + " is out of bounds", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

    TypeIndex getTypeId() const override { return TypeId<T>::value; }

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

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;

    void deserializeBinary(Field & field, ReadBuffer & istr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

    /// Decimal specific

    UInt32 getPrecision() const { return precision; }
    UInt32 getScale() const { return scale; }
    T getScaleMultiplier() const { return getScaleMultiplier(scale); }

    T wholePart(T x) const
    {
        return DecimalUtils::getWholePart(x, scale);
    }

    T fractionalPart(T x) const
    {
        return DecimalUtils::getFractionalPart(x, scale);
    }

    T maxWholeValue() const { return getScaleMultiplier(maxPrecision() - scale) - T(1); }

    bool canStoreWhole(T x) const
    {
        T max = maxWholeValue();
        if (x > max || x < -max)
            return false;
        return true;
    }

    /// @returns multiplier for U to become T with correct scale
    template <typename U>
    T scaleFactorFor(const DataTypeDecimalBase<U> & x, bool) const
    {
        if (getScale() < x.getScale())
            throw Exception("Decimal result's scale is less than argument's one", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        UInt32 scale_delta = getScale() - x.getScale(); /// scale_delta >= 0
        return getScaleMultiplier(scale_delta);
    }

    template <typename U>
    T scaleFactorFor(const DataTypeNumber<U> & , bool is_multiply_or_divisor) const
    {
        if (is_multiply_or_divisor)
            return 1;
        return getScaleMultiplier();
    }

    static T getScaleMultiplier(UInt32 scale);

protected:
    const UInt32 precision;
    const UInt32 scale;
};


template <typename T, typename U, template <typename> typename DecimalType>
typename std::enable_if_t<(sizeof(T) >= sizeof(U)), DecimalType<T>>
decimalResultType(const DecimalType<T> & tx, const DecimalType<U> & ty, bool is_multiply, bool is_divide)
{
    UInt32 scale = (tx.getScale() > ty.getScale() ? tx.getScale() : ty.getScale());
    if (is_multiply)
        scale = tx.getScale() + ty.getScale();
    else if (is_divide)
        scale = tx.getScale();
    return DecimalType<T>(DecimalUtils::maxPrecision<T>(), scale);
}

template <typename T, typename U, template <typename> typename DecimalType>
typename std::enable_if_t<(sizeof(T) < sizeof(U)), const DecimalType<U>>
decimalResultType(const DecimalType<T> & tx, const DecimalType<U> & ty, bool is_multiply, bool is_divide)
{
    UInt32 scale = (tx.getScale() > ty.getScale() ? tx.getScale() : ty.getScale());
    if (is_multiply)
        scale = tx.getScale() * ty.getScale();
    else if (is_divide)
        scale = tx.getScale();
    return DecimalType<U>(DecimalUtils::maxPrecision<U>(), scale);
}

template <typename T, typename U, template <typename> typename DecimalType>
const DecimalType<T> decimalResultType(const DecimalType<T> & tx, const DataTypeNumber<U> &, bool, bool)
{
    return DecimalType<T>(DecimalUtils::maxPrecision<T>(), tx.getScale());
}

template <typename T, typename U, template <typename> typename DecimalType>
const DecimalType<U> decimalResultType(const DataTypeNumber<T> &, const DecimalType<U> & ty, bool, bool)
{
    return DecimalType<U>(DecimalUtils::maxPrecision<U>(), ty.getScale());
}

template <template <typename> typename DecimalType>
DataTypePtr createDecimal(UInt64 precision_value, UInt64 scale_value)
{
    if (precision_value < DecimalUtils::minPrecision() || precision_value > DecimalUtils::maxPrecision<Decimal128>())
        throw Exception("Wrong precision", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    if (static_cast<UInt64>(scale_value) > precision_value)
        throw Exception("Negative scales and scales larger than precision are not supported", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    if (precision_value <= DecimalUtils::maxPrecision<Decimal32>())
        return std::make_shared<DecimalType<Decimal32>>(precision_value, scale_value);
    else if (precision_value <= DecimalUtils::maxPrecision<Decimal64>())
        return std::make_shared<DecimalType<Decimal64>>(precision_value, scale_value);
    return std::make_shared<DecimalType<Decimal128>>(precision_value, scale_value);
}

}
