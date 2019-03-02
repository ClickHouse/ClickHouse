#pragma once
#include <common/arithmeticOverflow.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeWithSimpleSerialization.h>


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


static constexpr size_t minDecimalPrecision() { return 1; }
template <typename T> static constexpr size_t maxDecimalPrecision() { return 0; }
template <> constexpr size_t maxDecimalPrecision<Decimal32>() { return 9; }
template <> constexpr size_t maxDecimalPrecision<Decimal64>() { return 18; }
template <> constexpr size_t maxDecimalPrecision<Decimal128>() { return 38; }


DataTypePtr createDecimal(UInt64 precision, UInt64 scale);

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

/// Implements Decimal(P, S), where P is precision, S is scale.
/// Maximum precisions for underlying types are:
/// Int32    9
/// Int64   18
/// Int128  38
/// Operation between two decimals leads to Decimal(P, S), where
///     P is one of (9, 18, 38); equals to the maximum precision for the biggest underlying type of operands.
///     S is maximum scale of operands. The allowed valuas are [0, precision]
template <typename T>
class DataTypeDecimal final : public DataTypeWithSimpleSerialization
{
    static_assert(IsDecimalNumber<T>);

public:
    using FieldType = T;
    using ColumnType = ColumnDecimal<T>;

    static constexpr bool is_parametric = true;

    static constexpr size_t maxPrecision() { return maxDecimalPrecision<T>(); }

    DataTypeDecimal(UInt32 precision_, UInt32 scale_)
    :   precision(precision_),
        scale(scale_)
    {
        if (unlikely(precision < 1 || precision > maxPrecision()))
            throw Exception("Precision " + std::to_string(precision) + " is out of bounds", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        if (unlikely(scale < 0 || static_cast<UInt32>(scale) > maxPrecision()))
            throw Exception("Scale " + std::to_string(scale) + " is out of bounds", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

    const char * getFamilyName() const override { return "Decimal"; }
    std::string doGetName() const override;
    TypeIndex getTypeId() const override { return TypeId<T>::value; }

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;

    void deserializeBinary(Field & field, ReadBuffer & istr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

    void serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const override;
    void deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const override;

    Field getDefault() const override;
    bool canBePromoted() const override { return true; }
    DataTypePtr promoteNumericType() const override;
    MutableColumnPtr createColumn() const override;
    bool equals(const IDataType & rhs) const override;

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

    T wholePart(T x) const
    {
        if (scale == 0)
            return x;
        return x / getScaleMultiplier();
    }

    T fractionalPart(T x) const
    {
        if (scale == 0)
            return 0;
        if (x < T(0))
            x *= T(-1);
        return x % getScaleMultiplier();
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
    T scaleFactorFor(const DataTypeDecimal<U> & x, bool) const
    {
        if (getScale() < x.getScale())
            throw Exception("Decimal result's scale is less then argiment's one", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
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

    T parseFromString(const String & str) const;

    void readText(T & x, ReadBuffer & istr) const { readText(x, istr, precision, scale); }
    static void readText(T & x, ReadBuffer & istr, UInt32 precision, UInt32 scale);
    static T getScaleMultiplier(UInt32 scale);

private:
    const UInt32 precision;
    const UInt32 scale;
};


template <typename T, typename U>
typename std::enable_if_t<(sizeof(T) >= sizeof(U)), const DataTypeDecimal<T>>
decimalResultType(const DataTypeDecimal<T> & tx, const DataTypeDecimal<U> & ty, bool is_multiply, bool is_divide)
{
    UInt32 scale = (tx.getScale() > ty.getScale() ? tx.getScale() : ty.getScale());
    if (is_multiply)
        scale = tx.getScale() + ty.getScale();
    else if (is_divide)
        scale = tx.getScale();
    return DataTypeDecimal<T>(maxDecimalPrecision<T>(), scale);
}

template <typename T, typename U>
typename std::enable_if_t<(sizeof(T) < sizeof(U)), const DataTypeDecimal<U>>
decimalResultType(const DataTypeDecimal<T> & tx, const DataTypeDecimal<U> & ty, bool is_multiply, bool is_divide)
{
    UInt32 scale = (tx.getScale() > ty.getScale() ? tx.getScale() : ty.getScale());
    if (is_multiply)
        scale = tx.getScale() * ty.getScale();
    else if (is_divide)
        scale = tx.getScale();
    return DataTypeDecimal<U>(maxDecimalPrecision<U>(), scale);
}

template <typename T, typename U>
const DataTypeDecimal<T> decimalResultType(const DataTypeDecimal<T> & tx, const DataTypeNumber<U> &, bool, bool)
{
    return DataTypeDecimal<T>(maxDecimalPrecision<T>(), tx.getScale());
}

template <typename T, typename U>
const DataTypeDecimal<U> decimalResultType(const DataTypeNumber<T> &, const DataTypeDecimal<U> & ty, bool, bool)
{
    return DataTypeDecimal<U>(maxDecimalPrecision<U>(), ty.getScale());
}


template <typename T>
inline const DataTypeDecimal<T> * checkDecimal(const IDataType & data_type)
{
    return typeid_cast<const DataTypeDecimal<T> *>(&data_type);
}

inline UInt32 getDecimalScale(const IDataType & data_type, UInt32 default_value = std::numeric_limits<UInt32>::max())
{
    if (auto * decimal_type = checkDecimal<Decimal32>(data_type))
        return decimal_type->getScale();
    if (auto * decimal_type = checkDecimal<Decimal64>(data_type))
        return decimal_type->getScale();
    if (auto * decimal_type = checkDecimal<Decimal128>(data_type))
        return decimal_type->getScale();
    return default_value;
}

///

template <typename DataType> constexpr bool IsDataTypeDecimal = false;
template <> constexpr bool IsDataTypeDecimal<DataTypeDecimal<Decimal32>> = true;
template <> constexpr bool IsDataTypeDecimal<DataTypeDecimal<Decimal64>> = true;
template <> constexpr bool IsDataTypeDecimal<DataTypeDecimal<Decimal128>> = true;

template <typename DataType> constexpr bool IsDataTypeDecimalOrNumber = IsDataTypeDecimal<DataType> || IsDataTypeNumber<DataType>;

template <typename FromDataType, typename ToDataType>
inline std::enable_if_t<IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>, typename ToDataType::FieldType>
convertDecimals(const typename FromDataType::FieldType & value, UInt32 scale_from, UInt32 scale_to)
{
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;
    using MaxFieldType = std::conditional_t<(sizeof(FromFieldType) > sizeof(ToFieldType)), FromFieldType, ToFieldType>;
    using MaxNativeType = typename MaxFieldType::NativeType;

    MaxNativeType converted_value;
    if (scale_to > scale_from)
    {
        converted_value = DataTypeDecimal<MaxFieldType>::getScaleMultiplier(scale_to - scale_from);
        if (common::mulOverflow(static_cast<MaxNativeType>(value), converted_value, converted_value))
            throw Exception("Decimal convert overflow", ErrorCodes::DECIMAL_OVERFLOW);
    }
    else
        converted_value = value / DataTypeDecimal<MaxFieldType>::getScaleMultiplier(scale_from - scale_to);

    if constexpr (sizeof(FromFieldType) > sizeof(ToFieldType))
    {
        if (converted_value < std::numeric_limits<typename ToFieldType::NativeType>::min() ||
            converted_value > std::numeric_limits<typename ToFieldType::NativeType>::max())
            throw Exception("Decimal convert overflow", ErrorCodes::DECIMAL_OVERFLOW);
    }

    return converted_value;
}

template <typename FromDataType, typename ToDataType>
inline std::enable_if_t<IsDataTypeDecimal<FromDataType> && IsDataTypeNumber<ToDataType>, typename ToDataType::FieldType>
convertFromDecimal(const typename FromDataType::FieldType & value, UInt32 scale)
{
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;

    if constexpr (std::is_floating_point_v<ToFieldType>)
        return static_cast<ToFieldType>(value) / FromDataType::getScaleMultiplier(scale);
    else
    {
        FromFieldType converted_value = convertDecimals<FromDataType, FromDataType>(value, scale, 0);

        if constexpr (sizeof(FromFieldType) > sizeof(ToFieldType) || !std::numeric_limits<ToFieldType>::is_signed)
        {
            if constexpr (std::numeric_limits<ToFieldType>::is_signed)
            {
                if (converted_value < std::numeric_limits<ToFieldType>::min() ||
                    converted_value > std::numeric_limits<ToFieldType>::max())
                    throw Exception("Decimal convert overflow", ErrorCodes::DECIMAL_OVERFLOW);
            }
            else
            {
                using CastIntType = std::conditional_t<std::is_same_v<ToFieldType, UInt64>, Int128, Int64>;

                if (converted_value < 0 ||
                    converted_value > static_cast<CastIntType>(std::numeric_limits<ToFieldType>::max()))
                    throw Exception("Decimal convert overflow", ErrorCodes::DECIMAL_OVERFLOW);
            }
        }
        return converted_value;
    }
}

template <typename FromDataType, typename ToDataType>
inline std::enable_if_t<IsDataTypeNumber<FromDataType> && IsDataTypeDecimal<ToDataType>, typename ToDataType::FieldType>
convertToDecimal(const typename FromDataType::FieldType & value, UInt32 scale)
{
    using FromFieldType = typename FromDataType::FieldType;

    if constexpr (std::is_floating_point_v<FromFieldType>)
        return value * ToDataType::getScaleMultiplier(scale);
    else
    {
        if constexpr (std::is_same_v<FromFieldType, UInt64>)
            if (value > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                return convertDecimals<DataTypeDecimal<Decimal128>, ToDataType>(value, 0, scale);
        return convertDecimals<DataTypeDecimal<Decimal64>, ToDataType>(value, 0, scale);
    }
}

}
