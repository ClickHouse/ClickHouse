#pragma once
#include <common/likely.h>
#include <Common/typeid_cast.h>
#include <DataTypes/IDataType.h>
#include <Columns/ColumnVector.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

///
class DataTypeSimpleSerialization : public IDataType
{
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        serializeText(column, row_num, ostr, settings);
    }

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        serializeText(column, row_num, ostr, settings);
    }

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        serializeText(column, row_num, ostr, settings);
    }

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        serializeText(column, row_num, ostr, settings);
    }

    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        deserializeText(column, istr, settings);
    }

    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        deserializeText(column, istr, settings);
    }

    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        deserializeText(column, istr, settings);
    }

    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        deserializeText(column, istr, settings);
    }

    virtual void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const = 0;
};


/// Enum for IDataType to DataTypeDecimal convertion.
enum class DecimalPrecision
{
    None = 0,
    I32 = 9,
    I64 = 18,
    I128 = 38,
};


static constexpr size_t minDecimalPrecision() { return 1; }
template <typename T> static constexpr size_t maxDecimalPrecision();
template <> constexpr size_t maxDecimalPrecision<Int32>() { return 9; }
template <> constexpr size_t maxDecimalPrecision<Int64>() { return 18; }
template <> constexpr size_t maxDecimalPrecision<Int128>() { return 38; }


/// Implements Decimal(P, S), where P is precision, S is scale.
/// Maximum precisions for underlying types are:
/// Int32    9
/// Int64   18
/// Int128  38
/// Operation between two decimals leads to Decimal(P, S), where
///     P is one of (9, 18, 38); equals to the maximum precision for the biggest underlying type of operands.
///     S is maximum scale of operands.
///
/// NOTE: It's possible to set scale as a template parameter then most of functions become static.
template <typename T>
class DataTypeDecimal final : public DataTypeSimpleSerialization
{
public:
    using UnderlyingType = T;
    using FieldType = typename NearestFieldType<T>::Type;
    using ColumnType = ColumnVector<T, false>;

    static constexpr bool is_parametric = true;

    DataTypeDecimal(UInt32 precision_, UInt32 scale_)
    :   precision(precision_),
        scale(scale_)
    {
        if (unlikely(precision < 1 || precision > maxDecimalPrecision<T>()))
            throw Exception("Precision is out of bounds", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        if (unlikely(scale < 0 || static_cast<UInt32>(scale) > maxDecimalPrecision<T>()))
            throw Exception("Scale is out of bounds", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

    const char * getFamilyName() const override { return "Decimal"; }
    std::string getName() const override;

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;

    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;

    void deserializeBinary(Field & field, ReadBuffer & istr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

    Field getDefault() const override;
    MutableColumnPtr createColumn() const override;
    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return false; }
    bool shouldAlignRightInPrettyFormats() const override { return true; }
    bool textCanContainOnlyValidUTF8() const override { return true; }
    bool isComparable() const override { return true; }
    bool isValueRepresentedByNumber() const override { return true; }
    bool isValueRepresentedByInteger() const override { return true; }
    bool isValueRepresentedByUnsignedInteger() const override { return false; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool haveMaximumSizeOfValue() const override { return true; }
    size_t getSizeOfValueInMemory() const override { return sizeof(T); }
    bool isCategorial() const override { return isValueRepresentedByInteger(); }

    bool canBeUsedAsVersion() const override { return false; }
    bool isSummable() const override { return true; }
    bool canBeUsedInBitOperations() const override { return false; }
    bool isUnsignedInteger() const override { return false; }
    bool canBeUsedInBooleanContext() const override { return true; }
    bool isNumber() const override { return true; }
    bool isInteger() const override { return false; }
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
        if (x < 0)
            x *= -1;
        return x % getScaleMultiplier();
    }

    T maxWholeValue() const { return getScaleMultiplier(maxDecimalPrecision<T>() - scale) - 1; }

    bool canStoreWhole(T x) const
    {
        T max = maxWholeValue();
        if (x > max || x < -max)
            return false;
        return true;
    }

    /// @returns multiplier for T to become UnderlyingType of result_type with correct scale
    template <typename R>
    R scaleFactor(const DataTypeDecimal<R> & result_type) const
    {
        if (getScale() > result_type.getScale())
            throw Exception("Decimal result's scale is less then argiment's one", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        UInt32 scale_delta = result_type.getScale() - getScale(); /// scale_delta >= 0
        return DataTypeDecimal<R>::getScaleMultiplier(scale_delta);
    }

private:
    const UInt32 precision;
    const UInt32 scale; /// TODO: should we support scales out of [0, precision]?

    static T getScaleMultiplier(UInt32 scale);

    template <typename>
    friend class DataTypeDecimal;
};


template <typename T, typename U>
typename std::enable_if_t<(sizeof(T) >= sizeof(U)), const DataTypeDecimal<T>>
decimalResultType(const DataTypeDecimal<T> & tx, const DataTypeDecimal<U> & ty)
{
    return DataTypeDecimal<T>(maxDecimalPrecision<T>(), (tx.getScale() > ty.getScale() ? tx.getScale() : ty.getScale()));
}

template <typename T, typename U>
typename std::enable_if_t<(sizeof(T) < sizeof(U)), const DataTypeDecimal<U>>
decimalResultType(const DataTypeDecimal<T> & tx, const DataTypeDecimal<U> & ty)
{
    return DataTypeDecimal<U>(maxDecimalPrecision<U>(), (tx.getScale() > ty.getScale() ? tx.getScale() : ty.getScale()));
}

template <typename T>
inline const DataTypeDecimal<T> * checkDecimal(const IDataType & data_type)
{
    return typeid_cast<const DataTypeDecimal<T> *>(&data_type);
}

inline DecimalPrecision checkDecimal(const IDataType & data_type)
{
    if (typeid_cast<const DataTypeDecimal<Int32> *>(&data_type))
        return DecimalPrecision::I32;
    if (typeid_cast<const DataTypeDecimal<Int64> *>(&data_type))
        return DecimalPrecision::I64;
    if (typeid_cast<const DataTypeDecimal<Int128> *>(&data_type))
        return DecimalPrecision::I128;
    return DecimalPrecision::None;
}

inline bool isDecimal(DecimalPrecision precision) { return precision != DecimalPrecision::None; }
inline bool isDecimal(const IDataType & data_type) { return isDecimal(checkDecimal(data_type)); }

///
inline bool notDecimalButComparableToDecimal(const IDataType & data_type)
{
    if (data_type.isInteger())
        return true;
    return false;
}

///
inline bool comparableToDecimal(const IDataType & data_type)
{
    if (data_type.isInteger())
        return true;
    return isDecimal(data_type);
}

}
