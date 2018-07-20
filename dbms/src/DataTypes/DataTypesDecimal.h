#pragma once
#include <DataTypes/IDataType.h>


namespace DB
{

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


/// Implements Decimal(P, S), where P is precision, S is scale.
/// Maximum precisions for underlying types are:
/// Int32    9
/// Int64   18
/// Int128  38
/// Operation between two decimals leads to Decimal(P, S), where
///     P is one of (9, 18, 38); equals to the maximum precision for the biggest underlying type of operands.
///     S is maximum scale of operands.
template <typename T>
class DataTypeDecimal final : public DataTypeSimpleSerialization
{
public:
    using UnderlyingType = T;
    using FieldType = typename NearestFieldType<T>::Type;

    static constexpr bool is_parametric = true;

    DataTypeDecimal(UInt32 precision_, Int32 scale_)
    :   precision(precision_),
        scale(scale_)
    {}

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

private:
    UInt32 precision;
    Int32 scale; /// TODO: should we support scales out of [0, precision]?
};


using DataTypeDecimal9 = DataTypeDecimal<Int32>;
using DataTypeDecimal18 = DataTypeDecimal<Int64>;
using DataTypeDecimal38 = DataTypeDecimal<Int128>;

static constexpr size_t minDecimalPrecision() { return 1; }
template <typename T> static constexpr size_t maxDecimalPrecision();
template <> constexpr size_t maxDecimalPrecision<Int32>() { return 9; }
template <> constexpr size_t maxDecimalPrecision<Int64>() { return 18; }
template <> constexpr size_t maxDecimalPrecision<Int128>() { return 38; }

}
