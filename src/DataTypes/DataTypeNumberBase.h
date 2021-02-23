#pragma once

#include <Common/UInt128.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeWithSimpleSerialization.h>


namespace DB
{

template <typename T>
class ColumnVector;

/** Implements part of the IDataType interface, common to all numbers and for Date and DateTime.
  */
template <typename T>
class DataTypeNumberBase : public DataTypeWithSimpleSerialization
{
    static_assert(IsNumber<T>);

public:
    static constexpr bool is_parametric = false;

    using FieldType = T;
    static constexpr auto type_id = TypeId<T>::value;
    static constexpr auto family_name = TypeName<T>::get();

    using ColumnType = ColumnVector<T>;

    const char * getFamilyName() const override { return family_name; }
    TypeIndex getTypeId() const override { return type_id; }

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;
    Field getDefault() const override;

    /** Format is platform-dependent. */
    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;
    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

    MutableColumnPtr createColumn() const override;

    bool isParametric() const override { return false; }
    bool haveSubtypes() const override { return false; }

    bool shouldAlignRightInPrettyFormats() const override
    {
        /// Just a number, without customizations. Counterexample: IPv4.
        return !custom_text_serialization;
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
};

/// Prevent implicit template instantiation of DataTypeNumberBase for common numeric types

extern template class DataTypeNumberBase<UInt8>;
extern template class DataTypeNumberBase<UInt16>;
extern template class DataTypeNumberBase<UInt32>;
extern template class DataTypeNumberBase<UInt64>;
extern template class DataTypeNumberBase<UInt128>; // base for UUID
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
