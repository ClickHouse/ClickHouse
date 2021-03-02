#pragma once

#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationNumber.h>


namespace DB
{

template <typename T>
class ColumnVector;

/** Implements part of the IDataType interface, common to all numbers and for Date and DateTime.
  */
template <typename T>
class DataTypeNumberBase : public IDataType
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

    Field getDefault() const override;

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

    SerializationPtr doGetDefaultSerialization() const override { return std::make_shared<SerializationNumber<T>>(); }
};

}
