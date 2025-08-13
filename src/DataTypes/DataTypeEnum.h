#pragma once

#include <DataTypes/IDataType.h>
#include <DataTypes/EnumValues.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnConst.h>
#include <Common/HashTable/HashMap.h>
#include <vector>
#include <unordered_map>


namespace DB
{

class IDataTypeEnum : public IDataType
{
public:
    virtual Field castToName(const Field & value_or_name) const = 0;
    virtual Field castToValue(const Field & value_or_name) const = 0;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return false; }
    bool isValueRepresentedByNumber() const override { return true; }
    bool isValueRepresentedByInteger() const override { return true; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool haveMaximumSizeOfValue() const override { return true; }
    bool isCategorial() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
    bool isComparable() const override { return true; }

    virtual bool contains(const IDataType & rhs) const = 0;
};


template <typename Type>
class DataTypeEnum final : public IDataTypeEnum, public EnumValues<Type>
{
public:
    using FieldType = Type;
    using ColumnType = ColumnVector<FieldType>;
    static constexpr auto type_id = sizeof(FieldType) == 1 ? TypeIndex::Enum8 : TypeIndex::Enum16;
    using typename EnumValues<Type>::Values;

    static constexpr bool is_parametric = true;

private:
    std::string type_name;
    static std::string generateName(const Values & values);

public:
    explicit DataTypeEnum(const Values & values_);

    std::string doGetName() const override { return type_name; }
    const char * getFamilyName() const override;

    TypeIndex getTypeId() const override { return type_id; }
    TypeIndex getColumnType() const override { return sizeof(FieldType) == 1 ? TypeIndex::Int8 : TypeIndex::Int16; }

    FieldType readValue(ReadBuffer & istr) const
    {
        FieldType x;
        readText(x, istr);
        return this->findByValue(x)->first;
    }

    Field castToName(const Field & value_or_name) const override;
    Field castToValue(const Field & value_or_name) const override;

    MutableColumnPtr createColumn() const override { return ColumnType::create(); }

    Field getDefault() const override;
    void insertDefaultInto(IColumn & column) const override;

    bool equals(const IDataType & rhs) const override;

    bool textCanContainOnlyValidUTF8() const override;
    size_t getSizeOfValueInMemory() const override { return sizeof(FieldType); }

    /// Check current Enum type extends another Enum type (contains all the same values and doesn't override name's with other values)
    /// Example:
    /// Enum('a' = 1, 'b' = 2) -> Enum('c' = 1, 'b' = 2, 'd' = 3) OK
    /// Enum('a' = 1, 'b' = 2) -> Enum('a' = 2, 'b' = 1) NOT OK
    bool contains(const IDataType & rhs) const override;

    SerializationPtr doGetDefaultSerialization() const override;
};


using DataTypeEnum8 = DataTypeEnum<Int8>;
using DataTypeEnum16 = DataTypeEnum<Int16>;

}
