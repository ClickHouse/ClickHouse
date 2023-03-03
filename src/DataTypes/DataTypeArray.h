#pragma once

#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationArray.h>


namespace DB
{


class DataTypeArray final : public IDataType
{
private:
    /// The type of array elements.
    DataTypePtr nested;

public:
    static constexpr bool is_parametric = true;

    explicit DataTypeArray(const DataTypePtr & nested_);

    TypeIndex getTypeId() const override { return TypeIndex::Array; }

    std::string doGetName() const override
    {
        return "Array(" + nested->getName() + ")";
    }

    const char * getFamilyName() const override
    {
        return "Array";
    }

    bool canBeInsideNullable() const override
    {
        return false;
    }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }
    bool cannotBeStoredInTables() const override { return nested->cannotBeStoredInTables(); }
    bool textCanContainOnlyValidUTF8() const override { return nested->textCanContainOnlyValidUTF8(); }
    bool isComparable() const override { return nested->isComparable(); }
    bool canBeComparedWithCollation() const override { return nested->canBeComparedWithCollation(); }

    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override
    {
        return nested->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion();
    }

    SerializationPtr doGetDefaultSerialization() const override;

    const DataTypePtr & getNestedType() const { return nested; }

    /// 1 for plain array, 2 for array of arrays and so on.
    size_t getNumberOfDimensions() const;
};

}
