#pragma once

#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationArray.h>
#include <Columns/ColumnArray.h>


namespace DB
{


class DataTypeArray final : public IDataType
{
private:
    /// The type of array elements.
    DataTypePtr nested;

public:
    using FieldType = Array;
    using ColumnType = ColumnArray;
    static constexpr bool is_parametric = true;

    explicit DataTypeArray(const DataTypePtr & nested_);

    TypeIndex getTypeId() const override { return TypeIndex::Array; }

    std::string doGetName() const override
    {
        return "Array(" + nested->getName() + ")";
    }

    std::string doGetPrettyName(size_t indent) const override;

    const char * getFamilyName() const override
    {
        return "Array";
    }

    bool canBeInsideNullable() const override
    {
        return false;
    }

    MutableColumnPtr createColumn() const override;

    void forEachChild(const ChildCallback & callback) const override;

    Field getDefault() const override;

    DataTypePtr getNormalizedType() const override { return std::make_shared<DataTypeArray>(nested->getNormalizedType()); }
    bool equals(const IDataType & rhs) const override;
    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }
    bool cannotBeStoredInTables() const override { return nested->cannotBeStoredInTables(); }
    bool textCanContainOnlyValidUTF8() const override { return nested->textCanContainOnlyValidUTF8(); }
    bool isComparable() const override { return nested->isComparable(); }
    bool canBeComparedWithCollation() const override { return nested->canBeComparedWithCollation(); }
    bool hasDynamicSubcolumnsDeprecated() const override { return nested->hasDynamicSubcolumnsDeprecated(); }

    /// Array column doesn't have subcolumns by itself but allows to read subcolumns of nested column.
    /// If nested column has dynamic subcolumns, Array of this type should also be able to read these dynamic subcolumns.
    bool hasDynamicSubcolumnsData() const override { return nested->hasDynamicSubcolumnsData(); }
    std::unique_ptr<SubstreamData> getDynamicSubcolumnData(std::string_view subcolumn_name, const SubstreamData & data, bool throw_if_null) const override;

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
