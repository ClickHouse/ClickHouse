#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

/// A nullable data type is an ordinary data type provided with a tag
/// indicating that it also contains the NULL value. The following class
/// embodies this concept.
class DataTypeNullable final : public IDataType
{
public:
    static constexpr bool is_parametric = true;

    explicit DataTypeNullable(const DataTypePtr & nested_data_type_);
    std::string doGetName() const override { return "Nullable(" + nested_data_type->getName() + ")"; }
    const char * getFamilyName() const override { return "Nullable"; }
    TypeIndex getTypeId() const override { return TypeIndex::Nullable; }
    void updateHashImpl(SipHash & hash) const override;

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }
    bool cannotBeStoredInTables() const override { return nested_data_type->cannotBeStoredInTables(); }
    bool shouldAlignRightInPrettyFormats() const override { return nested_data_type->shouldAlignRightInPrettyFormats(); }
    bool textCanContainOnlyValidUTF8() const override { return nested_data_type->textCanContainOnlyValidUTF8(); }
    bool isComparable() const override { return nested_data_type->isComparable(); }
    bool canBeComparedWithCollation() const override { return nested_data_type->canBeComparedWithCollation(); }
    bool canBeUsedAsVersion() const override { return false; }
    bool isSummable() const override { return nested_data_type->isSummable(); }
    bool canBeUsedInBooleanContext() const override { return nested_data_type->canBeUsedInBooleanContext() || onlyNull(); }
    bool haveMaximumSizeOfValue() const override { return nested_data_type->haveMaximumSizeOfValue(); }
    size_t getMaximumSizeOfValueInMemory() const override { return 1 + nested_data_type->getMaximumSizeOfValueInMemory(); }
    bool isNullable() const override { return true; }
    size_t getSizeOfValueInMemory() const override;
    bool onlyNull() const override;
    bool canBeInsideLowCardinality() const override { return nested_data_type->canBeInsideLowCardinality(); }
    bool canBePromoted() const override { return nested_data_type->canBePromoted(); }
    ColumnPtr createColumnConst(size_t size, const Field & field) const override;
    bool hasDynamicSubcolumnsData() const override { return nested_data_type->hasDynamicSubcolumns(); }
    std::unique_ptr<SubstreamData> getDynamicSubcolumnData(std::string_view subcolumn_name, const SubstreamData & data, bool throw_if_null) const override;

    const DataTypePtr & getNestedType() const { return nested_data_type; }

    void forEachChild(const ChildCallback & callback) const override;

private:
    SerializationPtr doGetDefaultSerialization() const override;

    DataTypePtr nested_data_type;
};


DataTypePtr makeNullable(const DataTypePtr & type);
DataTypePtr makeNullableSafe(const DataTypePtr & type);
DataTypePtr removeNullable(const DataTypePtr & type);
DataTypePtr makeNullableOrLowCardinalityNullable(const DataTypePtr & type);
DataTypePtr makeNullableOrLowCardinalityNullableSafe(const DataTypePtr & type);
/// Nullable(T) -> T, LowCardinality(Nullable(T)) -> T
DataTypePtr removeNullableOrLowCardinalityNullable(const DataTypePtr & type);

bool canContainNull(const IDataType & type);

}
