#pragma once

#include <DataTypes/IDataType.h>
#include <Columns/ColumnFixedArray.h>


namespace DB
{

/** `FixedArray(N, T)` - an array of exactly `N` elements of type `T`.
  *
  * A multidimensional array can be written as `FixedArray(N, M, T)`, which is the same type as `FixedArray(N, FixedArray(M, T))`.
  * The name of the type is always written in the first form.
  *
  * The values are the same as of `Array(T)`, and the functions that are not specialized for `FixedArray` process it as `Array`.
  * But the size is a part of the type, so the sizes are not stored, and the elements of a multidimensional array are stored
  * contiguously, like a tensor.
  */
class DataTypeFixedArray final : public IDataType
{
private:
    size_t n;
    DataTypePtr nested;

public:
    using FieldType = Array;
    using ColumnType = ColumnFixedArray;
    static constexpr bool is_parametric = true;

    /// The limit for the total number of elements in a value, which is the product of all dimensions.
    static constexpr size_t MAX_ELEMENTS = 1ULL << 30;

    DataTypeFixedArray(size_t n_, const DataTypePtr & nested_);

    TypeIndex getTypeId() const override { return TypeIndex::FixedArray; }
    std::string doGetName() const override;
    std::string doGetPrettyName(size_t indent) const override;
    const char * getFamilyName() const override { return "FixedArray"; }

    bool canBeInsideNullable() const override { return false; }

    MutableColumnPtr createColumn() const override;

    size_t getNumberOfChildren() const override { return 1; }
    const DataTypePtr & getChild(size_t index) const override
    {
        chassert(index == 0);
        return nested;
    }

    Field getDefault() const override;

    DataTypePtr getNormalizedType() const override { return std::make_shared<DataTypeFixedArray>(n, nested->getNormalizedType()); }
    bool equals(const IDataType & rhs) const override;
    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }
    bool cannotBeStoredInTables() const override { return nested->cannotBeStoredInTables(); }
    bool textCanContainOnlyValidUTF8() const override { return nested->textCanContainOnlyValidUTF8(); }
    bool isComparable() const override { return nested->isComparable(); }
    bool canBeComparedWithCollation() const override { return nested->canBeComparedWithCollation(); }
    bool hasDynamicStructure() const override { return nested->hasDynamicStructure(); }

    bool haveMaximumSizeOfValue() const override { return nested->haveMaximumSizeOfValue(); }
    size_t getMaximumSizeOfValueInMemory() const override { return n * nested->getMaximumSizeOfValueInMemory(); }
    size_t getSizeOfValueInMemory() const override { return n * nested->getSizeOfValueInMemory(); }

    /// The value is `n` consecutive values of the nested type.
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override
    {
        return nested->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion();
    }
    bool isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion() const override
    {
        return nested->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion();
    }

    void updateHashImpl(SipHash & hash) const override;

    SerializationPtr doGetSerialization(const SerializationInfoSettings & settings) const override;

    size_t getArraySize() const { return n; }
    const DataTypePtr & getNestedType() const { return nested; }

private:
    DataTypePtr doCloneWithChildren(const DataTypes & new_children) const override;
};

/// Replaces `FixedArray` with `Array` of the same elements in the type and in its nested types (inside `Array`, `Tuple`,
/// `Map` and `Nullable`). Returns the same pointer if the type does not contain `FixedArray`.
DataTypePtr recursiveRemoveFixedArray(const DataTypePtr & type);

}
