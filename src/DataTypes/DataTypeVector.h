#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

/// Maximum size in bytes of one Vector value (mirrors MAX_FIXEDSTRING_SIZE).
/// Also protects `element size * dimension` from overflowing size_t.
constexpr size_t MAX_VECTOR_VALUE_SIZE = 0xFFFFFF;

/** Vector data type: a fixed-dimension dense vector stored contiguously (Faiss "FLAT" layout).
  *
  * `Vector(element_type, dimension)` stores `dimension` elements of `element_type` (BFloat16, Float32 or
  * Float64) as a flat nested column with no offsets. Compared to `Array(element_type)` this enforces a
  * fixed dimension, drops the offsets stream, and keeps the data SIMD-friendly for exact brute-force
  * vector search. More details in ColumnDenseVector.h.
  */
class DataTypeVector final : public IDataType
{
private:
    /// Type of the elements in the vector: BFloat16, Float32, Float64.
    const DataTypePtr element_type;
    /// Number of elements in the vector.
    const size_t dimension;

public:
    DataTypeVector(const DataTypePtr & element_type_, size_t dimension_);

    TypeIndex getTypeId() const override { return TypeIndex::Vector; }
    std::string doGetName() const override;
    const char * getFamilyName() const override { return "Vector"; }

    MutableColumnPtr createColumn() const override;

    /// The default value is a vector of zeroes.
    Field getDefault() const override;
    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return false; }
    bool canBeInsideNullable() const override { return true; }
    bool isComparable() const override { return true; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    /// Must stay false: a true value routes GROUP BY keys into packFixed, which requires the column
    /// to be a ColumnFixedSizeHelper (see the comment on ColumnDenseVector::valuesHaveFixedSize).
    bool isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion() const override { return false; }
    bool haveMaximumSizeOfValue() const override { return true; }
    bool supportsSparseSerialization() const override { return false; }

    const DataTypePtr & getElementType() const { return element_type; }
    TypeIndex getElementTypeIndex() const { return element_type->getTypeId(); }
    /// Size of one vector element in bytes: 2 (BFloat16), 4 (Float32), 8 (Float64).
    size_t getElementSizeInBytes() const { return element_type->getSizeOfValueInMemory(); }
    size_t getDimension() const { return dimension; }
    size_t getSizeOfValueInMemory() const override { return getElementSizeInBytes() * dimension; }
    void updateHashImpl(SipHash & hash) const override;

    SerializationPtr doGetSerialization(const SerializationInfoSettings & settings) const override;
};

}
