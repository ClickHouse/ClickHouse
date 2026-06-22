#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

/** QBit data type.
  * Used as a column type for vectors for fast vector search. More details in ColumnQBit.h
  */
class DataTypeQBit final : public IDataType
{
private:
    /* Type of the elements in the vector: BFloat16, Float32, Float64 */
    const DataTypePtr element_type;
    /* Number of elements in the vector */
    const size_t dimension;
    /* Number of dimensions stored together in one group of streams. When equal to `dimension` (the default), QBit is not strided
     * and behaves exactly as before. Otherwise the `dimension` dimensions are split into `dimension / stride` contiguous groups,
     * each group's bit planes stored in separate streams, allowing to read fewer dimensions (e.g. for Matryoshka embeddings). */
    const size_t stride;

public:
    DataTypeQBit(const DataTypePtr & element_type_, size_t dimension_, size_t stride_);

    TypeIndex getTypeId() const override { return TypeIndex::QBit; }
    std::string doGetName() const override;
    const char * getFamilyName() const override { return "QBit"; }

    MutableColumnPtr createColumn() const override;

    /// The default value is a QBit of zeroes
    Field getDefault() const override;
    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
    bool supportsSparseSerialization() const override { return false; }
    bool isComparableForEquality() const override { return true; }

    const DataTypePtr & getElementType() const { return element_type; }
    /// Size of the vector element in bits: 16, 32, 64
    size_t getElementSize() const { return 8 * element_type->getSizeOfValueInMemory(); }
    size_t getDimension() const { return dimension; }
    /// Number of dimensions stored together in one group of streams. Equal to `dimension` when not strided.
    size_t getStride() const { return stride; }
    /// Number of stride groups. Equal to 1 when not strided.
    size_t getNumStrides() const { return dimension / stride; }
    size_t getSizeOfValueInMemory() const override { return (getElementSize() / 8) * dimension; }
    void updateHashImpl(SipHash & hash) const override;

    /// Get the tuple type that represents the internal structure of QBit
    DataTypePtr getNestedType() const;
    /// Get the type of the elements in the tuple (FixedString<N>)
    DataTypePtr getNestedTupleElementType() const;

    SerializationPtr doGetSerialization(const SerializationInfoSettings & settings) const override;

    static ALWAYS_INLINE inline size_t bitsToBytes(size_t n) { return (n + 7) / 8; }
};

}
