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
    /* Type of the elements in the vector: Int8, BFloat16, Float32, Float64 */
    const DataTypePtr element_type;
    /* Number of elements in the vector */
    const size_t dimension;
    /* Number of dimensions stored together in one group of streams. When equal to `dimension` (the default), QBit is not strided:
     * every bit plane spans all dimensions in a single stream. Otherwise the `dimension` dimensions are split into
     * `dimension / stride` contiguous groups, each group's bit planes stored in separate streams, allowing to read fewer
     * dimensions (e.g. for Matryoshka embeddings). */
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
    /// Size of the vector element in bits: 8, 16, 32, 64
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

    /// Whether `type` is a supported QBit element type (Int8, BFloat16, Float32, Float64). Only these have a fixed
    /// in-memory size, so only for these is `getElementSize` well-defined. Shared by the constructor's validation and
    /// the binary-encoding decode path so the whitelist can never drift between the two.
    static bool isSupportedElementType(const DataTypePtr & type);

    /// Upper bound on the number of stride groups (`dimension / stride`). The nested storage materializes
    /// `element size * number of stride groups` FixedString streams, so this caps that to a sane value and
    /// prevents a huge dimension with a tiny stride from exhausting memory. Generous for real Matryoshka use.
    static constexpr size_t MAX_STRIDE_GROUPS = 1024;
};

}
