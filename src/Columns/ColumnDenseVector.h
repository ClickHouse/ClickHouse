#pragma once

#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Columns/IColumnImpl.h>
#include <Core/Field.h>
#include <Core/TypeId.h>
#include <Common/Exception.h>
#include <Common/WeakHash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <base/BFloat16.h>
#include <base/TypeName.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

/** Column that stores fixed-dimension dense vectors contiguously for exact ("FLAT") vector search.
  *
  * The storage is a single flat nested ColumnVector<T> (T is BFloat16, Float32 or Float64) of length
  * size() * dimension, with no offsets: row i occupies nested elements [i * dimension, (i + 1) * dimension).
  * The bytes are laid out exactly like Faiss FLAT: row i occupies bytes
  * [i * dimension * sizeof(T), (i + 1) * dimension * sizeof(T)).
  *
  * Unlike ColumnArray there is no offsets column and the dimension is part of the type, so SIMD distance
  * kernels can read rows as `const T *` with a fixed stride, and conversions to/from Array(T) can share
  * the nested data column without copying.
  *
  * Rows are compared element-wise (like Array), not byte-wise, so ORDER BY follows numeric semantics.
  * The Field representation of one row is Array.
  */
class ColumnDenseVector final : public COWHelper<IColumnHelper<ColumnDenseVector>, ColumnDenseVector>
{
private:
    friend class COWHelper<IColumnHelper<ColumnDenseVector>, ColumnDenseVector>;

    /// Flat contiguous storage: ColumnVector<T> with size() * dimension elements.
    WrappedPtr nested;
    /// Type of one vector element: BFloat16, Float32 or Float64.
    TypeIndex element_type = TypeIndex::Nothing;
    /// Number of elements in each vector.
    size_t dimension = 0;

    ColumnDenseVector(MutableColumnPtr && nested_, TypeIndex element_type_, size_t dimension_);

    struct ComparatorBase;

    using ComparatorAscendingUnstable = ComparatorAscendingUnstableImpl<ComparatorBase>;
    using ComparatorAscendingStable = ComparatorAscendingStableImpl<ComparatorBase>;
    using ComparatorDescendingUnstable = ComparatorDescendingUnstableImpl<ComparatorBase>;
    using ComparatorDescendingStable = ComparatorDescendingStableImpl<ComparatorBase>;
    using ComparatorEqual = ComparatorEqualImpl<ComparatorBase>;

public:
    using Base = COWHelper<IColumnHelper<ColumnDenseVector>, ColumnDenseVector>;

    static Ptr create(const ColumnPtr & nested_, TypeIndex element_type_, size_t dimension_)
    {
        return Base::create(nested_->assumeMutable(), element_type_, dimension_);
    }
    static MutablePtr create(MutableColumnPtr && nested_, TypeIndex element_type_, size_t dimension_)
    {
        return Base::create(std::move(nested_), element_type_, dimension_);
    }

    /// Invoke `func(T{})` with T being the element type that corresponds to `element_type`.
    template <typename Func>
    static decltype(auto) dispatchByElementType(TypeIndex element_type, Func && func)
    {
        switch (element_type)
        {
            case TypeIndex::BFloat16:
                return func(BFloat16{});
            case TypeIndex::Float32:
                return func(Float32{});
            case TypeIndex::Float64:
                return func(Float64{});
            default:
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Unexpected element type {} of dense vector column", static_cast<UInt64>(element_type));
        }
    }

    static const char * elementTypeName(TypeIndex element_type)
    {
        return dispatchByElementType(element_type, [](auto tag) { return TypeName<decltype(tag)>.data(); });
    }

    static size_t elementSizeInBytes(TypeIndex element_type)
    {
        return dispatchByElementType(element_type, [](auto tag) { return sizeof(tag); });
    }

    const char * getFamilyName() const override { return "Vector"; }
    TypeIndex getDataType() const override { return TypeIndex::Vector; }
    std::string getName() const override;
    MutableColumnPtr cloneResized(size_t new_size) const override;
    bool canBeInsideNullable() const override { return true; }

    size_t size() const override { return nested->size() / dimension; }
    TypeIndex getElementType() const { return element_type; }
    size_t getElementSize() const { return elementSizeInBytes(element_type); }
    size_t getDimension() const { return dimension; }
    /// Size of one vector value in bytes.
    size_t getValueSize() const { return getElementSize() * dimension; }

    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;
    void getValueNameImpl(WriteBufferFromOwnString & name_buf, size_t n, const Options & options) const override;

    std::string_view getDataAt(size_t n) const override
    {
        const size_t value_size = getValueSize();
        return std::string_view(nested->getRawData().data() + n * value_size, value_size);
    }

    void insertData(const char * pos, size_t length) override;
    void insert(const Field & x) override;
    bool tryInsert(const Field & x) override;
    bool isDefaultAt(size_t n) const override;

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertFrom(const IColumn & src_, size_t n) override;
    void insertManyFrom(const IColumn & src, size_t position, size_t length) override;
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#else
    void doInsertFrom(const IColumn & src_, size_t n) override;
    void doInsertManyFrom(const IColumn & src, size_t position, size_t length) override;
    void doInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#endif

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override;
#else
    int doCompareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override;
#endif

    void insertDefault() override { nested->insertManyDefaults(dimension); }
    void popBack(size_t n) override { nested->popBack(n * dimension); }

    std::string_view serializeValueIntoArena(
        size_t n, Arena & arena, char const *& begin, const IColumn::SerializationSettings * settings) const override;
    char * serializeValueIntoMemory(size_t n, char * memory, const IColumn::SerializationSettings * settings) const override;
    void deserializeAndInsertFromArena(ReadBuffer & in, const IColumn::SerializationSettings * settings) override;
    void skipSerializedInArena(ReadBuffer & in) const override;

    void updateHashWithValue(size_t n, SipHash & hash) const override;
    void updateHashWithValueRange(size_t begin, size_t end, SipHash & hash) const override;
    void updateHashFast(SipHash & hash) const override { nested->updateHashFast(hash); }
    WeakHash32 getWeakHash32() const override;

    void expand(const Filter & mask, bool inverted) override;
    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    void filter(const Filter & filt) override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    template <typename Type> ColumnPtr indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const;
    ColumnPtr replicate(const Offsets & offsets) const override;
    ColumnPtr compress(bool force_compression) const override;

    void getExtremes(Field & min, Field & max, size_t start, size_t end) const override;
    void getPermutation(
        PermutationSortDirection direction,
        PermutationSortStability stability,
        size_t limit,
        int nan_direction_hint,
        Permutation & res) const override;
    void updatePermutation(
        PermutationSortDirection direction,
        PermutationSortStability stability,
        size_t limit,
        int nan_direction_hint,
        Permutation & res,
        EqualRanges & equal_ranges) const override;

    void reserve(size_t n) override { nested->reserve(n * dimension); }
    void shrinkToFit() override { nested->shrinkToFit(); }
    void ensureOwnership() override { nested->ensureOwnership(); }
    void protect() override { nested->protect(); }

    size_t capacity() const override { return nested->capacity() / dimension; }
    size_t byteSize() const override { return nested->byteSize(); }
    size_t byteSizeAt(size_t) const override { return getValueSize(); }
    size_t allocatedBytes() const override { return nested->allocatedBytes(); }

    /// The nested column has no sibling to keep in sync, so checkpoints are delegated as-is.
    void updateCheckpoint(ColumnCheckpoint & checkpoint) const override { nested->updateCheckpoint(checkpoint); }
    void rollback(const ColumnCheckpoint & checkpoint) override { nested->rollback(checkpoint); }
    ColumnCheckpointPtr getCheckpoint() const override { return nested->getCheckpoint(); }

    void forEachMutableSubcolumn(MutableColumnCallback callback) override;
    void forEachMutableSubcolumnRecursively(RecursiveMutableColumnCallback callback) override;
    void forEachSubcolumn(ColumnCallback callback) const override;
    void forEachSubcolumnRecursively(RecursiveColumnCallback callback) const override;
    void finalize() override { nested->finalize(); }
    bool isFinalized() const override { return nested->isFinalized(); }

    bool structureEquals(const IColumn & rhs) const override
    {
        if (const auto * rhs_vec = typeid_cast<const ColumnDenseVector *>(&rhs))
            return element_type == rhs_vec->element_type && dimension == rhs_vec->dimension
                && nested->structureEquals(*rhs_vec->nested);
        return false;
    }

    /// Values are fixed-size and the storage is contiguous, but isFixedAndContiguous must stay false:
    /// callers (packFixed in aggregation, SetVariants, HashJoin, arrayUniq) take it as a license to
    /// static_cast the column to ColumnFixedSizeHelper, whose layout hack requires the data array to be
    /// the first member of the column object — not true here, where the data lives in the nested column.
    bool valuesHaveFixedSize() const override { return true; }
    size_t sizeOfValueIfFixed() const override { return getValueSize(); }
    std::string_view getRawData() const override { return nested->getRawData(); }

    /// Access to the flat nested storage (used by SIMD distance kernels and Array <-> Vector casts).
    const ColumnPtr & getNestedColumnPtr() const { return nested; }
    ColumnPtr & getNestedColumnPtr() { return nested; }
    const IColumn & getNestedColumn() const { return *nested; }
    IColumn & getNestedColumn() { return *nested; }

    template <typename T>
    const PaddedPODArray<T> & getTypedData() const
    {
        return assert_cast<const ColumnVector<T> &>(*nested).getData();
    }

    template <typename T>
    PaddedPODArray<T> & getTypedData()
    {
        return assert_cast<ColumnVector<T> &>(*nested).getData();
    }

private:
    int compareAtImpl(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const;

    /// Throws if `src` is a dense vector column of a different element type or dimension.
    const ColumnDenseVector & checkCompatibility(const IColumn & src) const;
};

}
