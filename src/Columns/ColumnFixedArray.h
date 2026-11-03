#pragma once

#include <Columns/IColumn.h>
#include <Columns/IColumnImpl.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>


namespace DB
{

class ColumnArray;

/** A column of arrays of the same fixed size `n`.
  *
  * In memory, it is represented only by the column of elements, whose size is `n` times the number of rows:
  * the elements of the row `i` are the elements `[i * n, (i + 1) * n)` of the nested column. There are no offsets.
  *
  * Multidimensional arrays are represented recursively: `FixedArray(N, M, T)` is `FixedArray(N, FixedArray(M, T))`,
  * so the column of a multidimensional array is a chain of `ColumnFixedArray` ending with the column of scalars,
  * and all its elements are laid out contiguously in the row-major order.
  */
class ColumnFixedArray final : public COWHelper<IColumnHelper<ColumnFixedArray>, ColumnFixedArray>
{
private:
    friend class COWHelper<IColumnHelper<ColumnFixedArray>, ColumnFixedArray>;

    ColumnFixedArray(MutableColumnPtr && nested_column, size_t n_);
    ColumnFixedArray(const ColumnFixedArray &) = default;

    struct ComparatorBase;

    using ComparatorAscendingUnstable = ComparatorAscendingUnstableImpl<ComparatorBase>;
    using ComparatorAscendingStable = ComparatorAscendingStableImpl<ComparatorBase>;
    using ComparatorDescendingUnstable = ComparatorDescendingUnstableImpl<ComparatorBase>;
    using ComparatorDescendingStable = ComparatorDescendingStableImpl<ComparatorBase>;
    using ComparatorEqual = ComparatorEqualImpl<ComparatorBase>;

    struct ComparatorCollationBase;

    using ComparatorCollationAscendingUnstable = ComparatorAscendingUnstableImpl<ComparatorCollationBase>;
    using ComparatorCollationAscendingStable = ComparatorAscendingStableImpl<ComparatorCollationBase>;
    using ComparatorCollationDescendingUnstable = ComparatorDescendingUnstableImpl<ComparatorCollationBase>;
    using ComparatorCollationDescendingStable = ComparatorDescendingStableImpl<ComparatorCollationBase>;
    using ComparatorCollationEqual = ComparatorEqualImpl<ComparatorCollationBase>;

public:
    using Base = COWHelper<IColumnHelper<ColumnFixedArray>, ColumnFixedArray>;

    /// The nested column may be shared with other columns.
    static Ptr create(const ColumnPtr & nested_column, size_t n)
    {
        return ColumnFixedArray::create(nested_column->assumeMutable(), n);
    }

    template <typename... Args>
    requires (IsMutableColumns<Args...>::value)
    static MutablePtr create(Args &&... args) { return Base::create(std::forward<Args>(args)...); }

    static MutablePtr create(MutableColumnPtr && nested_column, size_t n) { return Base::create(std::move(nested_column), n); }

    std::string getName() const override;
    const char * getFamilyName() const override { return "FixedArray"; }
    TypeIndex getDataType() const override { return TypeIndex::FixedArray; }

    MutableColumnPtr cloneResized(size_t size) const override;
    size_t size() const override { return data->size() / n; }

    Field operator[](size_t index) const override;
    void get(size_t index, Field & res) const override;
    void getValueNameImpl(WriteBufferFromOwnString & name_buf, size_t index, const Options & options) const override;

    std::string_view getDataAt(size_t index) const override;
    /// The elements of a row are contiguous if they have fixed size, also when they are rows of a nested `ColumnFixedArray`.
    bool supportsGetDataAt() const override
    {
        return data->isFixedAndContiguous() || (typeid_cast<const ColumnFixedArray *>(data.get()) && data->supportsGetDataAt());
    }
    bool isDefaultAt(size_t index) const override;
    bool hasOnlyTypeDefaults() const override { return data->hasOnlyTypeDefaults(); }

    void insertData(const char * pos, size_t length) override;
    std::string_view serializeValueIntoArena(size_t index, Arena & arena, char const *& begin, const IColumn::SerializationSettings * settings) const override;
    char * serializeValueIntoMemory(size_t index, char * memory, const IColumn::SerializationSettings * settings) const override;
    std::optional<size_t> getSerializedValueSize(size_t index, const IColumn::SerializationSettings * settings) const override;
    void deserializeAndInsertFromArena(ReadBuffer & in, const IColumn::SerializationSettings * settings) override;

    void updateHashWithValue(size_t index, SipHash & hash) const override;
    void updateHashWithValueRange(size_t begin, size_t end, SipHash & hash) const override;
    void computeHashInto(size_t row_begin, size_t row_end, UInt32 * hash_out, bool initial) const override;
    void updateHashFast(SipHash & hash) const override;

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    void insertFrom(const IColumn & src, size_t index) override;
    void insertManyFrom(const IColumn & src, size_t position, size_t length) override;
#else
    void doInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    void doInsertFrom(const IColumn & src, size_t index) override;
    void doInsertManyFrom(const IColumn & src, size_t position, size_t length) override;
#endif

    void insert(const Field & x) override;
    bool tryInsert(const Field & x) override;
    void insertDefault() override { data->insertManyDefaults(n); }
    void insertManyDefaults(size_t length) override { data->insertManyDefaults(n * length); }
    void popBack(size_t elems) override;

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    void filter(const Filter & filt) override;
    void expand(const Filter & mask, bool inverted) override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    template <typename Type> ColumnPtr indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const;
    ColumnPtr replicate(const Offsets & replicate_offsets) const override;

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    int compareAt(size_t lhs, size_t rhs, const IColumn & rhs_column, int nan_direction_hint) const override;
#else
    int doCompareAt(size_t lhs, size_t rhs, const IColumn & rhs_column, int nan_direction_hint) const override;
#endif
    int compareAtWithCollation(size_t lhs, size_t rhs, const IColumn & rhs_column, int nan_direction_hint, const Collator & collator) const override;
    bool isCollationSupported() const override { return data->isCollationSupported(); }

    void getPermutation(PermutationSortDirection direction, PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, Permutation & res) const override;
    void updatePermutation(PermutationSortDirection direction, PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, Permutation & res, EqualRanges & equal_ranges) const override;
    void getPermutationWithCollation(const Collator & collator, PermutationSortDirection direction, PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, Permutation & res) const override;
    void updatePermutationWithCollation(const Collator & collator, PermutationSortDirection direction, PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, Permutation & res, EqualRanges & equal_ranges) const override;

    void reserve(size_t size) override { data->reserve(n * size); }
    size_t capacity() const override { return data->capacity() / n; }
    void prepareForSquashing(const VectorWithMemoryTracking<ColumnPtr> & source_columns, size_t factor) override;
    void shrinkToFit() override { data->shrinkToFit(); }
    void ensureOwnership() override { data->ensureOwnership(); }

    size_t byteSize() const override { return data->byteSize(); }
    size_t byteSizeAt(size_t index) const override;
    size_t allocatedBytes() const override { return data->allocatedBytes(); }
    void protect() override { data->protect(); }

    void getExtremes(Field & min, Field & max, size_t start, size_t end) const override;

    ColumnPtr compress(bool force_compression) const override;

    ColumnCheckpointPtr getCheckpoint() const override;
    void updateCheckpoint(ColumnCheckpoint & checkpoint) const override;
    void rollback(const ColumnCheckpoint & checkpoint) override;

    /// A row is a contiguous range of `n` values in the nested column, but this column is not reported as fixed
    /// and contiguous: the consumers of this property (the packing of fixed keys for hash tables, the in-place
    /// application of patch parts and so on) assume the physical layout of `ColumnFixedSizeHelper`.
    bool isFixedAndContiguous() const override { return false; }
    size_t sizeOfValueIfFixed() const override { return n * data->sizeOfValueIfFixed(); }

    void forEachMutableSubcolumn(MutableColumnCallback callback) override { callback(data); }

    void forEachMutableSubcolumnRecursively(RecursiveMutableColumnCallback callback) override
    {
        callback(*data);
        data->forEachMutableSubcolumnRecursively(callback);
    }

    void forEachSubcolumn(ColumnCallback callback) const override { callback(data); }

    void forEachSubcolumnRecursively(RecursiveColumnCallback callback) const override
    {
        callback(*data);
        data->forEachSubcolumnRecursively(callback);
    }

    bool structureEquals(const IColumn & rhs) const override
    {
        if (const auto * rhs_concrete = typeid_cast<const ColumnFixedArray *>(&rhs))
            return n == rhs_concrete->n && data->structureEquals(*rhs_concrete->data);
        return false;
    }

    void finalize() override { data->finalize(); }
    bool isFinalized() const override { return data->isFinalized(); }

    bool hasDynamicStructure() const override { return data->hasDynamicStructure(); }
    void takeExactDynamicStructureFrom(const IColumn & source) override;
    void chooseDynamicStructureForMerge(const VectorWithMemoryTracking<ColumnPtr> & source_columns, std::optional<size_t> max_dynamic_subcolumns) override;
    void fixDynamicStructure() override { data->fixDynamicStructure(); }

    bool dynamicStructureEquals(const IColumn & rhs) const override
    {
        if (const auto * rhs_concrete = typeid_cast<const ColumnFixedArray *>(&rhs))
            return n == rhs_concrete->n && data->dynamicStructureEquals(*rhs_concrete->data);
        return false;
    }

    /// The number of elements in each row.
    size_t getArraySize() const { return n; }

    IColumn & getData() { return *data; }
    const IColumn & getData() const { return *data; }
    const ColumnPtr & getDataPtr() const { return data; }
    ColumnPtr & getDataPtr() { return data; }

    /// The same values as an ordinary `ColumnArray`. The nested column is shared, only the offsets are generated.
    /// The nested column is not converted, so it may still contain `ColumnFixedArray`.
    ColumnPtr convertToArray() const;

private:
    WrappedPtr data;
    const size_t n;

    /// Expand a selection of rows to the selection of elements of the nested column.
    template <typename Type>
    MutableColumnPtr expandIndexes(const PaddedPODArray<Type> & indexes, size_t limit) const;

    int compareAtImpl(size_t lhs, size_t rhs, const IColumn & rhs_column, int nan_direction_hint, const Collator * collator) const;
};

/// Replaces `ColumnFixedArray` with `ColumnArray` of the same elements in the column and in its nested columns
/// (inside `ColumnConst`, `ColumnArray`, `ColumnTuple`, `ColumnMap`, `ColumnNullable` and `ColumnReplicated`), in agreement with
/// `recursiveRemoveFixedArray` for types. The elements are not copied. Returns the same pointer if there is no `ColumnFixedArray`.
ColumnPtr recursiveRemoveFixedArray(const ColumnPtr & column);

}
