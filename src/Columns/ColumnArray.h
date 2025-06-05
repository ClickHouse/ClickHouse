#pragma once

#include <Columns/IColumn.h>
#include <Columns/IColumnImpl.h>
#include <Columns/ColumnVector.h>
#include <Core/Defines.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>


namespace DB
{

/** A column of array values.
  * In memory, it is represented as one column of a nested type, whose size is equal to the sum of the sizes of all arrays,
  *  and as an array of offsets in it, which allows you to get each element.
  */
class ColumnArray final : public COWHelper<IColumnHelper<ColumnArray>, ColumnArray>
{
private:
    friend class COWHelper<IColumnHelper<ColumnArray>, ColumnArray>;

    /** Create an array column with specified values and offsets. */
    ColumnArray(MutableColumnPtr && nested_column, MutableColumnPtr && offsets_column);

    /** Create an empty column of arrays with the type of values as in the column `nested_column` */
    explicit ColumnArray(MutableColumnPtr && nested_column);

    ColumnArray(const ColumnArray &) = default;

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
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumnHelper<ColumnArray>, ColumnArray>;

    static Ptr create(const ColumnPtr & nested_column, const ColumnPtr & offsets_column)
    {
        return ColumnArray::create(nested_column->assumeMutable(), offsets_column->assumeMutable());
    }

    static Ptr create(const ColumnPtr & nested_column)
    {
        return ColumnArray::create(nested_column->assumeMutable());
    }

    template <typename ... Args>
    requires (IsMutableColumns<Args ...>::value)
    static MutablePtr create(Args &&... args) { return Base::create(std::forward<Args>(args)...); }

    /** On the index i there is an offset to the beginning of the i + 1 -th element. */
    using ColumnOffsets = ColumnVector<Offset>;

    std::string getName() const override;
    const char * getFamilyName() const override { return "Array"; }
    TypeIndex getDataType() const override { return TypeIndex::Array; }
    MutableColumnPtr cloneResized(size_t size) const override;
    size_t size() const override;
    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;
    StringRef getDataAt(size_t n) const override;
    bool isDefaultAt(size_t n) const override;
    void insertData(const char * pos, size_t length) override;
    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    char * serializeValueIntoMemory(size_t, char * memory) const override;
    const char * deserializeAndInsertFromArena(const char * pos) override;
    const char * skipSerializedInArena(const char * pos) const override;
    void updateHashWithValue(size_t n, SipHash & hash) const override;
    WeakHash32 getWeakHash32() const override;
    void updateHashFast(SipHash & hash) const override;
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#else
    void doInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#endif
    void insert(const Field & x) override;
    bool tryInsert(const Field & x) override;
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertFrom(const IColumn & src_, size_t n) override;
#else
    void doInsertFrom(const IColumn & src_, size_t n) override;
#endif
    void insertDefault() override;
    void popBack(size_t n) override;
    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    void expand(const Filter & mask, bool inverted) override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    template <typename Type> ColumnPtr indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const;
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override;
#else
    int doCompareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override;
#endif
    int compareAtWithCollation(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint, const Collator & collator) const override;
    void getPermutation(PermutationSortDirection direction, PermutationSortStability stability,
                            size_t limit, int nan_direction_hint, Permutation & res) const override;
    void updatePermutation(PermutationSortDirection direction, PermutationSortStability stability,
                            size_t limit, int nan_direction_hint, Permutation & res, EqualRanges & equal_ranges) const override;
    void getPermutationWithCollation(const Collator & collator, PermutationSortDirection direction, PermutationSortStability stability,
                                    size_t limit, int nan_direction_hint, Permutation & res) const override;
    void updatePermutationWithCollation(const Collator & collator, PermutationSortDirection direction, PermutationSortStability stability,
                                    size_t limit, int nan_direction_hint, Permutation & res, EqualRanges& equal_ranges) const override;
    void reserve(size_t n) override;
    size_t capacity() const override;
    void prepareForSquashing(const Columns & source_columns) override;
    void shrinkToFit() override;
    void ensureOwnership() override;
    size_t byteSize() const override;
    size_t byteSizeAt(size_t n) const override;
    size_t allocatedBytes() const override;
    void protect() override;
    ColumnPtr replicate(const Offsets & replicate_offsets) const override;
    ColumnPtr convertToFullColumnIfConst() const override;
    void getExtremes(Field & min, Field & max) const override;

    bool hasEqualOffsets(const ColumnArray & other) const;

    /** More efficient methods of manipulation */
    IColumn & getData() { return *data; }
    const IColumn & getData() const { return *data; }

    IColumn & getOffsetsColumn() { return *offsets; }
    const IColumn & getOffsetsColumn() const { return *offsets; }

    Offsets & ALWAYS_INLINE getOffsets()
    {
        return assert_cast<ColumnOffsets &>(*offsets).getData();
    }

    const Offsets & ALWAYS_INLINE getOffsets() const
    {
        return assert_cast<const ColumnOffsets &>(*offsets).getData();
    }

    const ColumnPtr & getDataPtr() const { return data; }
    ColumnPtr & getDataPtr() { return data; }

    const ColumnPtr & getOffsetsPtr() const { return offsets; }
    ColumnPtr & getOffsetsPtr() { return offsets; }

    /// Returns a copy of the data column's part corresponding to a specified range of rows.
    /// For example, `getDataInRange(0, size())` is the same as `getDataPtr()->clone()`.
    MutableColumnPtr getDataInRange(size_t start, size_t length) const;

    ColumnPtr compress() const override;

    void forEachSubcolumn(MutableColumnCallback callback) override
    {
        callback(offsets);
        callback(data);
    }

    void forEachSubcolumnRecursively(RecursiveMutableColumnCallback callback) override
    {
        callback(*offsets);
        offsets->forEachSubcolumnRecursively(callback);
        callback(*data);
        data->forEachSubcolumnRecursively(callback);
    }

    bool structureEquals(const IColumn & rhs) const override
    {
        if (const auto * rhs_concrete = typeid_cast<const ColumnArray *>(&rhs))
            return data->structureEquals(*rhs_concrete->data);
        return false;
    }

    void finalize() override { data->finalize(); }
    bool isFinalized() const override { return data->isFinalized(); }

    bool isCollationSupported() const override { return getData().isCollationSupported(); }

    size_t getNumberOfDimensions() const;

    bool hasDynamicStructure() const override { return getData().hasDynamicStructure(); }
    void takeDynamicStructureFromSourceColumns(const Columns & source_columns) override;

private:
    WrappedPtr data;
    WrappedPtr offsets;

    size_t ALWAYS_INLINE offsetAt(ssize_t i) const { return getOffsets()[i - 1]; }
    size_t ALWAYS_INLINE sizeAt(ssize_t i) const { return getOffsets()[i] - getOffsets()[i - 1]; }


    /// Multiply values if the nested column is ColumnVector<T>.
    template <typename T>
    ColumnPtr replicateNumber(const Offsets & replicate_offsets) const;

    /// Multiply the values if the nested column is ColumnString. The code is too complicated.
    ColumnPtr replicateString(const Offsets & replicate_offsets) const;

    /** Non-constant arrays of constant values are quite rare.
      * Most functions can not work with them, and does not create such columns as a result.
      * An exception is the function `replicate` (see FunctionsMiscellaneous.h), which has service meaning for the implementation of lambda functions.
      * Only for its sake is the implementation of the `replicate` method for ColumnArray(ColumnConst).
      */
    ColumnPtr replicateConst(const Offsets & replicate_offsets) const;

    /** The following is done by simply replicating of nested columns.
      */
    ColumnPtr replicateTuple(const Offsets & replicate_offsets) const;
    ColumnPtr replicateNullable(const Offsets & replicate_offsets) const;
    ColumnPtr replicateGeneric(const Offsets & replicate_offsets) const;


    /// Specializations for the filter function.
    template <typename T>
    ColumnPtr filterNumber(const Filter & filt, ssize_t result_size_hint) const;

    ColumnPtr filterString(const Filter & filt, ssize_t result_size_hint) const;
    ColumnPtr filterTuple(const Filter & filt, ssize_t result_size_hint) const;
    ColumnPtr filterNullable(const Filter & filt, ssize_t result_size_hint) const;
    ColumnPtr filterGeneric(const Filter & filt, ssize_t result_size_hint) const;

    int compareAtImpl(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint, const Collator * collator=nullptr) const;
};


}
