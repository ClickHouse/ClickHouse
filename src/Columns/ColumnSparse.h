#pragma once

#include <Columns/IColumn.h>
#include <Columns/IColumnImpl.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

class Collator;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class ColumnSparse final : public COWHelper<IColumn, ColumnSparse>
{
private:
    friend class COWHelper<IColumn, ColumnSparse>;

    explicit ColumnSparse(MutableColumnPtr && values_);
    ColumnSparse(MutableColumnPtr && values_, MutableColumnPtr && offsets_, size_t size_);
    ColumnSparse(const ColumnSparse &) = default;

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumn, ColumnSparse>;
    static Ptr create(const ColumnPtr & values_, const ColumnPtr & offsets_, size_t size_)
    {
        return Base::create(values_->assumeMutable(), offsets_->assumeMutable(), size_);
    }

    static MutablePtr create(MutableColumnPtr && values_, MutableColumnPtr && offsets_, size_t size_)
    {
        return Base::create(std::move(values_), std::move(offsets_), size_);
    }

    static Ptr create(const ColumnPtr & values_)
    {
        return Base::create(values_->assumeMutable());
    }

    template <typename Arg, typename = typename std::enable_if_t<std::is_rvalue_reference_v<Arg &&>>>
    static MutablePtr create(Arg && arg)
    {
        return Base::create(std::forward<Arg>(arg));
    }

    const char * getFamilyName() const override { return "Sparse"; }
    std::string getName() const override { return "Sparse(" + values->getName() + ")"; }
    TypeIndex getDataType() const override { return values->getDataType(); }
    MutableColumnPtr cloneResized(size_t new_size) const override;
    size_t size() const override { return _size; }
    bool isNullAt(size_t n) const override;
    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;
    bool getBool(size_t n) const override;
    UInt64 get64(size_t n) const override;
    StringRef getDataAt(size_t n) const override;

    ColumnPtr convertToFullColumnIfSparse() const override;

    /// Will insert null value if pos=nullptr
    void insertData(const char * pos, size_t length) override;
    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    const char * deserializeAndInsertFromArena(const char * pos) override;
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    void insert(const Field & x) override;
    void insertFrom(const IColumn & src, size_t n) override;
    void insertDefault() override;
    void insertManyDefaults(size_t length) override;

    void popBack(size_t n) override;
    ColumnPtr filter(const Filter & filt, ssize_t) const override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    int compareAt(size_t n, size_t m, const IColumn & rhs_, int null_direction_hint) const override;
    void compareColumn(const IColumn & rhs, size_t rhs_row_num,
                       PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                       int direction, int nan_direction_hint) const override;
    int compareAtWithCollation(size_t n, size_t m, const IColumn & rhs, int null_direction_hint, const Collator &) const override;
    bool hasEqualValues() const override;
    void getPermutation(bool reverse, size_t limit, int null_direction_hint, Permutation & res) const override;
    void updatePermutation(bool reverse, size_t limit, int null_direction_hint, Permutation & res, EqualRanges & equal_range) const override;
    void getPermutationWithCollation(const Collator & collator, bool reverse, size_t limit, int null_direction_hint, Permutation & res) const override;
    void updatePermutationWithCollation(
        const Collator & collator, bool reverse, size_t limit, int null_direction_hint, Permutation & res, EqualRanges& equal_range) const override;
    void reserve(size_t n) override;
    size_t byteSize() const override;
    size_t byteSizeAt(size_t n) const override;
    size_t allocatedBytes() const override;
    void protect() override;
    ColumnPtr replicate(const Offsets & replicate_offsets) const override;
    void updateHashWithValue(size_t n, SipHash & hash) const override;
    void updateWeakHash32(WeakHash32 & hash) const override;
    void updateHashFast(SipHash & hash) const override;
    void getExtremes(Field & min, Field & max) const override;

    void getIndicesOfNonDefaultValues(IColumn::Offsets & indices, size_t from, size_t limit) const override;
    size_t getNumberOfDefaultRows(size_t step) const override;

    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override;

    void gather(ColumnGathererStream & gatherer_stream) override;

    ColumnPtr compress() const override;

    void forEachSubcolumn(ColumnCallback callback) override
    {
        callback(values);
        callback(offsets);
    }

    bool structureEquals(const IColumn & rhs) const override;

    bool isNullable() const override { return values->isNullable(); }
    bool isFixedAndContiguous() const override { return false; }
    bool valuesHaveFixedSize() const override { return values->valuesHaveFixedSize(); }
    size_t sizeOfValueIfFixed() const override { return values->sizeOfValueIfFixed() + values->sizeOfValueIfFixed(); }
    bool isCollationSupported() const override { return values->isCollationSupported(); }

    size_t getNumberOfDefaults() const { return _size - offsets->size(); }
    size_t getNumberOfTrailingDefaults() const
    {
        return offsets->empty() ? _size : _size - getOffsetsData().back() - 1;
    }

    size_t getValueIndex(size_t n) const;

    const IColumn & getValuesColumn() const { return *values; }
    IColumn & getValuesColumn() { return *values; }

    const ColumnPtr & getValuesPtr() const { return values; }
    ColumnPtr & getValuesPtr() { return values; }

    const IColumn::Offsets & getOffsetsData() const;
    IColumn::Offsets & getOffsetsData();

    const ColumnPtr & getOffsetsPtr() const { return offsets; }
    ColumnPtr & getOffsetsPtr() { return offsets; }

    const IColumn & getOffsetsColumn() const { return *offsets; }
    IColumn & getOffsetsColumn() { return *offsets; }

private:
    [[noreturn]] void throwMustBeDense() const
    {
        throw Exception("Not implemented for ColumnSparse", ErrorCodes::LOGICAL_ERROR);
    }

    WrappedPtr values;
    WrappedPtr offsets;
    size_t _size;
};

}
