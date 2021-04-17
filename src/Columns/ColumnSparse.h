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

    template <typename TColumnPtr, typename = typename std::enable_if<IsMutableColumns<TColumnPtr>::value>::type>
    static MutablePtr create(TColumnPtr && values_, TColumnPtr && offsets_, size_t size_)
    {
        return Base::create(std::move(values_), std::move(offsets_), size_);
    }

    static Ptr create(const ColumnPtr & values_)
    {
        return Base::create(values_->assumeMutable());
    }

    template <typename TColumnPtr, typename = typename std::enable_if<IsMutableColumns<TColumnPtr>::value>::type>
    static MutablePtr create(TColumnPtr && values_)
    {
        return Base::create(std::forward<TColumnPtr>(values_));
    }

    bool isSparse() const override { return true; }
    const char * getFamilyName() const override { return "Sparse"; }
    std::string getName() const override { return "Sparse(" + values->getName() + ")"; }
    TypeIndex getDataType() const override { return values->getDataType(); }
    MutableColumnPtr cloneResized(size_t new_size) const override;
    size_t size() const override { return _size; }
    bool isNullAt(size_t n) const override;
    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;
    bool getBool(size_t n) const override;
    Float64 getFloat64(size_t n) const override;
    Float32 getFloat32(size_t n) const override;
    UInt64 getUInt(size_t n) const override;
    Int64 getInt(size_t n) const override;
    UInt64 get64(size_t n) const override;
    StringRef getDataAt(size_t n) const override;

    ColumnPtr convertToFullColumnIfSparse() const override;

    /// Will insert null value if pos=nullptr
    void insertData(const char * pos, size_t length) override;
    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    const char * deserializeAndInsertFromArena(const char * pos) override;
    const char * skipSerializedInArena(const char *) const override;
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    void insert(const Field & x) override;
    void insertFrom(const IColumn & src, size_t n) override;
    void insertDefault() override;
    void insertManyDefaults(size_t length) override;

    void popBack(size_t n) override;
    ColumnPtr filter(const Filter & filt, ssize_t) const override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;

    ColumnPtr index(const IColumn & indexes, size_t limit) const override;

    template <typename Type>
    ColumnPtr indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const;

    int compareAt(size_t n, size_t m, const IColumn & rhs_, int null_direction_hint) const override;
    void compareColumn(const IColumn & rhs, size_t rhs_row_num,
                       PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                       int direction, int nan_direction_hint) const override;

    int compareAtWithCollation(size_t n, size_t m, const IColumn & rhs, int null_direction_hint, const Collator & collator) const override;
    bool hasEqualValues() const override;

    void getPermutationImpl(bool reverse, size_t limit, int null_direction_hint, Permutation & res, const Collator * collator) const;

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

    void forEachSubcolumn(ColumnCallback callback) override;

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
    class Iterator
    {
    public:
        Iterator(const PaddedPODArray<UInt64> & offsets_, size_t size_, size_t current_offset_, size_t current_row_)
            : offsets(offsets_), size(size_), current_offset(current_offset_), current_row(current_row_)
        {
        }

        bool isDefault() const { return current_offset == offsets.size() || current_row != offsets[current_offset]; }
        size_t getValueIndex() const { return isDefault() ? 0 : current_offset + 1; }
        size_t getCurrentRow() const { return current_row; }

        bool operator==(const Iterator & other) const
        {
            return size == other.size
                && current_offset == other.current_offset
                && current_row == other.current_row;
        }

        bool operator!=(const Iterator & other) const { return !(*this == other); }

        Iterator operator++()
        {
            if (!isDefault())
                ++current_offset;
            ++current_row;
            return *this;
        }

    private:
        const PaddedPODArray<UInt64> & offsets;
        const size_t size;
        size_t current_offset;
        size_t current_row;
    };

    Iterator begin() const { return Iterator(getOffsetsData(), _size, 0, 0); }
    Iterator end() const { return Iterator(getOffsetsData(), _size, getOffsetsData().size(), _size); }

    WrappedPtr values;
    WrappedPtr offsets;
    size_t _size;
};

ColumnPtr recursiveRemoveSparse(const ColumnPtr & column);

}
