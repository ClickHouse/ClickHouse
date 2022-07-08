#pragma once

#include <Core/Block.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>

namespace DB
{

/** Column, that stores a nested Array(Tuple(key, value)) column.
 */
class ColumnMap final : public COWHelper<IColumn, ColumnMap>
{
private:
    friend class COWHelper<IColumn, ColumnMap>;

    WrappedPtr nested;

    explicit ColumnMap(MutableColumnPtr && nested_);

    ColumnMap(const ColumnMap &) = default;

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumn, ColumnMap>;

    static Ptr create(const ColumnPtr & keys, const ColumnPtr & values, const ColumnPtr & offsets)
    {
        auto nested_column = ColumnArray::create(ColumnTuple::create(Columns{keys, values}), offsets);
        return ColumnMap::create(nested_column);
    }

    static Ptr create(const ColumnPtr & column) { return ColumnMap::create(column->assumeMutable()); }
    static Ptr create(ColumnPtr && arg) { return create(arg); }

    template <typename ... Args, typename = typename std::enable_if<IsMutableColumns<Args ...>::value>::type>
    static MutablePtr create(Args &&... args) { return Base::create(std::forward<Args>(args)...); }

    std::string getName() const override;
    const char * getFamilyName() const override { return "Map"; }
    TypeIndex getDataType() const override { return TypeIndex::Map; }

    MutableColumnPtr cloneEmpty() const override;
    MutableColumnPtr cloneResized(size_t size) const override;

    size_t size() const override { return nested->size(); }

    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;

    bool isDefaultAt(size_t n) const override;
    StringRef getDataAt(size_t n) const override;
    void insertData(const char * pos, size_t length) override;
    void insert(const Field & x) override;
    void insertDefault() override;
    void popBack(size_t n) override;
    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    const char * deserializeAndInsertFromArena(const char * pos) override;
    const char * skipSerializedInArena(const char * pos) const override;
    void updateHashWithValue(size_t n, SipHash & hash) const override;
    void updateWeakHash32(WeakHash32 & hash) const override;
    void updateHashFast(SipHash & hash) const override;
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    void expand(const Filter & mask, bool inverted) override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    ColumnPtr replicate(const Offsets & offsets) const override;
    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override;
    void gather(ColumnGathererStream & gatherer_stream) override;
    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
    void compareColumn(const IColumn & rhs, size_t rhs_row_num,
                       PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                       int direction, int nan_direction_hint) const override;
    bool hasEqualValues() const override;
    void getExtremes(Field & min, Field & max) const override;
    void getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, IColumn::Permutation & res) const override;
    void updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                        size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges) const override;
    void reserve(size_t n) override;
    void ensureOwnership() override;
    size_t byteSize() const override;
    size_t byteSizeAt(size_t n) const override;
    size_t allocatedBytes() const override;
    void protect() override;
    void forEachSubcolumn(ColumnCallback callback) override;
    bool structureEquals(const IColumn & rhs) const override;
    double getRatioOfDefaultRows(double sample_ratio) const override;
    void getIndicesOfNonDefaultRows(Offsets & indices, size_t from, size_t limit) const override;

    const ColumnArray & getNestedColumn() const { return assert_cast<const ColumnArray &>(*nested); }
    ColumnArray & getNestedColumn() { return assert_cast<ColumnArray &>(*nested); }

    const ColumnPtr & getNestedColumnPtr() const { return nested; }
    ColumnPtr & getNestedColumnPtr() { return nested; }

    const ColumnTuple & getNestedData() const { return assert_cast<const ColumnTuple &>(getNestedColumn().getData()); }
    ColumnTuple & getNestedData() { return assert_cast<ColumnTuple &>(getNestedColumn().getData()); }

    ColumnPtr compress() const override;
};

}
