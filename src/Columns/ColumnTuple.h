#pragma once

#include <Columns/IColumn.h>


namespace DB
{

/** Column, that is just group of few another columns.
  *
  * For constant Tuples, see ColumnConst.
  * Mixed constant/non-constant columns is prohibited in tuple
  *  for implementation simplicity.
  */
class ColumnTuple final : public COWHelper<IColumn, ColumnTuple>
{
private:
    friend class COWHelper<IColumn, ColumnTuple>;

    using TupleColumns = std::vector<WrappedPtr>;
    TupleColumns columns;

    template <bool positive>
    struct Less;

    explicit ColumnTuple(MutableColumns && columns);
    ColumnTuple(const ColumnTuple &) = default;

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumn, ColumnTuple>;
    static Ptr create(const Columns & columns);
    static Ptr create(const TupleColumns & columns);
    static Ptr create(Columns && arg) { return create(arg); }

    template <typename Arg>
    requires std::is_rvalue_reference_v<Arg &&>
    static MutablePtr create(Arg && arg) { return Base::create(std::forward<Arg>(arg)); }

    std::string getName() const override;
    const char * getFamilyName() const override { return "Tuple"; }
    TypeIndex getDataType() const override { return TypeIndex::Tuple; }

    MutableColumnPtr cloneEmpty() const override;
    MutableColumnPtr cloneResized(size_t size) const override;

    size_t size() const override
    {
        return columns.at(0)->size();
    }

    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;

    bool isDefaultAt(size_t n) const override;
    StringRef getDataAt(size_t n) const override;
    void insertData(const char * pos, size_t length) override;
    void insert(const Field & x) override;
    void insertFrom(const IColumn & src_, size_t n) override;
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
    int compareAtWithCollation(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint, const Collator & collator) const override;
    bool hasEqualValues() const override;
    void getExtremes(Field & min, Field & max) const override;
    void getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                    size_t limit, int nan_direction_hint, IColumn::Permutation & res) const override;
    void updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                    size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges) const override;
    void getPermutationWithCollation(const Collator & collator, IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                    size_t limit, int nan_direction_hint, IColumn::Permutation & res) const override;
    void updatePermutationWithCollation(const Collator & collator, IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                    size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges& equal_ranges) const override;
    void reserve(size_t n) override;
    void ensureOwnership() override;
    size_t byteSize() const override;
    size_t byteSizeAt(size_t n) const override;
    size_t allocatedBytes() const override;
    void protect() override;
    void forEachSubcolumn(ColumnCallback callback) override;
    bool structureEquals(const IColumn & rhs) const override;
    bool isCollationSupported() const override;
    ColumnPtr compress() const override;
    double getRatioOfDefaultRows(double sample_ratio) const override;
    void getIndicesOfNonDefaultRows(Offsets & indices, size_t from, size_t limit) const override;

    size_t tupleSize() const { return columns.size(); }

    const IColumn & getColumn(size_t idx) const { return *columns[idx]; }
    IColumn & getColumn(size_t idx) { return *columns[idx]; }

    const TupleColumns & getColumns() const { return columns; }
    Columns getColumnsCopy() const { return {columns.begin(), columns.end()}; }

    const ColumnPtr & getColumnPtr(size_t idx) const { return columns[idx]; }
    ColumnPtr & getColumnPtr(size_t idx) { return columns[idx]; }

private:
    int compareAtImpl(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint, const Collator * collator=nullptr) const;

    void getPermutationImpl(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability, size_t limit, int nan_direction_hint, Permutation & res, const Collator * collator) const;

    void updatePermutationImpl(
        IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability, size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_ranges, const Collator * collator=nullptr) const;
};


}
