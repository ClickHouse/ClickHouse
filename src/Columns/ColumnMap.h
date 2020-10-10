#pragma once

#include <Core/Block.h>

namespace DB
{

/** Column, that is just group of 2 columns.
  *
  */
class ColumnMap final : public COWHelper<IColumn, ColumnMap>
{
private:
    friend class COWHelper<IColumn, ColumnMap>;

    using MapColumns = std::vector<WrappedPtr>;
    MapColumns columns;

    template <bool positive>
    struct Less;

    explicit ColumnMap(MutableColumns && columns);
    ColumnMap(const ColumnMap &) = default;

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumn, ColumnMap>;
    static Ptr create(const Columns & columns);
    static Ptr create(const MapColumns & columns);
    static Ptr create(Columns && arg) { return create(arg); }

    template <typename Arg, typename = typename std::enable_if<std::is_rvalue_reference<Arg &&>::value>::type>
    static MutablePtr create(Arg && arg) { return Base::create(std::forward<Arg>(arg)); }

    std::string getName() const override;
    const char * getFamilyName() const override { return "Map"; }
    TypeIndex getDataType() const override { return TypeIndex::Map; }

    MutableColumnPtr cloneEmpty() const override;
    MutableColumnPtr cloneResized(size_t size) const override;

    size_t size() const override
    {
        return columns.at(0)->size();
    }

    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;

    StringRef getDataAt(size_t n) const override;
    void insertData(const char * pos, size_t length) override;
    void insert(const Field & x) override;
    void insertDefault() override;
    void popBack(size_t n) override;
    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    const char * deserializeAndInsertFromArena(const char * pos) override;
    void updateHashWithValue(size_t n, SipHash & hash) const override;
    void updateWeakHash32(WeakHash32 & hash) const override;
    void updateHashFast(SipHash & hash) const override;
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    ColumnPtr replicate(const Offsets & offsets) const override;
    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override;
    void gather(ColumnGathererStream & gatherer_stream) override;
    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
    void compareColumn(const IColumn & rhs, size_t rhs_row_num,
                       PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                       int direction, int nan_direction_hint) const override;
    void getExtremes(Field & min, Field & max) const override;
    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override;
    void updatePermutation(bool reverse, size_t limit, int nan_direction_hint, IColumn::Permutation & res, EqualRanges & equal_range) const override;
    void reserve(size_t n) override;
    size_t byteSize() const override;
    size_t allocatedBytes() const override;
    void protect() override;
    void forEachSubcolumn(ColumnCallback callback) override;
    bool structureEquals(const IColumn & rhs) const override;

    const IColumn & getColumn(size_t idx) const { return *columns[idx]; }
    IColumn & getColumn(size_t idx) { return *columns[idx]; }

    const MapColumns & getColumns() const { return columns; }
    Columns getColumnsCopy() const { return {columns.begin(), columns.end()}; }

    const ColumnPtr & getColumnPtr(size_t idx) const { return columns[idx]; }
};

}
