#pragma once

#include <Core/Block.h>


namespace DB
{

/** Column, that is just group of few another columns.
  *
  * For constant Tuples, see ColumnConst.
  * Mixed constant/non-constant columns is prohibited in tuple
  *  for implementation simplicity.
  */
class ColumnTuple final : public COWPtrHelper<IColumn, ColumnTuple>
{
private:
    friend class COWPtrHelper<IColumn, ColumnTuple>;

    Columns columns;

    template <bool positive>
    struct Less;

    ColumnTuple(const Columns & columns);
    ColumnTuple(const ColumnTuple &) = default;

public:
    std::string getName() const override;
    const char * getFamilyName() const override { return "Tuple"; }

    MutableColumnPtr cloneEmpty() const override;

    size_t size() const override
    {
        return columns.at(0)->size();
    }

    Field operator[](size_t n) const override;
    void get(size_t n, Field & res) const override;

    StringRef getDataAt(size_t n) const override;
    void insertData(const char * pos, size_t length) override;
    void insert(const Field & x) override;
    void insertFrom(const IColumn & src_, size_t n) override;
    void insertDefault() override;
    void popBack(size_t n) override;
    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;
    const char * deserializeAndInsertFromArena(const char * pos) override;
    void updateHashWithValue(size_t n, SipHash & hash) const override;
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    MutableColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    MutableColumnPtr permute(const Permutation & perm, size_t limit) const override;
    MutableColumnPtr replicate(const Offsets & offsets) const override;
    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override;
    void gather(ColumnGathererStream & gatherer_stream) override;
    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
    void getExtremes(Field & min, Field & max) const override;
    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override;
    void reserve(size_t n) override;
    size_t byteSize() const override;
    size_t allocatedBytes() const override;
    void forEachSubcolumn(ColumnCallback callback) override;

    size_t tupleSize() const { return columns.size(); }

    const IColumn & getColumn(size_t idx) const { return *columns[idx]; }
    IColumn & getColumn(size_t idx) { return columns[idx]->assumeMutableRef(); }

    const Columns & getColumns() const { return columns; }

    const ColumnPtr & getColumnPtr(size_t idx) const { return columns[idx]; }
};


}
