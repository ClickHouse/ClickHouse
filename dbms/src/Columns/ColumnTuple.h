#pragma once

#include <Core/Block.h>


namespace DB
{

/** Column, that is just group of few another columns.
  *
  * For constant Tuples, see ColumnConstTuple.
  * Mixed constant/non-constant columns is prohibited in tuple
  *  for implementation simplicity.
  */
class ColumnTuple final : public IColumn
{
private:
    Block data;
    Columns columns;

    template <bool positive>
    struct Less;

public:
    ColumnTuple() {}
    ColumnTuple(Block data_);

    std::string getName() const override { return "Tuple"; }

    ColumnPtr cloneEmpty() const override;

    size_t size() const override
    {
        return data.rows();
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
    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;
    ColumnPtr replicate(const Offsets_t & offsets) const override;
    Columns scatter(ColumnIndex num_columns, const Selector & selector) const override;
    void gather(ColumnGathererStream & gatherer_stream) override;
    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;
    void getExtremes(Field & min, Field & max) const override;
    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override;
    void reserve(size_t n) override;
    size_t byteSize() const override;
    size_t allocatedSize() const override;
    ColumnPtr convertToFullColumnIfConst() const override;

    const Block & getData() const { return data; }
    Block & getData() { return data; }

    const Columns & getColumns() const { return columns; }
    Columns & getColumns() { return columns; }
};


}
