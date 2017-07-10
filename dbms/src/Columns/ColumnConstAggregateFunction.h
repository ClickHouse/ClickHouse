#pragma once
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeAggregateFunction.h>


namespace DB
{
class ColumnConstAggregateFunction : public IColumnConst
{
public:
    ColumnConstAggregateFunction(size_t size, const Field & value_, const DataTypePtr & data_type_)
        : data_type(data_type_), value(value_), s(size)
    {
    }

    String getName() const override
    {
        return "ColumnConstAggregateFunction";
    }

    bool isConst() const override
    {
        return true;
    }

    ColumnPtr convertToFullColumnIfConst() const override;

    ColumnPtr convertToFullColumn() const override;

    ColumnPtr cloneResized(size_t new_size) const override;

    size_t size() const override
    {
        return s;
    }

    Field operator[](size_t n) const override
    {
        /// NOTE: there are no out of bounds check (like in ColumnConstBase)
        return value;
    }

    void get(size_t n, Field & res) const override
    {
        res = value;
    }

    StringRef getDataAt(size_t n) const override;

    void insert(const Field & x) override;

    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;

    void insertData(const char * pos, size_t length) override;

    void insertDefault() override
    {
        ++s;
    }

    void popBack(size_t n) override
    {
        s -= n;
    }

    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;

    const char * deserializeAndInsertFromArena(const char * pos) override;

    void updateHashWithValue(size_t n, SipHash & hash) const override;

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;

    ColumnPtr permute(const Permutation & perm, size_t limit) const override;

    int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override
    {
        return 0;
    }

    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override;

    ColumnPtr replicate(const Offsets_t & offsets) const override;

    void gather(ColumnGathererStream &) override
    {
        throw Exception("Cannot gather into constant column " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void getExtremes(Field & min, Field & max) const override;

    size_t byteSize() const override;

    size_t allocatedSize() const override;

private:
    DataTypePtr data_type;
    Field value;
    size_t s;

    AggregateFunctionPtr getAggregateFunction() const;

    bool equalsFuncAndValue(const IColumn & rhs) const;
};
}
