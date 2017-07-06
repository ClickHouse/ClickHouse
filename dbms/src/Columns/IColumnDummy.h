#pragma once

#include <Columns/IColumn.h>
#include <Columns/ColumnsCommon.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
}


/** Base class for columns-constants that contain a value that is not in the `Field`.
  * Not a full-fledged column and is used in a special way.
  */
class IColumnDummy : public IColumn
{
public:
    IColumnDummy(size_t s_) : s(s_) {}

    virtual ColumnPtr cloneDummy(size_t s_) const = 0;

    ColumnPtr cloneResized(size_t s_) const override { return cloneDummy(s_); }
    bool isConst() const override { return true; }
    size_t size() const override { return s; }
    void insertDefault() override { ++s; }
    void popBack(size_t n) override { s -= n; }
    size_t byteSize() const override { return 0; }
    size_t allocatedSize() const override { return 0; }
    int compareAt(size_t n, size_t m, const IColumn & rhs_, int nan_direction_hint) const override { return 0; }

    Field operator[](size_t n) const override { throw Exception("Cannot get value from " + getName(), ErrorCodes::NOT_IMPLEMENTED); }
    void get(size_t n, Field & res) const override { throw Exception("Cannot get value from " + getName(), ErrorCodes::NOT_IMPLEMENTED); };
    void insert(const Field & x) override { throw Exception("Cannot insert element into " + getName(), ErrorCodes::NOT_IMPLEMENTED); }
    StringRef getDataAt(size_t n) const override { throw Exception("Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED); }
    void insertData(const char * pos, size_t length) override { throw Exception("Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED); }

    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override
    {
        throw Exception("Method serializeValueIntoArena is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    const char * deserializeAndInsertFromArena(const char * pos) override
    {
        throw Exception("Method deserializeAndInsertFromArena is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void updateHashWithValue(size_t n, SipHash & hash) const override
    {
        throw Exception("Method updateHashWithValue is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override
    {
        s += length;
    }

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override
    {
        return cloneDummy(countBytesInFilter(filt));
    }

    ColumnPtr permute(const Permutation & perm, size_t limit) const override
    {
        if (s != perm.size())
            throw Exception("Size of permutation doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        return cloneDummy(limit ? std::min(s, limit) : s);
    }

    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override
    {
        res.resize(s);
        for (size_t i = 0; i < s; ++i)
            res[i] = i;
    }

    ColumnPtr replicate(const Offsets_t & offsets) const override
    {
        if (s != offsets.size())
            throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        return cloneDummy(s == 0 ? 0 : offsets.back());
    }

    Columns scatter(ColumnIndex num_columns, const Selector & selector) const override
    {
        if (s != selector.size())
            throw Exception("Size of selector doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        std::vector<size_t> counts(num_columns);
        for (auto idx : selector)
            ++counts[idx];

        Columns res(num_columns);
        for (size_t i = 0; i < num_columns; ++i)
            res[i] = cloneResized(counts[i]);

        return res;
    }

    void gather(ColumnGathererStream &) override
    {
        throw Exception("Method gather is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void getExtremes(Field & min, Field & max) const override
    {
        throw Exception("Method getExtremes is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

private:
    size_t s;
};

}
