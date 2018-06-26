#pragma once

#include <Common/Arena.h>
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
    IColumnDummy() : s(0) {}
    IColumnDummy(size_t s_) : s(s_) {}

public:
    virtual MutableColumnPtr cloneDummy(size_t s_) const = 0;

    MutableColumnPtr cloneResized(size_t s_) const override { return cloneDummy(s_); }
    size_t size() const override { return s; }
    void insertDefault() override { ++s; }
    void popBack(size_t n) override { s -= n; }
    size_t byteSize() const override { return 0; }
    size_t allocatedBytes() const override { return 0; }
    int compareAt(size_t, size_t, const IColumn &, int) const override { return 0; }

    Field operator[](size_t) const override { throw Exception("Cannot get value from " + getName(), ErrorCodes::NOT_IMPLEMENTED); }
    void get(size_t, Field &) const override { throw Exception("Cannot get value from " + getName(), ErrorCodes::NOT_IMPLEMENTED); }
    void insert(const Field &) override { throw Exception("Cannot insert element into " + getName(), ErrorCodes::NOT_IMPLEMENTED); }

    StringRef getDataAt(size_t) const override
    {
        return {};
    }

    void insertData(const char *, size_t) override
    {
        ++s;
    }

    StringRef serializeValueIntoArena(size_t /*n*/, Arena & arena, char const *& begin) const override
    {
        return { arena.allocContinue(0, begin), 0 };
    }

    const char * deserializeAndInsertFromArena(const char * pos) override
    {
        ++s;
        return pos;
    }

    void updateHashWithValue(size_t /*n*/, SipHash & /*hash*/) const override
    {
    }

    void insertFrom(const IColumn &, size_t) override
    {
        ++s;
    }

    void insertRangeFrom(const IColumn & /*src*/, size_t /*start*/, size_t length) override
    {
        s += length;
    }

    ColumnPtr filter(const Filter & filt, ssize_t /*result_size_hint*/) const override
    {
        return cloneDummy(countBytesInFilter(filt));
    }

    ColumnPtr permute(const Permutation & perm, size_t limit) const override
    {
        if (s != perm.size())
            throw Exception("Size of permutation doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        return cloneDummy(limit ? std::min(s, limit) : s);
    }

    ColumnPtr index(const IColumn & indexes, size_t limit) const override
    {
        if (indexes.size() < limit)
            throw Exception("Size of indexes is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        return cloneDummy(limit ? limit : s);
    }

    void getPermutation(bool /*reverse*/, size_t /*limit*/, int /*nan_direction_hint*/, Permutation & res) const override
    {
        res.resize(s);
        for (size_t i = 0; i < s; ++i)
            res[i] = i;
    }

    ColumnPtr replicate(const Offsets & offsets) const override
    {
        if (s != offsets.size())
            throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        return cloneDummy(s == 0 ? 0 : offsets.back());
    }

    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override
    {
        if (s != selector.size())
            throw Exception("Size of selector doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        std::vector<size_t> counts(num_columns);
        for (auto idx : selector)
            ++counts[idx];

        MutableColumns res(num_columns);
        for (size_t i = 0; i < num_columns; ++i)
            res[i] = cloneResized(counts[i]);

        return res;
    }

    void gather(ColumnGathererStream &) override
    {
        throw Exception("Method gather is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    void getExtremes(Field &, Field &) const override
    {
    }

    void addSize(size_t delta)
    {
        s += delta;
    }

    bool isDummy() const override
    {
        return true;
    }

protected:
    size_t s;
};

}
