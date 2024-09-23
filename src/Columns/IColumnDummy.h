#pragma once

#include <Columns/IColumn.h>
#include <Common/WeakHash.h>


namespace DB
{

class Arena;

/** Base class for columns-constants that contain a value that is not in the `Field`.
  * Not a full-fledged column and is used in a special way.
  */
class IColumnDummy : public IColumnHelper<IColumnDummy>
{
public:
    IColumnDummy() : s(0) {}
    explicit IColumnDummy(size_t s_) : s(s_) {}

    virtual MutableColumnPtr cloneDummy(size_t s_) const = 0;

    MutableColumnPtr cloneResized(size_t s_) const override { return cloneDummy(s_); }
    size_t size() const override { return s; }
    void insertDefault() override { ++s; }
    void popBack(size_t n) override { s -= n; }
    size_t byteSize() const override { return 0; }
    size_t byteSizeAt(size_t) const override { return 0; }
    size_t allocatedBytes() const override { return 0; }
#if !defined(DEBUG_OR_SANITIZER_BUILD)
    int compareAt(size_t, size_t, const IColumn &, int) const override { return 0; }
#else
    int doCompareAt(size_t, size_t, const IColumn &, int) const override { return 0; }
#endif
    void compareColumn(const IColumn &, size_t, PaddedPODArray<UInt64> *, PaddedPODArray<Int8> &, int, int) const override
    {
    }

    bool hasEqualValues() const override { return true; }

    Field operator[](size_t) const override;
    void get(size_t, Field &) const override;
    void insert(const Field &) override;
    bool tryInsert(const Field &) override { return false; }
    bool isDefaultAt(size_t) const override;

    StringRef getDataAt(size_t) const override
    {
        return {};
    }

    void insertData(const char *, size_t) override
    {
        ++s;
    }

    StringRef serializeValueIntoArena(size_t /*n*/, Arena & arena, char const *& begin) const override;

    const char * deserializeAndInsertFromArena(const char * pos) override;

    const char * skipSerializedInArena(const char * pos) const override;

    void updateHashWithValue(size_t /*n*/, SipHash & /*hash*/) const override
    {
    }

    WeakHash32 getWeakHash32() const override
    {
        return WeakHash32(s);
    }

    void updateHashFast(SipHash & /*hash*/) const override
    {
    }

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertFrom(const IColumn &, size_t) override
#else
    void doInsertFrom(const IColumn &, size_t) override
#endif
    {
        ++s;
    }

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertRangeFrom(const IColumn & /*src*/, size_t /*start*/, size_t length) override
#else
    void doInsertRangeFrom(const IColumn & /*src*/, size_t /*start*/, size_t length) override
#endif
    {
        s += length;
    }

    ColumnPtr filter(const Filter & filt, ssize_t /*result_size_hint*/) const override;

    void expand(const IColumn::Filter & mask, bool inverted) override;

    ColumnPtr permute(const Permutation & perm, size_t limit) const override;

    ColumnPtr index(const IColumn & indexes, size_t limit) const override;

    void getPermutation(IColumn::PermutationSortDirection /*direction*/, IColumn::PermutationSortStability /*stability*/,
                    size_t /*limit*/, int /*nan_direction_hint*/, Permutation & res) const override;

    void updatePermutation(IColumn::PermutationSortDirection /*direction*/, IColumn::PermutationSortStability /*stability*/,
                    size_t, int, Permutation &, EqualRanges&) const override
    {
    }

    ColumnPtr replicate(const Offsets & offsets) const override;

    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override;

    double getRatioOfDefaultRows(double) const override;
    UInt64 getNumberOfDefaultRows() const override;
    void getIndicesOfNonDefaultRows(Offsets &, size_t, size_t) const override;
    void gather(ColumnGathererStream &) override;

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
