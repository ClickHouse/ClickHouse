#pragma once
#include <Columns/IColumn.h>

namespace DB
{

class IColumnUnique : public IColumn
{
public:
    using ColumnUniquePtr = IColumn::template immutable_ptr<IColumnUnique>;
    using MutableColumnUniquePtr = IColumn::template mutable_ptr<IColumnUnique>;

    /// Column always contains Null if it's Nullable and empty string if it's String or Nullable(String).
    /// So, size may be greater than the number of inserted unique values.
    virtual const ColumnPtr & getNestedColumn() const = 0;
    /// The same as getNestedColumn, but removes null map if nested column is nullable.
    virtual const ColumnPtr & getNestedNotNullableColumn() const = 0;

    size_t size() const override { return getNestedColumn()->size(); }

    /// Appends new value at the end of column (column's size is increased by 1).
    /// Is used to transform raw strings to Blocks (for example, inside input format parsers)
    virtual size_t uniqueInsert(const Field & x) = 0;

    virtual size_t uniqueInsertFrom(const IColumn & src, size_t n) = 0;
    /// Appends range of elements from other column.
    /// Could be used to concatenate columns.
    virtual MutableColumnPtr uniqueInsertRangeFrom(const IColumn & src, size_t start, size_t length) = 0;

    struct IndexesWithOverflow
    {
        MutableColumnPtr indexes;
        MutableColumnPtr overflowed_keys;
    };
    /// Like uniqueInsertRangeFrom, but doesn't insert keys if inner dictionary has more than max_dictionary_size keys.
    /// Keys that won't be inserted into dictionary will be into overflowed_keys, indexes will be calculated for
    /// concatenation of nested column (which can be got from getNestedColumn() function) and overflowed_keys.
    virtual IndexesWithOverflow uniqueInsertRangeWithOverflow(const IColumn & src, size_t start,
                                                              size_t length, size_t max_dictionary_size) = 0;

    /// Appends data located in specified memory chunk if it is possible (throws an exception if it cannot be implemented).
    /// Is used to optimize some computations (in aggregation, for example).
    /// Parameter length could be ignored if column values have fixed size.
    virtual size_t uniqueInsertData(const char * pos, size_t length) = 0;
    virtual size_t uniqueInsertDataWithTerminatingZero(const char * pos, size_t length) = 0;

    virtual size_t getDefaultValueIndex() const = 0;
    virtual size_t getNullValueIndex() const = 0;
    virtual bool canContainNulls() const = 0;

    virtual size_t uniqueDeserializeAndInsertFromArena(const char * pos, const char *& new_pos) = 0;

    const char * getFamilyName() const override { return "ColumnUnique"; }

    void insert(const Field &) override
    {
        throw Exception("Method insert is not supported for ColumnUnique.", ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertRangeFrom(const IColumn &, size_t, size_t) override
    {
        throw Exception("Method insertRangeFrom is not supported for ColumnUnique.", ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertData(const char *, size_t) override
    {
        throw Exception("Method insertData is not supported for ColumnUnique.", ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertDefault() override
    {
        throw Exception("Method insertDefault is not supported for ColumnUnique.", ErrorCodes::NOT_IMPLEMENTED);
    }

    void popBack(size_t) override
    {
        throw Exception("Method popBack is not supported for ColumnUnique.", ErrorCodes::NOT_IMPLEMENTED);
    }

    void gather(ColumnGathererStream &) override
    {
        throw Exception("Method gather is not supported for ColumnUnique.", ErrorCodes::NOT_IMPLEMENTED);
    }

    const char * deserializeAndInsertFromArena(const char *) override
    {
        throw Exception("Method deserializeAndInsertFromArena is not supported for ColumnUnique.", ErrorCodes::NOT_IMPLEMENTED);
    }

    ColumnPtr index(const IColumn &, size_t) const override
    {
        throw Exception("Method index is not supported for ColumnUnique.", ErrorCodes::NOT_IMPLEMENTED);
    }

    ColumnPtr cut(size_t, size_t) const override
    {
        throw Exception("Method cut is not supported for ColumnUnique.", ErrorCodes::NOT_IMPLEMENTED);
    }

    ColumnPtr filter(const IColumn::Filter &, ssize_t) const override
    {
        throw Exception("Method filter is not supported for ColumnUnique.", ErrorCodes::NOT_IMPLEMENTED);
    }

    ColumnPtr permute(const IColumn::Permutation &, size_t) const override
    {
        throw Exception("Method permute is not supported for ColumnUnique.", ErrorCodes::NOT_IMPLEMENTED);
    }

    ColumnPtr replicate(const IColumn::Offsets &) const override
    {
        throw Exception("Method replicate is not supported for ColumnUnique.", ErrorCodes::NOT_IMPLEMENTED);
    }

    void getPermutation(bool, size_t, int, IColumn::Permutation &) const override
    {
        throw Exception("Method getPermutation is not supported for ColumnUnique.", ErrorCodes::NOT_IMPLEMENTED);
    }

    std::vector<MutableColumnPtr> scatter(IColumn::ColumnIndex, const IColumn::Selector &) const override
    {
        throw Exception("Method scatter is not supported for ColumnUnique.", ErrorCodes::NOT_IMPLEMENTED);
    }
};

using ColumnUniquePtr = IColumnUnique::ColumnUniquePtr;
using MutableColumnUniquePtr = IColumnUnique::MutableColumnUniquePtr;

}
