#pragma once
#include <optional>
#include <Columns/IColumn.h>
#include <Common/WeakHash.h>

namespace DB
{

[[noreturn]] void throwNotImplementedForColumnUnique(const char * method);

/// Sort of a dictionary
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

    virtual bool nestedColumnIsNullable() const = 0;
    virtual void nestedToNullable() = 0;
    virtual void nestedRemoveNullable() = 0;
    /// Returns an empty dictionary with nullable nested type and all required special values.
    virtual MutableColumnPtr cloneEmptyNullable() const = 0;

    /// Returns array with StringViewHash calculated for each row of getNestedNotNullableColumn() column.
    /// Returns nullptr if nested column doesn't contain strings. Otherwise calculates hash (if it wasn't).
    /// Uses thread-safe cache.
    virtual const UInt64 * tryGetSavedHash() const = 0;

    size_t size() const override { return getNestedNotNullableColumn()->size(); }

    /// Appends new value at the end of column (column's size is increased by 1).
    /// Is used to transform raw strings to Blocks (for example, inside input format parsers)
    virtual size_t uniqueInsert(const Field & x) = 0;

    /// Appends new value at the end of column if value has appropriate type (column's size is increased by 1).
    /// Return true if value is inserted and set @index to inserted value index and false otherwise.
    virtual bool tryUniqueInsert(const Field & x, size_t & index) = 0;

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

    virtual size_t getDefaultValueIndex() const = 0;  /// Nullable ? getNullValueIndex : getNestedTypeDefaultValueIndex
    virtual size_t getNullValueIndex() const = 0;  /// Throws if not nullable.
    virtual size_t getNestedTypeDefaultValueIndex() const = 0;  /// removeNullable()->getDefault() value index
    virtual bool canContainNulls() const = 0;

    virtual size_t uniqueDeserializeAndInsertFromArena(ReadBuffer & in, const IColumn::SerializationSettings * settings) = 0;
    virtual size_t uniqueDeserializeAndInsertAggregationStateValueFromArena(ReadBuffer & in) = 0;

    /// Returns dictionary hash which is SipHash is applied to each row of nested column.
    virtual UInt128 getHash() const = 0;

    const char * getFamilyName() const override { return "Unique"; }
    TypeIndex getDataType() const override { return getNestedColumn()->getDataType(); }

    void insert(const Field &) override
    {
        throwNotImplementedForColumnUnique("insert");
    }

    bool tryInsert(const Field &) override
    {
        throwNotImplementedForColumnUnique("tryInsert");
    }

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertRangeFrom(const IColumn &, size_t, size_t) override
#else
    void doInsertRangeFrom(const IColumn &, size_t, size_t) override
#endif
    {
        throwNotImplementedForColumnUnique("insertRangeFrom");
    }

    void insertData(const char *, size_t) override
    {
        throwNotImplementedForColumnUnique("insertData");
    }

    void insertDefault() override
    {
        throwNotImplementedForColumnUnique("insertDefault");
    }

    void popBack(size_t) override
    {
        throwNotImplementedForColumnUnique("popBack");
    }

    void gather(ColumnGathererStream &) override
    {
        throwNotImplementedForColumnUnique("gather");
    }

    void deserializeAndInsertFromArena(ReadBuffer &, const IColumn::SerializationSettings *) override
    {
        throwNotImplementedForColumnUnique("deserializeAndInsertFromArena");
    }

    ColumnPtr index(const IColumn &, size_t) const override
    {
        throwNotImplementedForColumnUnique("index");
    }

    ColumnPtr cut(size_t, size_t) const override
    {
        throwNotImplementedForColumnUnique("cut");
    }

    ColumnPtr filter(const IColumn::Filter &, ssize_t) const override
    {
        throwNotImplementedForColumnUnique("filter");
    }

    void filter(const IColumn::Filter &) override
    {
        throwNotImplementedForColumnUnique("in-place filter");
    }

    void expand(const IColumn::Filter &, bool) override
    {
        throwNotImplementedForColumnUnique("expand");
    }

    ColumnPtr permute(const IColumn::Permutation &, size_t) const override
    {
        throwNotImplementedForColumnUnique("permute");
    }

    ColumnPtr replicate(const IColumn::Offsets &) const override
    {
        throwNotImplementedForColumnUnique("replicate");
    }

    void getPermutation(IColumn::PermutationSortDirection, IColumn::PermutationSortStability,
                    size_t, int, IColumn::Permutation &) const override
    {
        throwNotImplementedForColumnUnique("getPermutation");
    }

    void updatePermutation(PermutationSortDirection, PermutationSortStability,
                    size_t, int, Permutation &, EqualRanges &) const override
    {
        throwNotImplementedForColumnUnique("updatePermutation");
    }

    std::vector<MutableColumnPtr> scatter(size_t, const IColumn::Selector &) const override
    {
        throwNotImplementedForColumnUnique("scatter");
    }

    WeakHash32 getWeakHash32() const override
    {
        throwNotImplementedForColumnUnique("getWeakHash32");
    }

    void updateHashFast(SipHash &) const override
    {
        throwNotImplementedForColumnUnique("updateHashFast");
    }

    void compareColumn(const IColumn &, size_t, PaddedPODArray<UInt64> *, PaddedPODArray<Int8> &, int, int) const override
    {
        throwNotImplementedForColumnUnique("compareColumn");
    }

    bool hasEqualValues() const override
    {
        throwNotImplementedForColumnUnique("hasEqualValues");
    }

    ColumnPtr updateFrom(const IColumn::Patch &) const override
    {
        throwNotImplementedForColumnUnique("updateFrom");
    }

    void updateInplaceFrom(const IColumn::Patch &) override
    {
        throwNotImplementedForColumnUnique("updateInplaceFrom");
    }

    /** Given some value (usually, of type @e ColumnType) @p value that is convertible to std::string_view, obtains its
     * index in the DB::ColumnUnique::reverse_index hashtable.
     *
     * The reverse index (std::string_view => UInt64) is built lazily, so there are two variants:
     * - On the function call it's present. Therefore we obtain the index in O(1).
     * - The reverse index is absent. We search for the index linearly.
     *
     * @see DB::ReverseIndex
     * @see DB::ColumnUnique
     *
     * The most common example uses https://clickhouse.com/docs/sql-reference/data-types/lowcardinality/ columns.
     * Consider data type @e LC(String). The inner type here is @e String which is more or less a contiguous memory
     * region, so it can be easily represented as a @e std::string_view. So we pass that ref to this function and get its
     * index in the dictionary, which can be used to operate with the indices column.
     */
    virtual std::optional<UInt64> getOrFindValueIndex(std::string_view value) const = 0;
};

using ColumnUniquePtr = IColumnUnique::ColumnUniquePtr;
using MutableColumnUniquePtr = IColumnUnique::MutableColumnUniquePtr;

}
