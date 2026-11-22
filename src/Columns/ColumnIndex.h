#pragma once

#include <optional>
#include <utility>

#include <Columns/IColumn.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{

/// Wrapper around ColumnVector to store indexes.
/// Supports dynamic change of underlying index type (UInt8 -> UInt16 -> UInt32 -> UInt64).
class ColumnIndex
{
public:
    ColumnIndex();
    ColumnIndex(const ColumnIndex & other) = default;
    ColumnIndex(ColumnIndex && other) = default;
    explicit ColumnIndex(MutableColumnPtr && indexes_);
    explicit ColumnIndex(ColumnPtr indexes_);

    [[nodiscard]] const ColumnPtr & getIndexes() const { return indexes; }
    [[nodiscard]] const IColumn::WrappedPtr & getIndexesPtr() const { return indexes; }
    IColumn::WrappedPtr & getIndexesPtr() { return indexes; }
    [[nodiscard]] size_t getIndexAt(size_t row) const;
    void insertIndex(size_t index);
    void insertManyIndexes(size_t index, size_t length);
    void insertIndexesRange(const IColumn & column, size_t offset, size_t limit);
    void insertIndexesRangeWithShift(const IColumn & column, size_t offset, size_t limit, size_t shift, size_t max_result_index);
    void insertIndexesRange(size_t start, size_t length);

    void popBack(size_t n) { indexes->popBack(n); }
    void reserve(size_t n) { indexes->reserve(n); }
    void resizeAssumeReserve(size_t n);
    [[nodiscard]] size_t size() const { return indexes->size(); }
    [[nodiscard]] size_t capacity() const { return indexes->capacity(); }
    void shrinkToFit() { indexes->shrinkToFit(); }

    static size_t getSizeOfIndexType(const IColumn & column, size_t hint);
    [[nodiscard]] size_t getSizeOfIndexType() const { return size_of_type; }

    MutableColumnPtr detachIndexes() { return IColumn::mutate(std::move(indexes)); }
    void attachIndexes(MutableColumnPtr indexes_);

    /// Count the number of occurrences of each row in indexed data.
    void countRowsInIndexedData(ColumnUInt64::Container & counts) const;

    [[nodiscard]] bool containsDefault() const;

    /// Per-row hash of indexed data: gathers the precomputed per-dictionary-row hashes
    /// `dict_hash` by index and writes (or combines, when initial == false) into `hash_out`.
    /// `hash_out[i]` corresponds to row `row_begin + i`.
    void computeHashInto(
        const PaddedPODArray<UInt32> & dict_hash, size_t row_begin, size_t row_end, UInt32 * hash_out, bool initial) const;

    void collectSerializedValueSizes(PaddedPODArray<UInt64> & sizes, const PaddedPODArray<UInt64> & dict_sizes) const;

    /// Call specified callback on each pair (row, index) in the specified range.
    void callForIndexes(std::function<void(size_t, size_t)> && callback, size_t start, size_t end) const;

    /// Remove all rows in the indexed data that are not reverenced in the indexes and adjust remaining indexes after it.
    /// Returns filtered indexed data where all rows are referenced in the indexes.
    ColumnPtr removeUnusedRowsInIndexedData(const ColumnPtr & indexed_data);

    void removeUnusedRowsInIndexedData(MutableColumnPtr & indexed_data);

    struct CompactIndexedColumnsResult
    {
        ColumnPtr compact_indexes;
        Columns compact_indexed_columns;
    };

    [[nodiscard]] CompactIndexedColumnsResult buildCompactIndexedColumns(const Columns & indexed_columns) const;

    /// Collect rows where mask[index] is 1.
    void getIndexesByMask(IColumn::Offsets & result_indexes, const PaddedPODArray<UInt8> & mask, size_t start, size_t end) const;

    void expand(const IColumn::Filter & mask, bool inverted);

    /// Calls `f` with the indexes as a `PaddedPODArray` of their unsigned type.
    template <typename F>
    decltype(auto) callForIndexesData(F && f) const
    {
        return callForIndexesData(*indexes, std::forward<F>(f));
    }

    /// The same for a column of one of the unsigned types that `ColumnIndex` takes, such as the codes of a
    /// dictionary.
    template <typename F>
    static decltype(auto) callForIndexesData(const IColumn & column, F && f)
    {
        if (const auto * column_uint8 = typeid_cast<const ColumnUInt8 *>(&column))
            return f(column_uint8->getData());
        if (const auto * column_uint16 = typeid_cast<const ColumnUInt16 *>(&column))
            return f(column_uint16->getData());
        if (const auto * column_uint32 = typeid_cast<const ColumnUInt32 *>(&column))
            return f(column_uint32->getData());
        if (const auto * column_uint64 = typeid_cast<const ColumnUInt64 *>(&column))
            return f(column_uint64->getData());
        throwUnexpectedIndexesType(column);
    }

    /// Set the stored index to `value` for every row where `mask` is zero.
    /// When `offset` is given, only the rows in `[offset, offset + mask.size())` are affected and `mask`
    /// covers just that range; otherwise it must cover the whole column.
    void setIndexesWhereMaskZero(const IColumn::Filter & mask, UInt64 value, size_t offset = 0);

private:
    [[noreturn]] static void throwUnexpectedIndexesType(const IColumn & column);

    [[nodiscard]] size_t getMaxIndexForCurrentType() const;

    void checkSizeOfType();

    template <typename Callback>
    static void callForType(Callback && callback, size_t size_of_type);

    template <typename IndexType>
    typename ColumnVector<IndexType>::Container & getIndexesData();

    template <typename IndexType>
    const typename ColumnVector<IndexType>::Container & getIndexesData() const;

    /// `indexes` may be shared with the source column when called from
    /// `ColumnLowCardinality::create(const ColumnPtr &, const ColumnPtr &, bool)`,
    /// so the non-const `WrappedPtr::operator*` would go through `assumeMutableRef`
    /// and trip `chassert(use_count() == 1)`. Use the const overload — read access
    /// is enough to inspect the index type.
    void updateSizeOfType() { size_of_type = getSizeOfIndexType(*std::as_const(indexes), size_of_type); }
    void expandType();

    template <typename IndexType>
    void convertIndexes();

    [[nodiscard]] std::optional<IColumn::Filter> buildUsedRowsFilter(size_t indexed_data_size) const;
    size_t compactIndexes(const IColumn::Filter & filter, size_t indexed_data_size);

    IColumn::WrappedPtr indexes;
    size_t size_of_type = 0;
};

}
