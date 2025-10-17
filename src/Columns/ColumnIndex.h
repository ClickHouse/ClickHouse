#pragma once

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
    explicit ColumnIndex(MutableColumnPtr && indexes_);
    explicit ColumnIndex(ColumnPtr indexes_);

    const ColumnPtr & getIndexes() const { return indexes; }
    IColumn::WrappedPtr & getIndexesPtr() { return indexes; }
    const IColumn::WrappedPtr & getIndexesPtr() const { return indexes; }
    size_t getIndexAt(size_t row) const;
    size_t getMaxIndex() const;
    void insertIndex(size_t index);
    void insertManyIndexes(size_t index, size_t length);
    void insertIndexesRange(const IColumn & column, size_t offset, size_t limit);
    void insertIndexesRange(size_t start, size_t length);

    void popBack(size_t n) { indexes->popBack(n); }
    void reserve(size_t n) { indexes->reserve(n); }
    void resizeAssumeReserve(size_t n);
    size_t capacity() const { return indexes->capacity(); }
    void shrinkToFit() { indexes->shrinkToFit(); }

    size_t getMaxIndexForCurrentType() const;

    static size_t getSizeOfIndexType(const IColumn & column, size_t hint);
    size_t getSizeOfIndexType() const { return size_of_type; }

    void checkSizeOfType();

    MutableColumnPtr detachIndexes() { return IColumn::mutate(std::move(indexes)); }
    void attachIndexes(MutableColumnPtr indexes_);

    void countKeys(ColumnUInt64::Container & counts) const;

    bool containsDefault() const;

    WeakHash32 getWeakHash(const WeakHash32 & dict_hash) const;

    void collectSerializedValueSizes(PaddedPODArray<UInt64> & sizes, const PaddedPODArray<UInt64> & dict_sizes) const;

    void callForIndexes(std::function<void(size_t, size_t)> && callback, size_t start, size_t end) const;

private:
    template <typename Callback>
    static void callForType(Callback && callback, size_t size_of_type);

    template <typename IndexType>
    typename ColumnVector<IndexType>::Container & getIndexesData();

    template <typename IndexType>
    const typename ColumnVector<IndexType>::Container & getIndexesData() const;

    void updateSizeOfType() { size_of_type = getSizeOfIndexType(*indexes, size_of_type); }
    void expandType();

    template <typename IndexType>
    void convertIndexes();

    IColumn::WrappedPtr indexes;
    size_t size_of_type = 0;
};

}
