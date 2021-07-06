#pragma once

#include <Columns/IColumn.h>


/// Common helper methods for implementation of different columns.

namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

/// Counts how many bytes of `filt` are greater than zero.
size_t countBytesInFilter(const UInt8 * filt, size_t sz);
size_t countBytesInFilter(const IColumn::Filter & filt);
size_t countBytesInFilterWithNull(const IColumn::Filter & filt, const UInt8 * null_map);

/// Returns vector with num_columns elements. vector[i] is the count of i values in selector.
/// Selector must contain values from 0 to num_columns - 1. NOTE: this is not checked.
std::vector<size_t> countColumnsSizeInSelector(IColumn::ColumnIndex num_columns, const IColumn::Selector & selector);

/// Returns true, if the memory contains only zeros.
bool memoryIsZero(const void * data, size_t size);
bool memoryIsByte(const void * data, size_t size, uint8_t byte);

/// The general implementation of `filter` function for ColumnArray and ColumnString.
template <typename T>
void filterArraysImpl(
    const PaddedPODArray<T> & src_elems, const IColumn::Offsets & src_offsets,
    PaddedPODArray<T> & res_elems, IColumn::Offsets & res_offsets,
    const IColumn::Filter & filt, ssize_t result_size_hint);

/// Same as above, but not fills res_offsets.
template <typename T>
void filterArraysImplOnlyData(
    const PaddedPODArray<T> & src_elems, const IColumn::Offsets & src_offsets,
    PaddedPODArray<T> & res_elems,
    const IColumn::Filter & filt, ssize_t result_size_hint);

namespace detail
{
    template <typename T>
    const PaddedPODArray<T> * getIndexesData(const IColumn & indexes);
}

/// Check limit <= indexes->size() and call column.indexImpl(const PaddedPodArray<Type> & indexes, UInt64 limit).
template <typename Column>
ColumnPtr selectIndexImpl(const Column & column, const IColumn & indexes, size_t limit)
{
    if (limit == 0)
        limit = indexes.size();

    if (indexes.size() < limit)
        throw Exception("Size of indexes is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (auto * data_uint8 = detail::getIndexesData<UInt8>(indexes))
        return column.template indexImpl<UInt8>(*data_uint8, limit);
    else if (auto * data_uint16 = detail::getIndexesData<UInt16>(indexes))
        return column.template indexImpl<UInt16>(*data_uint16, limit);
    else if (auto * data_uint32 = detail::getIndexesData<UInt32>(indexes))
        return column.template indexImpl<UInt32>(*data_uint32, limit);
    else if (auto * data_uint64 = detail::getIndexesData<UInt64>(indexes))
        return column.template indexImpl<UInt64>(*data_uint64, limit);
    else
        throw Exception("Indexes column for IColumn::select must be ColumnUInt, got " + indexes.getName(),
                        ErrorCodes::LOGICAL_ERROR);
}

#define INSTANTIATE_INDEX_IMPL(Column) \
    template ColumnPtr Column::indexImpl<UInt8>(const PaddedPODArray<UInt8> & indexes, size_t limit) const; \
    template ColumnPtr Column::indexImpl<UInt16>(const PaddedPODArray<UInt16> & indexes, size_t limit) const; \
    template ColumnPtr Column::indexImpl<UInt32>(const PaddedPODArray<UInt32> & indexes, size_t limit) const; \
    template ColumnPtr Column::indexImpl<UInt64>(const PaddedPODArray<UInt64> & indexes, size_t limit) const;
}
