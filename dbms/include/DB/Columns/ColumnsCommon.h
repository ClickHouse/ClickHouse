#pragma once

#include <DB/Columns/IColumn.h>


/// Common helper methods for implementation of different columns.

namespace DB
{

/// Counts how many bytes of `filt` are greater than zero.
size_t countBytesInFilter(const IColumn::Filter & filt);


/// Check if the memory region contains only zero bytes.
bool memoryIsZero(const void * memory, size_t size);


/// The general implementation of `filter` function for ColumnArray and ColumnString.
template <typename T>
void filterArraysImpl(
	const PaddedPODArray<T> & src_elems, const IColumn::Offsets_t & src_offsets,
	PaddedPODArray<T> & res_elems, IColumn::Offsets_t & res_offsets,
	const IColumn::Filter & filt, ssize_t result_size_hint);

}
