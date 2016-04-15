#pragma once

#include <DB/Columns/IColumn.h>


/// Общие вспомогательные методы для реализации разных столбцов.

namespace DB
{

/// Считает, сколько байт в filt больше нуля.
size_t countBytesInFilter(const IColumn::Filter & filt);


/// Общая реализация функции filter для ColumnArray и ColumnString.
template <typename T>
void filterArraysImpl(
	const PaddedPODArray<T> & src_elems, const IColumn::Offsets_t & src_offsets,
	PaddedPODArray<T> & res_elems, IColumn::Offsets_t & res_offsets,
	const IColumn::Filter & filt, ssize_t result_size_hint);

}
