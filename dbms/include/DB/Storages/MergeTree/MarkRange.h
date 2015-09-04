#pragma once

#include <cstddef>


namespace DB
{


/** Пара засечек, определяющая диапазон строк в куске. Именно, диапазон имеет вид [begin * index_granularity, end * index_granularity).
  */
struct MarkRange
{
	std::size_t begin;
	std::size_t end;

	MarkRange() = default;
	MarkRange(const std::size_t begin, const std::size_t end) : begin{begin}, end{end} {}
};

using MarkRanges = std::vector<MarkRange>;


}
