#pragma once

#include <DB/Core/ColumnNumbers.h>

#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{

/** Сортирует каждый блок по отдельности по значениям указанных столбцов.
  * На данный момент, используется сильно неоптимальный алгоритм.
  */
class PartialSortingBlockInputStream : public IBlockInputStream
{
public:
	PartialSortingBlockInputStream(BlockInputStreamPtr input_, ColumnNumbers & column_numbers_)
		: input(input_), column_numbers(column_numbers_) {}

	Block read();

private:
	BlockInputStreamPtr input;
	ColumnNumbers column_numbers;
};

}
