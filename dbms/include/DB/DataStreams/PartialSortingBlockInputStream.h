#pragma once

#include <DB/Core/SortDescription.h>

#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{

/** Сортирует каждый блок по отдельности по значениям указанных столбцов.
  * На данный момент, используется не очень оптимальный алгоритм.
  */
class PartialSortingBlockInputStream : public IBlockInputStream
{
public:
	PartialSortingBlockInputStream(BlockInputStreamPtr input_, SortDescription & description_)
		: input(input_), description(description_) {}

	Block read();

private:
	BlockInputStreamPtr input;
	SortDescription description;
};

}
