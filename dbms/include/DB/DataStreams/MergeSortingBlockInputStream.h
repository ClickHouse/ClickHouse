#pragma once

#include <DB/Core/SortDescription.h>

#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{

/** Соединяет поток сортированных по отдельности блоков в сортированный целиком поток.
  */
class MergeSortingBlockInputStream : public IBlockInputStream
{
public:
	MergeSortingBlockInputStream(BlockInputStreamPtr input_, SortDescription & description_)
		: input(input_), description(description_) {}

	Block read();

private:
	BlockInputStreamPtr input;
	SortDescription description;

	void merge(Block & left, Block & right);
};

}
