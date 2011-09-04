#pragma once

#include <DB/Core/SortDescription.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** Соединяет поток сортированных по отдельности блоков в сортированный целиком поток.
  */
class MergeSortingBlockInputStream : public IProfilingBlockInputStream
{
public:
	MergeSortingBlockInputStream(BlockInputStreamPtr input_, SortDescription & description_)
		: input(input_), description(description_)
	{
		children.push_back(input);
	}

	Block readImpl();

	String getName() const { return "MergeSortingBlockInputStream"; }

private:
	BlockInputStreamPtr input;
	SortDescription description;

	void merge(Block & left, Block & right);
};

}
