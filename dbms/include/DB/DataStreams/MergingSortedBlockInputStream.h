#pragma once

#include <queue>

#include <DB/Core/SortDescription.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** Соединяет несколько сортированных потоков в один.
  */
class MergingSortedBlockInputStream : public IProfilingBlockInputStream
{
public:
	MergingSortedBlockInputStream(BlockInputStreams inputs_, SortDescription & description_, size_t max_block_size_)
		: inputs(inputs_), description(description_), max_block_size(max_block_size_), first(true),
		num_columns(0), source_blocks(inputs.size()), cursors(inputs.size()), log(&Logger::get("MergingSortedBlockInputStream"))
	{
		children.insert(children.end(), inputs.begin(), inputs.end());
	}

	Block readImpl();

	String getName() const { return "MergingSortedBlockInputStream"; }

	BlockInputStreamPtr clone() { return new MergingSortedBlockInputStream(inputs, description, max_block_size); }

private:
	BlockInputStreams inputs;
	SortDescription description;
	size_t max_block_size;

	bool first;

	/// Текущие сливаемые блоки.
	size_t num_columns;
	Blocks source_blocks;
	
	typedef std::vector<SortCursorImpl> CursorImpls;
	CursorImpls cursors;

	typedef std::priority_queue<SortCursor> Queue;
	Queue queue;

	Logger * log;
};

}
