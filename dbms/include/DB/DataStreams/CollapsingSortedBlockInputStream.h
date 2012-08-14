#pragma once

#include <DB/DataStreams/MergingSortedBlockInputStream.h>


namespace DB
{

/** Соединяет несколько сортированных потоков в один.
  * При этом, для каждого значения столбца id_column,
  *  оставляет первую строку со значением столбца sign_column = -1
  *  и последнюю строку со значением столбца sign_column = 1
  */
class CollapsingSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
	CollapsingSortedBlockInputStream(BlockInputStreams inputs_, SortDescription & description_, size_t max_block_size_)
		: inputs(inputs_), description(description_), max_block_size(max_block_size_), first(true),
		num_columns(0), source_blocks(inputs.size()), cursors(inputs.size()), log(&Logger::get("CollapsingSortedBlockInputStream"))
	{
		children.insert(children.end(), inputs.begin(), inputs.end());
	}

	Block readImpl();

	String getName() const { return "CollapsingSortedBlockInputStream"; }

	BlockInputStreamPtr clone() { return new CollapsingSortedBlockInputStream(inputs, description, max_block_size); }

private:
	SortCursor first_negative;
	SortCursor last_positive;

	Logger * log;
};

}
