#pragma once

#include <queue>

#include <Yandex/logger_useful.h>

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
		: description(description_), max_block_size(max_block_size_), first(true),
		num_columns(0), source_blocks(inputs_.size()), cursors(inputs_.size()), log(&Logger::get("MergingSortedBlockInputStream"))
	{
		children.insert(children.end(), inputs_.begin(), inputs_.end());
	}

	void readSuffix();

	String getName() const { return "MergingSortedBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "MergingSorted(";

		Strings children_ids(children.size());
		for (size_t i = 0; i < children.size(); ++i)
			children_ids[i] = children[i]->getID();

		/// Порядок не имеет значения.
		std::sort(children_ids.begin(), children_ids.end());

		for (size_t i = 0; i < children_ids.size(); ++i)
			res << (i == 0 ? "" : ", ") << children_ids[i];

		for (size_t i = 0; i < description.size(); ++i)
			res << ", " << description[i].getID();

		res << ")";
		return res.str();
	}

protected:
	Block readImpl();
	
	/// Инициализирует очередь и следующий блок результата.
	void init(Block & merged_block, ColumnPlainPtrs & merged_columns);
	
	/// Достаёт из источника, соответствующего current следующий блок.
	void fetchNextBlock(const SortCursor & current);
	
	
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

private:
	Logger * log;
};

}
