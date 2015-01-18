#include <queue>
#include <iomanip>

#include <DB/DataStreams/MergingSortedBlockInputStream.h>


namespace DB
{


void MergingSortedBlockInputStream::init(Block & merged_block, ColumnPlainPtrs & merged_columns)
{
	/// Читаем первые блоки, инициализируем очередь.
	if (first)
	{
		first = false;

		/// Например, если дети - AsynchronousBlockInputStream, это действие инициирует начало работы в фоновых потоках.
		for (auto & child : children)
			child->readPrefix();

		size_t i = 0;
		for (Blocks::iterator it = source_blocks.begin(); it != source_blocks.end(); ++it, ++i)
		{
			if (*it)
				continue;

			*it = children[i]->read();

			if (it->rowsInFirstColumn() == 0)
				continue;

			if (!num_columns)
				num_columns = it->columns();

			cursors[i] = SortCursorImpl(*it, description, i);
			has_collation |= cursors[i].has_collation;
		}

		if (has_collation)
			initQueue(queue_with_collation);
		else
			initQueue(queue);
	}

	/// Инициализируем результат.

	/// Клонируем структуру первого непустого блока источников.
	{
		Blocks::const_iterator it = source_blocks.begin();
		for (; it != source_blocks.end(); ++it)
		{
			if (*it)
			{
				merged_block = it->cloneEmpty();
				break;
			}
		}

		/// Если все входные блоки пустые.
		if (it == source_blocks.end())
			return;
	}

	/// Проверим, что у всех блоков-источников одинаковая структура.
	for (Blocks::const_iterator it = source_blocks.begin(); it != source_blocks.end(); ++it)
	{
		if (!*it)
			continue;

		size_t src_columns = it->columns();
		size_t dst_columns = merged_block.columns();

		if (src_columns != dst_columns)
			throw Exception("Merging blocks has different number of columns", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);

		for (size_t i = 0; i < src_columns; ++i)
			if (it->getByPosition(i).name != merged_block.getByPosition(i).name
				|| it->getByPosition(i).type->getName() != merged_block.getByPosition(i).type->getName()
				|| it->getByPosition(i).column->getName() != merged_block.getByPosition(i).column->getName())
				throw Exception("Merging blocks has different names or types of columns", ErrorCodes::BLOCKS_HAS_DIFFERENT_STRUCTURE);
	}

	for (size_t i = 0; i < num_columns; ++i)
		merged_columns.push_back(&*merged_block.getByPosition(i).column);
}


template <typename TSortCursor>
void MergingSortedBlockInputStream::initQueue(std::priority_queue<TSortCursor> & queue)
{
	for (size_t i = 0; i < cursors.size(); ++i)
		if (!cursors[i].empty())
			queue.push(TSortCursor(&cursors[i]));
}


Block MergingSortedBlockInputStream::readImpl()
{
	if (finished)
		return Block();

	if (children.size() == 1)
		return children[0]->read();

	Block merged_block;
	ColumnPlainPtrs merged_columns;

	init(merged_block, merged_columns);
	if (merged_columns.empty())
		return Block();

	if (has_collation)
		merge(merged_block, merged_columns, queue_with_collation);
	else
		merge(merged_block, merged_columns, queue);

	return merged_block;
}

template <typename TSortCursor>
void MergingSortedBlockInputStream::merge(Block & merged_block, ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue)
{
	size_t merged_rows = 0;

	/// Вынимаем строки в нужном порядке и кладём в merged_block, пока строк не больше max_block_size
	while (!queue.empty())
	{
		TSortCursor current = queue.top();
		queue.pop();

		for (size_t i = 0; i < num_columns; ++i)
			merged_columns[i]->insertFrom(*current->all_columns[i], current->pos);

		if (!current->isLast())
		{
			current->next();
			queue.push(current);
		}
		else
		{
			/// Достаём из соответствующего источника следующий блок, если есть.
			fetchNextBlock(current, queue);
		}

		++total_merged_rows;
		if (limit && total_merged_rows == limit)
		{
			cancel();
			finished = true;
			return;
		}

		++merged_rows;
		if (merged_rows == max_block_size)
			return;
	}

	cancel();
	finished = true;
}


template <typename TSortCursor>
void MergingSortedBlockInputStream::fetchNextBlock(const TSortCursor & current, std::priority_queue<TSortCursor> & queue)
{
	size_t i = 0;
	size_t size = cursors.size();
	for (; i < size; ++i)
	{
		if (&cursors[i] == current.impl)
		{
			source_blocks[i] = children[i]->read();
			if (source_blocks[i])
			{
				cursors[i].reset(source_blocks[i]);
				queue.push(TSortCursor(&cursors[i]));
			}

			break;
		}
	}

	if (i == size)
		throw Exception("Logical error in MergingSortedBlockInputStream", ErrorCodes::LOGICAL_ERROR);
}


void MergingSortedBlockInputStream::readSuffixImpl()
{
	const BlockStreamProfileInfo & profile_info = getInfo();
	double seconds = profile_info.total_stopwatch.elapsedSeconds();
	LOG_DEBUG(log, std::fixed << std::setprecision(2)
		<< "Merge sorted " << profile_info.blocks << " blocks, " << profile_info.rows << " rows"
		<< " in " << seconds << " sec., "
		<< profile_info.rows / seconds << " rows/sec., "
		<< profile_info.bytes / 1000000.0 / seconds << " MiB/sec.");
}

}
