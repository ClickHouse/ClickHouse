#include <queue>
#include <iomanip>

#include <statdaemons/Stopwatch.h>

#include <DB/DataStreams/MergeSortingBlockInputStream.h>


namespace DB
{

Block MergeSortingBlockInputStream::readImpl()
{
	/** Достаточно простой алгоритм:
	  * - прочитать в оперативку все блоки;
	  * - объединить их всех;
	  */

	if (has_been_read)
		return Block();
	
	has_been_read = true;

	Blocks blocks;
	while (Block block = input->read())
		blocks.push_back(block);

	if (blocks.empty())
		return Block();
	else if (blocks.size() == 1)
		return blocks[0];
	else if (blocks.size() == 2)
	{
		merge(blocks[0], blocks[1]);
		return blocks[0];
	}
	else
		return merge(blocks);
}


Block MergeSortingBlockInputStream::merge(Blocks & blocks)
{
	Stopwatch watch;
	Block merged;

	if (!blocks.size())
		return merged;

	if (blocks.size() == 1)
		return blocks[0];

	LOG_DEBUG(log, "Merge sorting");

	merged = blocks[0].cloneEmpty();

	typedef std::priority_queue<SortCursor> Queue;
	Queue queue;

	typedef std::vector<SortCursorImpl> CursorImpls;
	CursorImpls cursors(blocks.size());

	size_t i = 0;
	size_t num_columns = blocks[0].columns();
	for (Blocks::const_iterator it = blocks.begin(); it != blocks.end(); ++it, ++i)
	{
		if (!*it)
			continue;

		cursors[i] = SortCursorImpl(*it, description);
		queue.push(SortCursor(&cursors[i]));
	}

	ColumnPlainPtrs merged_columns;
	for (size_t i = 0; i < num_columns; ++i)
		merged_columns.push_back(&*merged.getByPosition(i).column);

	/// Вынимаем строки в нужном порядке и кладём в merged.
	while (!queue.empty())
	{
		SortCursor current = queue.top();
		queue.pop();

		for (size_t i = 0; i < num_columns; ++i)
			merged_columns[i]->insert((*current->all_columns[i])[current->pos]);

		if (!current->isLast())
		{
			current->next();
			queue.push(current);
		}
	}

	LOG_DEBUG(log, std::fixed << std::setprecision(2)
		<< "Merge sorted " << blocks.size() << " blocks, " << merged.rows() << " rows"
		<< " in " << watch.elapsedSeconds() << " sec., "
		<< merged.rows() / watch.elapsedSeconds() << " rows/sec., "
		<< merged.bytes() / 1000000.0 / watch.elapsedSeconds() << " MiB/sec.");

	return merged;
}


void MergeSortingBlockInputStream::merge(Block & left, Block & right)
{
	Block merged = left.cloneEmpty();

	size_t left_size = left.rows();
	size_t right_size = right.rows();

	size_t left_pos = 0;
	size_t right_pos = 0;

	/// Все столбцы блоков.
	ConstColumnPlainPtrs left_columns;
	ConstColumnPlainPtrs right_columns;
	ColumnPlainPtrs merged_columns;

	/// Столбцы, по которым идёт сортировка.
	ConstColumnPlainPtrs left_sort_columns;
	ConstColumnPlainPtrs right_sort_columns;

	size_t num_columns = left.columns();
	for (size_t i = 0; i < num_columns; ++i)
	{
		left_columns.push_back(&*left.getByPosition(i).column);
		right_columns.push_back(&*right.getByPosition(i).column);
		merged_columns.push_back(&*merged.getByPosition(i).column);
	}

	for (size_t i = 0, size = description.size(); i < size; ++i)
	{
		size_t column_number = !description[i].column_name.empty()
			? left.getPositionByName(description[i].column_name)
			: description[i].column_number;
		left_sort_columns.push_back(&*left.getByPosition(column_number).column);
		right_sort_columns.push_back(&*right.getByPosition(column_number).column);
	}

	/// Объединяем.
	while (right_pos < right_size || left_pos < left_size)
	{
		/// Откуда брать строку - из левого или из правого блока?
		int res = 0;
		if (right_pos == right_size)
			res = -1;
		else if (left_pos == left_size)
			res = 1;
		else
			for (size_t i = 0, size = description.size(); i < size; ++i)
				if ((res = description[i].direction * left_sort_columns[i]->compareAt(left_pos, right_pos, *right_sort_columns[i])))
					break;

		/// Вставляем строку в объединённый блок.
		if (res <= 0)
		{
			for (size_t i = 0; i < num_columns; ++i)
				merged_columns[i]->insert((*left_columns[i])[left_pos]);
			++left_pos;
		}
		else
		{
			for (size_t i = 0; i < num_columns; ++i)
				merged_columns[i]->insert((*right_columns[i])[right_pos]);
			++right_pos;
		}
	}

	left = merged;
}

}
