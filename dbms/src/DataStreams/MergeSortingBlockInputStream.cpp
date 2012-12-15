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

	return merge(blocks);
}


Block MergeSortingBlockInputStream::merge(Blocks & blocks)
{
	Stopwatch watch;
	Block merged;

	if (blocks.empty())
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

	ColumnPlainPtrs merged_columns(num_columns);
	for (size_t i = 0; i < num_columns; ++i)
	{
		merged_columns[i] = &*merged.getByPosition(i).column;

		size_t total_rows = 0;
		size_t total_bytes = 0;
		for (size_t j = 0; j < blocks.size(); ++j)
		{
			total_rows += blocks[j].getByPosition(i).column->size();
			total_bytes += blocks[j].getByPosition(i).column->byteSize();
		}

		merged_columns[i]->reserve(total_rows, total_bytes);
	}

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

}
