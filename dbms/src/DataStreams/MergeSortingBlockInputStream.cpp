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
	while (Block block = children.back()->read())
		blocks.push_back(block);

	if (isCancelled())
		return Block();

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

	ColumnPlainPtrs merged_columns;
	for (size_t i = 0; i < num_columns; ++i)
		merged_columns.push_back(&*merged.getByPosition(i).column);

	/// Вынимаем строки в нужном порядке и кладём в merged.
	while (!queue.empty())
	{
		SortCursor current = queue.top();
		queue.pop();

		for (size_t i = 0; i < num_columns; ++i)
			merged_columns[i]->insertFrom(*current->all_columns[i], current->pos);

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
