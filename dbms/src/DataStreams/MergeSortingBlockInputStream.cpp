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
	if (blocks.empty())
		return Block();

	if (blocks.size() == 1)
		return blocks[0];

	Stopwatch watch;

	LOG_DEBUG(log, "Merge sorting");

	CursorImpls cursors(blocks.size());

	bool has_collation = false;

	size_t nonempty_blocks = 0;
	for (Blocks::const_iterator it = blocks.begin(); it != blocks.end(); ++it)
	{
		if (it->rowsInFirstColumn() == 0)
			continue;

		cursors[nonempty_blocks] = SortCursorImpl(*it, description);
		has_collation |= cursors[nonempty_blocks].has_collation;

		++nonempty_blocks;
	}

	if (nonempty_blocks == 0)
		return Block();

	cursors.resize(nonempty_blocks);

	Block merged;

	if (has_collation)
		merged = mergeImpl<SortCursorWithCollation>(blocks, cursors);
	else
		merged = mergeImpl<SortCursor>(blocks, cursors);

	watch.stop();

	size_t rows_before_merge = 0;
	size_t bytes_before_merge = 0;
	for (const auto & block : blocks)
	{
		rows_before_merge += block.rowsInFirstColumn();
		bytes_before_merge += block.bytes();
	}

	LOG_DEBUG(log, std::fixed << std::setprecision(2)
		<< "Merge sorted " << blocks.size() << " blocks, from " << rows_before_merge << " to " << merged.rows() << " rows"
		<< " in " << watch.elapsedSeconds() << " sec., "
		<< rows_before_merge / watch.elapsedSeconds() << " rows/sec., "
		<< bytes_before_merge / 1048576.0 / watch.elapsedSeconds() << " MiB/sec.");

	return merged;
}

template <typename TSortCursor>
Block MergeSortingBlockInputStream::mergeImpl(Blocks & blocks, CursorImpls & cursors)
{
	Block merged = blocks[0].cloneEmpty();
	size_t num_columns = blocks[0].columns();

	typedef std::priority_queue<TSortCursor> Queue;
	Queue queue;

	for (size_t i = 0; i < cursors.size(); ++i)
		queue.push(TSortCursor(&cursors[i]));

	ColumnPlainPtrs merged_columns;
	for (size_t i = 0; i < num_columns; ++i)	/// TODO: reserve
		merged_columns.push_back(&*merged.getByPosition(i).column);

	/// Вынимаем строки в нужном порядке и кладём в merged.
	for (size_t row = 0; (!limit || row < limit) && !queue.empty(); ++row)
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
	}

	return merged;
}

}
