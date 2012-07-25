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
	LOG_DEBUG(log, "Merge sorting");
		
	Block merged;

	if (!blocks.size())
		return merged;

	if (blocks.size() == 1)
		return blocks[0];

	merged = blocks[0].cloneEmpty();

	typedef std::priority_queue<SortCursor> Queue;
	Queue queue;

	typedef std::vector<ConstColumnPlainPtrs> ConstColumnPlainPtrsForBlocks;
	ConstColumnPlainPtrsForBlocks all_columns(blocks.size());
	ConstColumnPlainPtrsForBlocks sort_columns(blocks.size());

	size_t i = 0;
	size_t num_columns = blocks[0].columns();
	for (Blocks::const_iterator it = blocks.begin(); it != blocks.end(); ++it, ++i)
	{
		if (!*it)
			continue;

		for (size_t j = 0; j < num_columns; ++j)
			all_columns[i].push_back(&*it->getByPosition(j).column);

		for (size_t j = 0, size = description.size(); j < size; ++j)
		{
			size_t column_number = !description[j].column_name.empty()
				? it->getPositionByName(description[j].column_name)
				: description[j].column_number;

			sort_columns[i].push_back(&*it->getByPosition(column_number).column);
		}

		queue.push(SortCursor(&all_columns[i], &sort_columns[i], &description));
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
			merged_columns[i]->insert((*(*current.all_columns)[i])[current.pos]);

		if (!current.isLast())
			queue.push(current.next());
	}

	LOG_DEBUG(log, std::fixed << std::setprecision(2)
		<< "Merge sorted " << blocks.size() << " blocks, " << merged.rows() << " rows"
		<< " in " << watch.elapsedSeconds() << " sec., "
		<< merged.rows() / watch.elapsedSeconds() << " rows/sec., "
		<< merged.bytes() / 1000000.0 / watch.elapsedSeconds() << " MiB/sec.");

	return merged;
}

}
