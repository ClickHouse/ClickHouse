#include <queue>
#include <iomanip>

#include <DB/DataStreams/MergingSortedBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
	extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
	extern const int BLOCKS_HAS_DIFFERENT_STRUCTURE;
}


MergingSortedBlockInputStream::MergingSortedBlockInputStream(BlockInputStreams & inputs_, const SortDescription & description_,
	size_t max_block_size_, size_t limit_, MergedRowSources * out_row_sources_, bool quiet_)
	: description(description_), max_block_size(max_block_size_), limit(limit_), quiet(quiet_),
	source_blocks(inputs_.size()), cursors(inputs_.size()), out_row_sources(out_row_sources_)
{
	children.insert(children.end(), inputs_.begin(), inputs_.end());
}

String MergingSortedBlockInputStream::getID() const
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

void MergingSortedBlockInputStream::init(Block & merged_block, ColumnPlainPtrs & merged_columns)
{
	/// Читаем первые блоки, инициализируем очередь.
	if (first)
	{
		first = false;

		size_t i = 0;
		for (auto it = source_blocks.begin(); it != source_blocks.end(); ++it, ++i)
		{
			SharedBlockPtr & shared_block_ptr = *it;

			if (shared_block_ptr.get())
				continue;

			shared_block_ptr = new detail::SharedBlock(children[i]->read());

			const size_t rows = shared_block_ptr->rows();

			if (rows == 0)
				continue;

			if (!num_columns)
				num_columns = shared_block_ptr->columns();

			if (expected_block_size < rows)
				expected_block_size = std::min(rows, max_block_size);

			cursors[i] = SortCursorImpl(*shared_block_ptr, description, i);
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
		auto it = source_blocks.cbegin();
		for (; it != source_blocks.cend(); ++it)
		{
			const SharedBlockPtr & shared_block_ptr = *it;

			if (*shared_block_ptr)
			{
				merged_block = shared_block_ptr->cloneEmpty();
				break;
			}
		}

		/// Если все входные блоки пустые.
		if (it == source_blocks.cend())
			return;
	}

	/// Проверим, что у всех блоков-источников одинаковая структура.
	for (auto it = source_blocks.cbegin(); it != source_blocks.cend(); ++it)
	{
		const SharedBlockPtr & shared_block_ptr = *it;

		if (!*shared_block_ptr)
			continue;

		size_t src_columns = shared_block_ptr->columns();
		size_t dst_columns = merged_block.columns();

		if (src_columns != dst_columns)
			throw Exception("Merging blocks has different number of columns ("
				+ toString(src_columns) + " and " + toString(dst_columns) + ")",
				ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);

		for (size_t i = 0; i < src_columns; ++i)
		{
			if (shared_block_ptr->safeGetByPosition(i).name != merged_block.safeGetByPosition(i).name
				|| shared_block_ptr->safeGetByPosition(i).type->getName() != merged_block.safeGetByPosition(i).type->getName()
				|| shared_block_ptr->safeGetByPosition(i).column->getName() != merged_block.safeGetByPosition(i).column->getName())
			{
				throw Exception("Merging blocks has different names or types of columns:\n"
					+ shared_block_ptr->dumpStructure() + "\nand\n" + merged_block.dumpStructure(),
					ErrorCodes::BLOCKS_HAS_DIFFERENT_STRUCTURE);
			}
		}
	}

	for (size_t i = 0; i < num_columns; ++i)
	{
		merged_columns.emplace_back(merged_block.safeGetByPosition(i).column.get());
		merged_columns.back()->reserve(expected_block_size);
	}
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

	/** Увеличить счётчики строк.
	  * Вернуть true, если пора закончить формировать текущий блок данных.
	  */
	auto count_row_and_check_limit = [&, this]()
	{
		++total_merged_rows;
		if (limit && total_merged_rows == limit)
		{
	//		std::cerr << "Limit reached\n";
			cancel();
			finished = true;
			return true;
		}

		++merged_rows;
		if (merged_rows == max_block_size)
		{
	//		std::cerr << "max_block_size reached\n";
			return true;
		}

		return false;
	};

	/// Вынимаем строки в нужном порядке и кладём в merged_block, пока строк не больше max_block_size
	while (!queue.empty())
	{
		TSortCursor current = queue.top();
		queue.pop();

		while (true)
		{
			/** А вдруг для текущего курсора блок целиком меньше или равен, чем остальные?
			  * Или в очереди остался только один источник данных? Тогда можно целиком взять блок текущего курсора.
			  */
			if (current.impl->isFirst() && (queue.empty() || current.totallyLessOrEquals(queue.top())))
			{
	//			std::cerr << "current block is totally less or equals\n";

				/// Если в текущем блоке уже есть данные, то сначала вернём его. Мы попадём сюда снова при следующем вызове функции merge.
				if (merged_rows != 0)
				{
	//				std::cerr << "merged rows is non-zero\n";
					queue.push(current);
					return;
				}

				/// Actually, current.impl->order stores source number (i.e. cursors[current.impl->order] == current.impl)
				size_t source_num = current.impl->order;

				if (source_num >= cursors.size())
					throw Exception("Logical error in MergingSortedBlockInputStream", ErrorCodes::LOGICAL_ERROR);

				for (size_t i = 0; i < num_columns; ++i)
					merged_block.getByPosition(i).column = source_blocks[source_num]->getByPosition(i).column;

	//			std::cerr << "copied columns\n";

				size_t merged_rows = merged_block.rows();

				if (limit && total_merged_rows + merged_rows > limit)
				{
					merged_rows = limit - total_merged_rows;
					for (size_t i = 0; i < num_columns; ++i)
					{
						auto & column = merged_block.getByPosition(i).column;
						column = column->cut(0, merged_rows);
					}

					cancel();
					finished = true;
				}

				if (out_row_sources)
					out_row_sources->resize_fill(out_row_sources->size() + merged_rows, RowSourcePart(source_num));

	//			std::cerr << "fetching next block\n";

				total_merged_rows += merged_rows;
				fetchNextBlock(current, queue);
				return;
			}

	//		std::cerr << "total_merged_rows: " << total_merged_rows << ", merged_rows: " << merged_rows << "\n";
	//		std::cerr << "Inserting row\n";
			for (size_t i = 0; i < num_columns; ++i)
				merged_columns[i]->insertFrom(*current->all_columns[i], current->pos);

			if (out_row_sources)
			{
				/// Actually, current.impl->order stores source number (i.e. cursors[current.impl->order] == current.impl)
				out_row_sources->emplace_back(current.impl->order);
			}

			if (!current->isLast())
			{
	//			std::cerr << "moving to next row\n";
				current->next();

				if (queue.empty() || !(current.greater(queue.top())))
				{
					if (count_row_and_check_limit())
					{
	//					std::cerr << "pushing back to queue\n";
						queue.push(current);
						return;
					}

					/// Не кладём курсор обратно в очередь, а продолжаем работать с текущим курсором.
	//				std::cerr << "current is still on top, using current row\n";
					continue;
				}
				else
				{
	//				std::cerr << "next row is not least, pushing back to queue\n";
					queue.push(current);
				}
			}
			else
			{
				/// Достаём из соответствующего источника следующий блок, если есть.
	//			std::cerr << "It was last row, fetching next block\n";
				fetchNextBlock(current, queue);
			}

			break;
		}

		if (count_row_and_check_limit())
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
			source_blocks[i] = new detail::SharedBlock(children[i]->read());
			if (*source_blocks[i])
			{
				cursors[i].reset(*source_blocks[i]);
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
 	if (quiet)
 		return;

	const BlockStreamProfileInfo & profile_info = getProfileInfo();
	double seconds = profile_info.total_stopwatch.elapsedSeconds();
	LOG_DEBUG(log, std::fixed << std::setprecision(2)
		<< "Merge sorted " << profile_info.blocks << " blocks, " << profile_info.rows << " rows"
		<< " in " << seconds << " sec., "
		<< profile_info.rows / seconds << " rows/sec., "
		<< profile_info.bytes / 1000000.0 / seconds << " MB/sec.");
}

}
