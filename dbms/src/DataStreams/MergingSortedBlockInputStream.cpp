#include <queue>

#include <DB/DataStreams/MergingSortedBlockInputStream.h>


namespace DB
{

Block MergingSortedBlockInputStream::readImpl()
{
	if (!inputs.size())
		return Block();
	
	if (inputs.size() == 1)
		return inputs[0]->read();

	/// Читаем первые блоки, инициализируем очередь.
	if (first)
	{
		first = false;
		size_t i = 0;
		for (Blocks::iterator it = source_blocks.begin(); it != source_blocks.end(); ++it, ++i)
		{
			if (*it)
				continue;

			*it = inputs[i]->read();

			if (!*it)
				continue;

			if (!num_columns)
				num_columns = source_blocks[0].columns();

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
	}

	/// Инициализируем результат.
	size_t merged_rows = 0;
	Block merged_block;
	ColumnPlainPtrs merged_columns;

	/// Клонируем структуру первого непустого блока источников.
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
		return Block();
		
	for (size_t i = 0; i < num_columns; ++i)
		merged_columns.push_back(&*merged_block.getByPosition(i).column);

	/// Вынимаем строки в нужном порядке и кладём в merged_block, пока строк не больше max_block_size
	while (!queue.empty())
	{
		SortCursor current = queue.top();
		queue.pop();

		for (size_t i = 0; i < num_columns; ++i)
			merged_columns[i]->insert((*(*current.all_columns)[i])[current.pos]);

		if (!current.isLast())
			queue.push(current.next());
		else
		{
			/// Достаём из соответствующего источника следующий блок, если есть.
			/// Источник, соответствующий этому курсору, ищем с помощью небольшого хака (сравнивая адреса all_columns).

			size_t i = 0;
			size_t size = all_columns.size();
			for (; i < size; ++i)
			{
				if (&all_columns[i] == current.all_columns)
				{
					source_blocks[i] = inputs[i]->read();
					if (source_blocks[i])
					{
						all_columns[i].clear();
						sort_columns[i].clear();
						
						for (size_t j = 0; j < num_columns; ++j)
							all_columns[i].push_back(&*source_blocks[i].getByPosition(j).column);

						for (size_t j = 0, size = description.size(); j < size; ++j)
						{
							size_t column_number = !description[j].column_name.empty()
								? source_blocks[i].getPositionByName(description[j].column_name)
								: description[j].column_number;

							sort_columns[i].push_back(&*source_blocks[i].getByPosition(column_number).column);
						}

						queue.push(SortCursor(&all_columns[i], &sort_columns[i], &description));
					}

					break;
				}
			}

			if (i == size)
				throw Exception("Logical error in MergingSortedBlockInputStream", ErrorCodes::LOGICAL_ERROR);
		}

		++merged_rows;
		if (merged_rows == max_block_size)
			return merged_block;
	}

	inputs.clear();
	return merged_block;
}

}
