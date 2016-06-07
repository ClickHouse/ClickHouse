#include <DB/DataStreams/AggregatingSortedBlockInputStream.h>


namespace DB
{


Block AggregatingSortedBlockInputStream::readImpl()
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

	/// Дополнительная инициализация.
	if (next_key.empty())
	{
		next_key.columns.resize(description.size());

		/// Заполним номера столбцов, которые нужно доагрегировать.
		for (size_t i = 0; i < num_columns; ++i)
		{
			ColumnWithTypeAndName & column = merged_block.getByPosition(i);

			/// Оставляем только состояния аггрегатных функций.
			if (strncmp(column.type->getName().data(), "AggregateFunction", strlen("AggregateFunction")) != 0)
			{
				column_numbers_not_to_aggregate.push_back(i);
				continue;
			}

			/// Входят ли в PK?
			SortDescription::const_iterator it = description.begin();
			for (; it != description.end(); ++it)
				if (it->column_name == column.name || (it->column_name.empty() && it->column_number == i))
					break;

			if (it != description.end())
			{
				column_numbers_not_to_aggregate.push_back(i);
				continue;
			}

			column_numbers_to_aggregate.push_back(i);
		}
	}

	columns_to_aggregate.resize(column_numbers_to_aggregate.size());
	for (size_t i = 0, size = columns_to_aggregate.size(); i < size; ++i)
		columns_to_aggregate[i] = typeid_cast<ColumnAggregateFunction *>(merged_columns[column_numbers_to_aggregate[i]]);

	if (has_collation)
		merge(merged_columns, queue_with_collation);
	else
		merge(merged_columns, queue);

	return merged_block;
}


template<class TSortCursor>
void AggregatingSortedBlockInputStream::merge(ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue)
{
	size_t merged_rows = 0;

	/// Вынимаем строки в нужном порядке и кладём в merged_block, пока строк не больше max_block_size
	while (!queue.empty())
	{
		TSortCursor current = queue.top();

		setPrimaryKeyRef(next_key, current);

		bool key_differs;

		if (current_key.empty())	/// Первый встретившийся ключ.
		{
			current_key.columns.resize(description.size());
			setPrimaryKeyRef(current_key, current);
			key_differs = true;
		}
		else
			key_differs = next_key != current_key;

		/// если накопилось достаточно строк и последняя посчитана полностью
		if (key_differs && merged_rows >= max_block_size)
			return;

		queue.pop();

		if (key_differs)
		{
			current_key.swap(next_key);

			/// Запишем данные для очередной группы. Копируем значения обычных столбцов.
			for (size_t i = 0, size = column_numbers_not_to_aggregate.size(); i < size; ++i)
			{
				size_t j = column_numbers_not_to_aggregate[i];
				merged_columns[j]->insertFrom(*current->all_columns[j], current->pos);
			}

			/// Добавляем в агрегатные столбцы пустое состояние агрегации. Состояние будет обновлено в функции addRow.
			for (auto & column_to_aggregate : columns_to_aggregate)
				column_to_aggregate->insertDefault();

			++merged_rows;
		}

		addRow(current);

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
	}

	finished = true;
}


template <class TSortCursor>
void AggregatingSortedBlockInputStream::addRow(TSortCursor & cursor)
{
	for (size_t i = 0, size = column_numbers_to_aggregate.size(); i < size; ++i)
	{
		size_t j = column_numbers_to_aggregate[i];
		columns_to_aggregate[i]->insertMergeFrom(*cursor->all_columns[j], cursor->pos);
	}
}

}
