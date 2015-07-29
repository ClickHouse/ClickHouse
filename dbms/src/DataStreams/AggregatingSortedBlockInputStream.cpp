#include <DB/DataStreams/AggregatingSortedBlockInputStream.h>


namespace DB
{


void AggregatingSortedBlockInputStream::insertCurrentRow(ColumnPlainPtrs & merged_columns)
{
	for (size_t i = 0; i < num_columns; ++i)
		merged_columns[i]->insert(current_row[i]);
}


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
	if (current_row.empty())
	{
		current_row.resize(num_columns);
		current_key.resize(description.size());
		next_key.resize(description.size());

		/// Заполним номера столбцов, которые нужно доагрегировать.
		for (size_t i = 0; i < num_columns; ++i)
		{
			ColumnWithTypeAndName & column = merged_block.getByPosition(i);

			/// Оставляем только состояния аггрегатных функций.
			if (strncmp(column.type->getName().data(), "AggregateFunction", strlen("AggregateFunction")) != 0)
				continue;

			/// Входят ли в PK?
			SortDescription::const_iterator it = description.begin();
			for (; it != description.end(); ++it)
				if (it->column_name == column.name || (it->column_name.empty() && it->column_number == i))
					break;

			if (it != description.end())
				continue;

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

		setPrimaryKey(next_key, current);

		bool key_differs = next_key != current_key;

		/// если накопилось достаточно строк и последняя посчитана полностью
		if (key_differs && merged_rows >= max_block_size)
			return;

		queue.pop();

		if (key_differs)
		{
			current_key = std::move(next_key);
			next_key.resize(description.size());

			/// Запишем данные для очередной группы.
			setRow(current_row, current);
			insertCurrentRow(merged_columns);
			++merged_rows;
		}
		else
		{
			addRow(current);
		}

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

}
