#include <DB/DataStreams/SummingSortedBlockInputStream.h>


namespace DB
{


void SummingSortedBlockInputStream::insertCurrentRow(ColumnPlainPtrs & merged_columns)
{
	for (size_t i = 0; i < num_columns; ++i)
		merged_columns[i]->insert(current_row[i]);
}


Block SummingSortedBlockInputStream::readImpl()
{
	if (!children.size())
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

		/** Заполним номера столбцов, которые должны быть просуммированы.
		  * Это могут быть только числовые столбцы, не входящие в ключ сортировки.
		  * Если задан непустой список column_names_to_sum, то берём только эти столбцы.
		  * Часть столбцов из column_names_to_sum может быть не найдена. Это игнорируется.
		  */
		for (size_t i = 0; i < num_columns; ++i)
		{
			ColumnWithNameAndType & column = merged_block.getByPosition(i);

			/// Оставляем только числовые типы. При чём, даты и даты-со-временем здесь такими не считаются.
			if (!column.type->isNumeric() || column.type->getName() == "Date" || column.type->getName() == "DateTime")
				continue;

			/// Входят ли в PK?
			SortDescription::const_iterator it = description.begin();
			for (; it != description.end(); ++it)
				if (it->column_name == column.name || (it->column_name.empty() && it->column_number == i))
					break;

			if (it != description.end())
				continue;

			if (column_names_to_sum.empty()
				|| column_names_to_sum.end() != std::find(column_names_to_sum.begin(), column_names_to_sum.end(), column.name))
			{
				column_numbers_to_sum.push_back(i);
			}
		}
	}

	if (has_collation)
		merge(merged_block, merged_columns, queue_with_collation);
	else
		merge(merged_block, merged_columns, queue);

	return merged_block;
}


template<class TSortCursor>
void SummingSortedBlockInputStream::merge(Block & merged_block, ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue)
{
	size_t merged_rows = 0;

	/// Вынимаем строки в нужном порядке и кладём в merged_block, пока строк не больше max_block_size
	while (!queue.empty())
	{
		TSortCursor current = queue.top();
		queue.pop();

		setPrimaryKey(next_key, current);

		if (next_key != current_key)
		{
			/// Запишем данные для предыдущей группы.
			if (!current_key[0].isNull() && !current_row_is_zero)
			{
				++merged_rows;
				output_is_non_empty = true;
				insertCurrentRow(merged_columns);
			}

			current_key = std::move(next_key);
			next_key.resize(description.size());

			setRow(current_row, current);
			current_row_is_zero = false;
		}
		else
		{
			current_row_is_zero = !addRow(current_row, current);
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

		if (merged_rows >= max_block_size)
			return;
	}

	/// Запишем данные для последней группы, если она ненулевая.
	/// Если она нулевая, и без нее выходной поток окажется пустым, запишем ее все равно.
	if (!current_row_is_zero || !output_is_non_empty)
	{
		++merged_rows;
		insertCurrentRow(merged_columns);
	}

	children.clear();
}

}
