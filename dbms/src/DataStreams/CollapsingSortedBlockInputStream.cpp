#include <DB/DataStreams/CollapsingSortedBlockInputStream.h>


namespace DB
{


void CollapsingSortedBlockInputStream::insertRows(ColumnPlainPtrs & merged_columns)
{
	if (count_positive != 0 || count_negative != 0)
	{
		if (count_positive == count_negative || count_positive + 1 == count_negative)
			for (size_t i = 0; i < num_columns; ++i)
				merged_columns[i]->insert(first_negative[i]);
			
		if (count_positive == count_negative || count_positive == count_negative + 1)
			for (size_t i = 0; i < num_columns; ++i)
				merged_columns[i]->insert(last_positive[i]);
			
		if (!(count_positive == count_negative || count_positive + 1 == count_negative || count_positive == count_negative + 1))
			throw Exception("Incorrect data: number of rows with sign = 1 ("
				+ Poco::NumberFormatter::format(count_positive) +
				") differs with number of rows with sign = -1 ("
				+ Poco::NumberFormatter::format(count_negative) +
				") by more than one (for id = "
				+ Poco::NumberFormatter::format(current_id) + ").",
				ErrorCodes::INCORRECT_DATA);
	}
}
	

Block CollapsingSortedBlockInputStream::readImpl()
{
	if (!inputs.size())
		return Block();
	
	if (inputs.size() == 1)
		return inputs[0]->read();

	size_t merged_rows = 0;
	Block merged_block;
	ColumnPlainPtrs merged_columns;
	
	init(merged_block, merged_columns);
	if (merged_columns.empty())
		return Block();

	/// Дополнительная инициализация.
	if (first_negative.empty())
	{
		first_negative.resize(num_columns);
		last_positive.resize(num_columns);

		id_column_number = merged_block.getPositionByName(id_column);
		sign_column_number = merged_block.getPositionByName(sign_column);
	}

	/// Вынимаем строки в нужном порядке и кладём в merged_block, пока строк не больше max_block_size
	while (!queue.empty())
	{
		SortCursor current = queue.top();
		queue.pop();

		UInt64 id = boost::get<UInt64>((*current->all_columns[id_column_number])[current->pos]);
		Int8 sign = boost::get<Int64>((*current->all_columns[sign_column_number])[current->pos]);

		if (id != current_id)
		{
			/// Запишем данные для предыдущего визита.
			insertRows(merged_columns);

			current_id = id;
			count_negative = 0;
			count_positive = 0;
		}

		if (sign == 1)
		{
			++count_positive;

			setRow(last_positive, current);
		}
		else if (sign == -1)
		{
			if (!count_negative)
				setRow(first_negative, current);
			
			++count_negative;
		}
		else
			throw Exception("Incorrect data: Sign = " + Poco::NumberFormatter::format(sign) + " (must be 1 or -1).",
				ErrorCodes::INCORRECT_DATA);

		if (!current->isLast())
		{
			current->next();
			queue.push(current);
		}
		else
		{
			/// Достаём из соответствующего источника следующий блок, если есть.
			fetchNextBlock(current);
		}

		++merged_rows;
		if (merged_rows == max_block_size)
			return merged_block;
	}

	/// Запишем данные для последнего визита.
	insertRows(merged_columns);

	inputs.clear();
	return merged_block;
}

}
