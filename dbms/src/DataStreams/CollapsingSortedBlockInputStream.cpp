#include <DB/DataStreams/CollapsingSortedBlockInputStream.h>


namespace DB
{


void CollapsingSortedBlockInputStream::reportIncorrectData()
{
	std::stringstream s;
	s << "Incorrect data: number of rows with sign = 1 (" << count_positive
		<< ") differs with number of rows with sign = -1 (" << count_negative
		<< ") by more than one (for key: ";

	for (size_t i = 0, size = current_key.size(); i < size; ++i)
	{
		if (i != 0)
			s << ", ";
		s << apply_visitor(FieldVisitorToString(), current_key[i]);
	}

	s << ").";

	/** Пока ограничимся всего лишь логгированием таких ситуаций,
	  *  так как данные генерируются внешними программами.
	  */
	LOG_ERROR(log, s.rdbuf());
}


void CollapsingSortedBlockInputStream::insertRows(ColumnPlainPtrs & merged_columns, size_t & merged_rows)
{
	if (count_positive != 0 || count_negative != 0)
	{
		if (count_positive <= count_negative)
		{
			++merged_rows;
			for (size_t i = 0; i < num_columns; ++i)
				merged_columns[i]->insert(first_negative[i]);
		}
			
		if (count_positive >= count_negative)
		{
			++merged_rows;
			for (size_t i = 0; i < num_columns; ++i)
				merged_columns[i]->insert(last_positive[i]);
		}
			
		if (!(count_positive == count_negative || count_positive + 1 == count_negative || count_positive == count_negative + 1))
			reportIncorrectData();
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
		current_key.resize(description.size());
		next_key.resize(description.size());

		sign_column_number = merged_block.getPositionByName(sign_column);
	}

	/// Вынимаем строки в нужном порядке и кладём в merged_block, пока строк не больше max_block_size
	while (!queue.empty())
	{
		SortCursor current = queue.top();
		queue.pop();

		Int8 sign = get<Int64>((*current->all_columns[sign_column_number])[current->pos]);
		setPrimaryKey(next_key, current);

		if (next_key != current_key)
		{
			/// Запишем данные для предыдущего визита.
			insertRows(merged_columns, merged_rows);

			current_key = next_key;
			next_key.resize(description.size());
			
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

		if (merged_rows >= max_block_size)
			return merged_block;
	}

	/// Запишем данные для последнего визита.
	insertRows(merged_columns, merged_rows);

	inputs.clear();
	return merged_block;
}

}
