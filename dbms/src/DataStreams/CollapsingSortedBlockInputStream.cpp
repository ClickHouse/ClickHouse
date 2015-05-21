#include <DB/DataStreams/CollapsingSortedBlockInputStream.h>

/// Максимальное количество сообщений о некорректных данных в логе.
#define MAX_ERROR_MESSAGES 10


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
	  * При неконсистентных данных, это - неизбежная ошибка, которая не может быть легко исправлена админами. Поэтому Warning.
	  */
	LOG_WARNING(log, s.rdbuf());
}


void CollapsingSortedBlockInputStream::insertRows(ColumnPlainPtrs & merged_columns, size_t & merged_rows, bool last_in_stream)
{
	if (count_positive != 0 || count_negative != 0)
	{
		if (count_positive == count_negative && !last_is_positive)
		{
			/// Если все строки во входных потоках схлопнулись, мы все равно хотим выдать хоть один блок в результат.
			if (last_in_stream && merged_rows == 0 && !blocks_written)
			{
				LOG_INFO(log, "All rows collapsed");
				++merged_rows;
				for (size_t i = 0; i < num_columns; ++i)
					merged_columns[i]->insert(last_positive[i]);
				++merged_rows;
				for (size_t i = 0; i < num_columns; ++i)
					merged_columns[i]->insert(last_negative[i]);
			}
			return;
		}

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
		{
			if (count_incorrect_data < MAX_ERROR_MESSAGES)
				reportIncorrectData();
			++count_incorrect_data;
		}
	}
}


Block CollapsingSortedBlockInputStream::readImpl()
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
	if (first_negative.empty())
	{
		first_negative.resize(num_columns);
		last_negative.resize(num_columns);
		last_positive.resize(num_columns);
		current_key.resize(description.size());
		next_key.resize(description.size());

		sign_column_number = merged_block.getPositionByName(sign_column);
	}

	if (has_collation)
		merge(merged_columns, queue_with_collation);
	else
		merge(merged_columns, queue);

	return merged_block;
}

template<class TSortCursor>
void CollapsingSortedBlockInputStream::merge(ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue)
{
	size_t merged_rows = 0;

	/// Вынимаем строки в нужном порядке и кладём в merged_block, пока строк не больше max_block_size
	while (!queue.empty())
	{
		TSortCursor current = queue.top();

		Int8 sign = get<Int64>((*current->all_columns[sign_column_number])[current->pos]);
		setPrimaryKey(next_key, current);

		bool key_differs = next_key != current_key;

		/// если накопилось достаточно строк и последняя посчитана полностью
		if (key_differs && merged_rows >= max_block_size)
		{
			++blocks_written;
			return;
		}

		queue.pop();

		if (key_differs)
		{
			/// Запишем данные для предыдущего визита.
			insertRows(merged_columns, merged_rows);

			current_key = std::move(next_key);
			next_key.resize(description.size());

			count_negative = 0;
			count_positive = 0;
		}

		if (sign == 1)
		{
			++count_positive;
			last_is_positive = true;

			setRow(last_positive, current);
		}
		else if (sign == -1)
		{
			if (!count_negative)
				setRow(first_negative, current);
			if (!blocks_written && !merged_rows)
				setRow(last_negative, current);

			++count_negative;
			last_is_positive = false;
		}
		else
			throw Exception("Incorrect data: Sign = " + toString(sign) + " (must be 1 or -1).",
				ErrorCodes::INCORRECT_DATA);

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

	/// Запишем данные для последнего визита.
	insertRows(merged_columns, merged_rows, true);

	finished = true;
}

}
