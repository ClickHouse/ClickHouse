#include <DB/DataStreams/GraphiteRollupSortedBlockInputStream.h>
#include <type_traits>


namespace DB
{

namespace ErrorCodes
{
	extern const int NO_SUCH_COLUMN_IN_TABLE;
}


const Graphite::Pattern * GraphiteRollupSortedBlockInputStream::selectPatternForPath(StringRef path) const
{
	for (const auto & pattern : params.patterns)
		if (!pattern.regexp || pattern.regexp->match(path.data, path.size))
			return &pattern;

	return nullptr;
}


UInt32 GraphiteRollupSortedBlockInputStream::selectPrecision(const Graphite::Retentions & retentions, time_t time) const
{
	static_assert(std::is_signed<time_t>::value, "time_t must be signed type");

	for (const auto & retention : retentions)
	{
		if (time_of_merge - time >= static_cast<time_t>(retention.age))
			return retention.precision;
	}

	/// No rounding.
	return 1;
}


/** Округлить unix timestamp до precision секунд.
  * При этом, дата не должна измениться. Дата исчисляется с помощью локального часового пояса.
  *
  * Если величина округления не больше часа,
  *  то, исходя из допущения, что часовые пояса, отличающиеся от UTC на нецелое количество часов не поддерживаются,
  *  достаточно просто округлить unix timestamp вниз до числа, кратного 3600.
  * А если величина округления больше,
  *  то будем подвергать округлению число секунд от начала суток в локальном часовом поясе.
  *
  * Округление более чем до суток не поддерживается.
  */
static time_t roundTimeToPrecision(const DateLUTImpl & date_lut, time_t time, UInt32 precision)
{
	if (precision <= 3600)
	{
		return time / precision * precision;
	}
	else
	{
		time_t date = date_lut.toDate(time);
		time_t remainder = time - date;
		return date + remainder / precision * precision;
	}
}


Block GraphiteRollupSortedBlockInputStream::readImpl()
{
	if (finished)
		return Block();

	Block merged_block;
	ColumnPlainPtrs merged_columns;

	init(merged_block, merged_columns);
	if (merged_columns.empty())
		return Block();

	/// Additional initialization.
	if (!current_path.data)
	{
		size_t max_size_of_aggregate_state = 0;
		for (const auto & pattern : params.patterns)
			if (pattern.function->sizeOfData() > max_size_of_aggregate_state)
				max_size_of_aggregate_state = pattern.function->sizeOfData();

		place_for_aggregate_state.resize(max_size_of_aggregate_state);

		/// Memoize column numbers in block.
		path_column_num = merged_block.getPositionByName(params.path_column_name);
		time_column_num = merged_block.getPositionByName(params.time_column_name);
		value_column_num = merged_block.getPositionByName(params.value_column_name);
		version_column_num = merged_block.getPositionByName(params.version_column_name);

		for (size_t i = 0; i < num_columns; ++i)
			if (i != time_column_num && i != value_column_num && i != version_column_num)
				unmodified_column_numbers.push_back(i);

		if (selected_row.empty())
			selected_row.columns.resize(num_columns);
	}

	if (has_collation)
		merge(merged_columns, queue_with_collation);
	else
		merge(merged_columns, queue);

	return merged_block;
}


template <typename TSortCursor>
void GraphiteRollupSortedBlockInputStream::merge(ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue)
{
	const DateLUTImpl & date_lut = DateLUT::instance();

	size_t merged_rows = 0;

	/// Вынимаем строки в нужном порядке и кладём в merged_block, пока строк не больше max_block_size
	while (!queue.empty())
	{
		TSortCursor current = queue.top();

		next_path = current->all_columns[path_column_num]->getDataAt(current->pos);
		next_time = current->all_columns[time_column_num]->get64(current->pos);

		auto prev_pattern = current_pattern;
		bool path_differs = is_first || next_path != current_path;
		is_first = false;

		/// Is new key before rounding.
		bool is_new_key = path_differs || next_time != current_time;

		UInt64 current_version = current->all_columns[version_column_num]->get64(current->pos);

		if (is_new_key)
		{
			current_path = next_path;
			current_time = next_time;

			/// For previous group of rows with same key, accumulate a row that has maximum version.
			if (merged_rows)
				accumulateRow(selected_row);

			if (path_differs)
				current_pattern = selectPatternForPath(next_path);

			if (current_pattern)
			{
				UInt32 precision = selectPrecision(current_pattern->retentions, next_time);
				next_time_rounded = roundTimeToPrecision(date_lut, next_time, precision);
			}
			/// If no patterns has matched - it means that no need to do rounding.

			/// Key will be new after rounding. It means new result row.
			bool will_be_new_key = path_differs || next_time_rounded != current_time_rounded;

			if (will_be_new_key)
			{
				/// This is not the first row in block.
				if (merged_rows)
				{
					finishCurrentRow(merged_columns);

					/// if we have enough rows
					if (merged_rows >= max_block_size)
						return;
				}

				startNextRow(merged_columns, current);

				current_time_rounded = next_time_rounded;

				if (prev_pattern)
					prev_pattern->function->destroy(place_for_aggregate_state.data());
				if (current_pattern)
					current_pattern->function->create(place_for_aggregate_state.data());

				++merged_rows;
			}
		}

		/// Within all rows with same key, we should leave only one row with maximum version;
		///  and for rows with same maximum version - only last row.
		if (is_new_key || current_version >= current_max_version)
		{
			current_max_version = current_version;
			setRowRef(selected_row, current);
		}

		queue.pop();

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

	/// Write result row for the last group.
	++merged_rows;
	accumulateRow(selected_row);
	finishCurrentRow(merged_columns);

	finished = true;

	if (current_pattern)
	{
		current_pattern->function->destroy(place_for_aggregate_state.data());
		current_pattern = nullptr;
	}
}


template <class TSortCursor>
void GraphiteRollupSortedBlockInputStream::startNextRow(ColumnPlainPtrs & merged_columns, TSortCursor & cursor)
{
	/// Копируем не модифицированные значения столбцов.
	for (size_t i = 0, size = unmodified_column_numbers.size(); i < size; ++i)
	{
		size_t j = unmodified_column_numbers[i];
		merged_columns[j]->insertFrom(*cursor->all_columns[j], cursor->pos);
	}

	if (!current_pattern)
		merged_columns[value_column_num]->insertFrom(*cursor->all_columns[value_column_num], cursor->pos);
}


void GraphiteRollupSortedBlockInputStream::finishCurrentRow(ColumnPlainPtrs & merged_columns)
{
	/// Вставляем вычисленные значения столбцов time, value, version.
	merged_columns[time_column_num]->insert(UInt64(current_time_rounded));
	merged_columns[version_column_num]->insert(current_max_version);

	if (current_pattern)
		current_pattern->function->insertResultInto(place_for_aggregate_state.data(), *merged_columns[value_column_num]);
}


void GraphiteRollupSortedBlockInputStream::accumulateRow(RowRef & row)
{
	if (current_pattern)
		current_pattern->function->add(place_for_aggregate_state.data(), &row.columns[value_column_num], row.row_num, nullptr);
}

}
