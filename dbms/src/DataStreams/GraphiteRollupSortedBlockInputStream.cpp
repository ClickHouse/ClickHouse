#include <DB/DataStreams/GraphiteRollupSortedBlockInputStream.h>


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
	/// Рассчитываем на то, что time_t - знаковый тип.
	for (const auto & retention : retentions)
	{
		if (time_of_merge - time >= static_cast<time_t>(retention.age))
			return retention.precision;
	}

	/// Без огрубления.
	return 1;
}


Block GraphiteRollupSortedBlockInputStream::readImpl()
{
	if (finished)
		return Block();

	if (children.size() == 1)	/// TODO Возможность прореживать даже один источник без слияния.
		return children[0]->read();

	Block merged_block;
	ColumnPlainPtrs merged_columns;

	init(merged_block, merged_columns);
	if (merged_columns.empty())
		return Block();

	/// Дополнительная инициализация.
	if (!current_path.data)
	{
		/// Определим максимальный размер состояния агрегатных функций.
		size_t max_size_of_aggregate_state = 0;
		for (const auto & pattern : params.patterns)
			if (pattern.function->sizeOfData() > max_size_of_aggregate_state)
				max_size_of_aggregate_state = pattern.function->sizeOfData();

		place_for_aggregate_state.resize(max_size_of_aggregate_state);

		/// Запомним номера столбцов.
		path_column_num = merged_block.getPositionByName(params.path_column_name);
		time_column_num = merged_block.getPositionByName(params.time_column_name);
		value_column_num = merged_block.getPositionByName(params.value_column_name);
		version_column_num = merged_block.getPositionByName(params.version_column_name);

		for (size_t i = 0; i < num_columns; ++i)
			if (i != time_column_num && i != value_column_num && i != version_column_num)
				unmodified_column_numbers.push_back(i);
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
	size_t merged_rows = 0;

	/// Вынимаем строки в нужном порядке и кладём в merged_block, пока строк не больше max_block_size
	while (!queue.empty())
	{
		TSortCursor current = queue.top();

		next_path = current->all_columns[path_column_num]->getDataAt(current->pos);
		next_time = current->all_columns[time_column_num]->get64(current->pos);

		bool is_new_key;
		bool was_first = is_first;
		auto prev_pattern = current_pattern;

		if (is_first)	/// Первый встретившийся ключ.
		{
			is_first = false;
			current_pattern = selectPatternForPath(next_path);

			if (current_pattern)
			{
				current_pattern->function->create(place_for_aggregate_state.data());
				UInt32 precision = selectPrecision(current_pattern->retentions, next_time);
				next_time = next_time / precision * precision;
			}

			current_path = next_path;
			current_time = next_time;

			is_new_key = true;
		}
		else
		{
			bool path_differs = next_path != current_path;

			if (path_differs)
				current_pattern = selectPatternForPath(next_path);

			if (current_pattern)
			{
				UInt32 precision = selectPrecision(current_pattern->retentions, next_time);
				next_time = next_time / precision * precision;
			}

			is_new_key = path_differs || next_time != current_time;

			if (is_new_key)
			{
				if (prev_pattern)
					prev_pattern->function->destroy(place_for_aggregate_state.data());
				if (current_pattern)
					current_pattern->function->create(place_for_aggregate_state.data());
			}
		}

		/// если накопилось достаточно строк и последняя посчитана полностью
		if (is_new_key && merged_rows >= max_block_size)
			return;

		queue.pop();

		if (is_new_key)
		{
			if (!was_first)
				finishCurrentRow(merged_columns);

			startNextRow(merged_columns, current);

			std::swap(current_path, next_path);
			std::swap(current_time, next_time);
			current_max_version = 0;

			if (prev_pattern)
				prev_pattern->function->destroy(place_for_aggregate_state.data());
			if (current_pattern)
				current_pattern->function->create(place_for_aggregate_state.data());

			++merged_rows;
		}

		accumulateRow(current);
		current_max_version = std::max(current_max_version, current->all_columns[version_column_num]->get64(current->pos));

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

	/// Запишем данные для последней группы.
	++merged_rows;
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
	merged_columns[time_column_num]->insert(UInt64(current_time));
	merged_columns[version_column_num]->insert(current_max_version);

	if (current_pattern)
		current_pattern->function->insertResultInto(place_for_aggregate_state.data(), *merged_columns[value_column_num]);
}


template <class TSortCursor>
void GraphiteRollupSortedBlockInputStream::accumulateRow(TSortCursor & cursor)
{
	if (current_pattern)
		current_pattern->function->add(place_for_aggregate_state.data(), &cursor->all_columns[value_column_num], cursor->pos);
}

}
