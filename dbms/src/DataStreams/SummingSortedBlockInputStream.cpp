#include <DB/DataStreams/SummingSortedBlockInputStream.h>
#include <DB/DataTypes/DataTypeNested.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <boost/range/iterator_range_core.hpp>


namespace DB
{


void SummingSortedBlockInputStream::insertCurrentRow(ColumnPlainPtrs & merged_columns)
{
	for (size_t i = 0; i < num_columns; ++i)
		merged_columns[i]->insert(current_row[i]);
}


namespace
{
	bool endsWith(const std::string & s, const std::string & suffix)
	{
		return s.size() >= suffix.size() && 0 == strncmp(s.data() + s.size() - suffix.size(), suffix.data(), suffix.size());
	}

	bool isInPrimaryKey(const SortDescription & description, const std::string & name, const std::size_t number)
	{
		for (auto & desc : description)
			if (desc.column_name == name || (desc.column_name.empty() && desc.column_number == number))
				return true;

		return false;
	}
}


Block SummingSortedBlockInputStream::readImpl()
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

		std::unordered_map<std::string, std::vector<std::size_t>> discovered_maps;
		/** Заполним номера столбцов, которые должны быть просуммированы.
		  * Это могут быть только числовые столбцы, не входящие в ключ сортировки.
		  * Если задан непустой список column_names_to_sum, то берём только эти столбцы.
		  * Часть столбцов из column_names_to_sum может быть не найдена. Это игнорируется.
		  */
		for (size_t i = 0; i < num_columns; ++i)
		{
			ColumnWithTypeAndName & column = merged_block.getByPosition(i);

			/// Discover nested Maps and find columns for summation
			if (typeid_cast<const DataTypeArray *>(column.type.get()))
			{
				const auto map_name = DataTypeNested::extractNestedTableName(column.name);
				/// if nested table name ends with `Map` it is a possible candidate for special handling
				if (map_name == column.name || !endsWith(map_name, "Map"))
					continue;

				discovered_maps[map_name].emplace_back(i);
			}
			else
			{
				/// Оставляем только числовые типы. При чём, даты и даты-со-временем здесь такими не считаются.
				if (!column.type->isNumeric() || column.type->getName() == "Date" ||
												 column.type->getName() == "DateTime")
					continue;

				/// Входят ли в PK?
				if (isInPrimaryKey(description, column.name, i))
					continue;

				if (column_names_to_sum.empty()
					|| column_names_to_sum.end() !=
					   std::find(column_names_to_sum.begin(), column_names_to_sum.end(), column.name))
				{
					column_numbers_to_sum.push_back(i);
				}
			}
		}

		/// select actual nested Maps from list of candidates
		for (const auto & map : discovered_maps)
		{
			/// map should contain at least two elements (key -> value)
			if (map.second.size() < 2)
				continue;

			/// check type of key
			const auto key_num = map.second.front();
			auto & key_col = merged_block.getByPosition(key_num);
			/// skip maps, whose members are part of primary key
			if (isInPrimaryKey(description, key_col.name, key_num))
				continue;

			auto & key_nested_type = static_cast<const DataTypeArray *>(key_col.type.get())->getNestedType();
			/// key can only be integral
			if (!key_nested_type->isNumeric() || key_nested_type->getName() == "Float32" || key_nested_type->getName() == "Float64")
				continue;

			/// check each value type (skip the first column number which is for key)
			auto correct_types = true;
			for (auto & value_num : boost::make_iterator_range(std::next(map.second.begin()), map.second.end()))
			{
				auto & value_col = merged_block.getByPosition(value_num);
				/// skip maps, whose members are part of primary key
				if (isInPrimaryKey(description, value_col.name, value_num))
				{
					correct_types = false;
					break;
				}

				auto & value_nested_type = static_cast<const DataTypeArray *>(value_col.type.get())->getNestedType();
				/// value can be any arithmetic type except date and datetime
				if (!value_nested_type->isNumeric() || value_nested_type->getName() == "Date" ||
													   value_nested_type->getName() == "DateTime")
				{
					correct_types = false;
					break;
				}
			}

			if (correct_types)
				maps_to_sum.push_back({ key_num, { std::next(map.second.begin()), map.second.end() } });
		}
	}

	if (has_collation)
		merge(merged_columns, queue_with_collation);
	else
		merge(merged_columns, queue);

	return merged_block;
}


template <class TSortCursor>
void SummingSortedBlockInputStream::merge(ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue)
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
	}

	/// Запишем данные для последней группы, если она ненулевая.
	/// Если она нулевая, и без нее выходной поток окажется пустым, запишем ее все равно.
	if (!current_row_is_zero || !output_is_non_empty)
	{
		++merged_rows;
		insertCurrentRow(merged_columns);
	}

	finished = true;
}


template <class TSortCursor>
bool SummingSortedBlockInputStream::mergeMaps(Row & row, TSortCursor & cursor)
{
	auto non_empty_map_present = false;

	/// merge nested maps
	for (const auto & map : maps_to_sum)
	{
		const size_t val_count = map.val_col_nums.size();

		/// fetch key array reference from accumulator-row
		auto & key_array_lhs = row[map.key_col_num].get<Array>();
		/// returns a Field for pos-th item of val_index-th value
		const auto val_getter_lhs = [&] (const auto val_index, const auto pos) -> decltype(auto)
		{
			return row[map.val_col_nums[val_index]].get<Array>()[pos];
		};

		/// we will be sorting key positions, not the entire rows, to minimize actions
		std::vector<std::size_t> key_pos_lhs(ext::range_iterator<std::size_t>{0},
			ext::range_iterator<std::size_t>{key_array_lhs.size()});
		std::sort(std::begin(key_pos_lhs), std::end(key_pos_lhs), [&] (const auto pos1, const auto pos2)
		{
			return key_array_lhs[pos1] < key_array_lhs[pos2];
		});

		/// copy key field from current row under cursor
		const auto key_field_rhs = (*cursor->all_columns[map.key_col_num])[cursor->pos];
		/// for each element of `map.val_col_nums` copy corresponding array under cursor into vector
		const auto val_fields_rhs = ext::map<std::vector>(map.val_col_nums,
			[&] (const auto col_num) -> decltype(auto) {
				return (*cursor->all_columns[col_num])[cursor->pos];
			});

		/// fetch key array reference from row under cursor
		const auto & key_array_rhs = key_field_rhs.get<Array>();
		/// returns a Field for pos-th item of val_index-th value
		const auto val_getter_rhs = [&] (const auto val_index, const auto pos) -> decltype(auto)
		{
			return val_fields_rhs[val_index].get<Array>()[pos];
		};

		std::vector<std::size_t> key_pos_rhs(ext::range_iterator<std::size_t>{0},
			ext::range_iterator<std::size_t>{key_array_rhs.size()});
		std::sort(std::begin(key_pos_rhs), std::end(key_pos_rhs), [&] (const auto pos1, const auto pos2)
		{
			return key_array_rhs[pos1] < key_array_rhs[pos2];
		});

		/// max size after merge estimation
		const auto max_size = key_pos_lhs.size() + key_pos_rhs.size();

		/// create arrays with a single element (it will be overwritten on first iteration)
		Array key_array_result(1);
		key_array_result.reserve(max_size);
		std::vector<Array> val_arrays_result(val_count, Array(1));
		for (auto & val_array_result : val_arrays_result)
			val_array_result.reserve(max_size);

		/// discard first element
		bool discard_prev = true;

		/// either insert or merge new element
		const auto insert_or_sum = [val_count, &discard_prev, &key_array_result, &val_arrays_result]
			(std::size_t & index, const std::vector<std::size_t> & key_pos, const auto & key_array, auto && val_getter)
		{
			const auto pos = key_pos[index++];
			const auto & key = key_array[pos];

			if (discard_prev)
			{
				discard_prev = false;

				key_array_result.back() = key;
				for (const auto val_index : ext::range(0, val_count))
					val_arrays_result[val_index].back() = val_getter(val_index, pos);
			}
			else if (key_array_result.back() == key)
			{
				/// merge with same key
				auto should_discard = true;

				for (const auto val_index : ext::range(0, val_count))
					if (apply_visitor(FieldVisitorSum(val_getter(val_index, pos)),
						val_arrays_result[val_index].back()))
						should_discard = false;

				discard_prev = should_discard;
			}
			else
			{
				/// append new key
				key_array_result.emplace_back(key);
				for (const auto val_index : ext::range(0, val_count))
					val_arrays_result[val_index].emplace_back(val_getter(val_index, pos));
			}
		};

		std::size_t index_lhs = 0;
		std::size_t index_rhs = 0;

		/// perform 2-way merge
		while (true)
			if (index_lhs < key_pos_lhs.size() && index_rhs == key_pos_rhs.size())
				insert_or_sum(index_lhs, key_pos_lhs, key_array_lhs, val_getter_lhs);
			else if (index_lhs == key_pos_lhs.size() && index_rhs < key_pos_rhs.size())
				insert_or_sum(index_rhs, key_pos_rhs, key_array_rhs, val_getter_rhs);
			else if (index_lhs < key_pos_lhs.size() && index_rhs < key_pos_rhs.size())
				if (key_array_lhs[key_pos_lhs[index_lhs]] < key_array_rhs[key_pos_rhs[index_rhs]])
					insert_or_sum(index_lhs, key_pos_lhs, key_array_lhs, val_getter_lhs);
				else
					insert_or_sum(index_rhs, key_pos_rhs, key_array_rhs, val_getter_rhs);
			else
				break;

		/// discard last row if necessary
		if (discard_prev)
			key_array_result.pop_back();

		/// store results into accumulator-row
		key_array_lhs = std::move(key_array_result);
		for (const auto val_col_index : ext::range(0, val_count))
		{
			/// discard last row if necessary
			if (discard_prev)
				val_arrays_result[val_col_index].pop_back();

			row[map.val_col_nums[val_col_index]].get<Array>() = std::move(val_arrays_result[val_col_index]);
		}

		if (!key_array_lhs.empty())
			non_empty_map_present = true;
	}

	return non_empty_map_present;
}


template <class TSortCursor>
bool SummingSortedBlockInputStream::addRow(Row & row, TSortCursor & cursor)
{
	bool res = mergeMaps(row, cursor);	/// Есть ли хотя бы одно ненулевое число или непустой массив

	for (size_t i = 0, size = column_numbers_to_sum.size(); i < size; ++i)
	{
		size_t j = column_numbers_to_sum[i];
		if (apply_visitor(FieldVisitorSum((*cursor->all_columns[j])[cursor->pos]), row[j]))
			res = true;
	}

	return res;
}

}
