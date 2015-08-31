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
			if (const auto array_type = typeid_cast<const DataTypeArray *>(column.type.get()))
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


template<class TSortCursor>
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

}
