#pragma once

#include <Yandex/logger_useful.h>

#include <DB/Core/Row.h>
#include <DB/Core/ColumnNumbers.h>
#include <DB/DataStreams/MergingSortedBlockInputStream.h>
#include <DB/Storages/MergeTree/PKCondition.h>
#include <statdaemons/ext/range.hpp>
#include <statdaemons/ext/map.hpp>


namespace DB
{

/** Соединяет несколько сортированных потоков в один.
  * При этом, для каждой группы идущих подряд одинаковых значений первичного ключа (столбцов, по которым сортируются данные),
  *  схлопывает их в одну строку, суммируя все числовые столбцы кроме первичного ключа.
  * Если во всех числовых столбцах кроме первичного ключа получился ноль, то удаляет строчку.
  */
class SummingSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
	SummingSortedBlockInputStream(BlockInputStreams inputs_,
		const SortDescription & description_,
		/// Список столбцов, которых нужно суммировать. Если пустое - берутся все числовые столбцы, не входящие в description.
		const Names & column_names_to_sum_,
		size_t max_block_size_)
		: MergingSortedBlockInputStream(inputs_, description_, max_block_size_), column_names_to_sum(column_names_to_sum_)
	{
	}

	String getName() const override { return "SummingSortedBlockInputStream"; }

	String getID() const override
	{
		std::stringstream res;
		res << "SummingSorted(inputs";

		for (size_t i = 0; i < children.size(); ++i)
			res << ", " << children[i]->getID();

		res << ", description";

		for (size_t i = 0; i < description.size(); ++i)
			res << ", " << description[i].getID();

		res << ")";
		return res.str();
	}

protected:
	/// Может возвращаться на 1 больше записей, чем max_block_size.
	Block readImpl() override;

private:
	Logger * log = &Logger::get("SummingSortedBlockInputStream");

	/// Прочитали до конца.
	bool finished = false;

	/// Столбцы с какими номерами надо суммировать.
	Names column_names_to_sum;	/// Если задано - преобразуется в column_numbers_to_sum при инициализации.
	ColumnNumbers column_numbers_to_sum;

	/** Таблица может иметь вложенные таблицы, обрабатываемые особым образом.
	 *	Если название вложенной таблицы заканчинвается на `Map` и она содержит не менее двух столбцов,
	 *	удовлетворяющих следующим критериям:
	 *		- первый столбец - числовой ((U)IntN, Date, DateTime), назовем его условно key,
	 *		- остальные столбцы - арифметические ((U)IntN, Float32/64), условно (values...).
	 *	Такая вложенная таблица воспринимается как отображение key => (values...) и при слиянии
	 *	ее строк выполняется слияние элементов двух множеств по key со сложением соответствующих (values...).
	 *	Пример:
	 *	[(1, 100)] + [(2, 150)] -> [(1, 100), (2, 150)]
	 *	[(1, 100)] + [(1, 150)] -> [(1, 250)]
	 *	[(1, 100)] + [(1, 150), (2, 150)] -> [(1, 250), (2, 150)]
	 *	[(1, 100), (2, 150)] + [(1, -100)] -> [(2, 150)]
	 */

	/// Хранит номера столбца-ключа и столбцов-значений
	struct map_description
	{
		std::size_t key_col_num;
		std::vector<std::size_t> val_col_nums;
	};

	/// Найденные вложенные Map таблицы
	std::vector<map_description> maps_to_sum;

	Row current_key;		/// Текущий первичный ключ.
	Row next_key;			/// Первичный ключ следующей строки.

	Row current_row;
	bool current_row_is_zero = false;	/// Текущая строчка просуммировалась в ноль, и её следует удалить.

	bool output_is_non_empty = false; /// Отдали ли мы наружу хоть одну строку.

	/** Делаем поддержку двух разных курсоров - с Collation и без.
	 *  Шаблоны используем вместо полиморфных SortCursor'ов и вызовов виртуальных функций.
	 */
	template<class TSortCursor>
	void merge(Block & merged_block, ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue);

	/// Вставить в результат просуммированную строку для текущей группы.
	void insertCurrentRow(ColumnPlainPtrs & merged_columns);


	/** Реализует операцию +=.
	  * Возвращает false, если результат получился нулевым.
	  */
	class FieldVisitorSum : public StaticVisitor<bool>
	{
	private:
		const Field & rhs;
	public:
		FieldVisitorSum(const Field & rhs_) : rhs(rhs_) {}

		bool operator() (UInt64 	& x) const { x += get<UInt64>(rhs); return x != 0; }
		bool operator() (Int64 		& x) const { x += get<Int64>(rhs); return x != 0; }
		bool operator() (Float64 	& x) const { x += get<Float64>(rhs); return x != 0; }

		bool operator() (Null 		& x) const { throw Exception("Cannot sum Nulls", ErrorCodes::LOGICAL_ERROR); }
		bool operator() (String 	& x) const { throw Exception("Cannot sum Strings", ErrorCodes::LOGICAL_ERROR); }
		bool operator() (Array 		& x) const { throw Exception("Cannot sum Arrays", ErrorCodes::LOGICAL_ERROR); }
	};

	/** Прибавить строчку под курсором к row.
	  * Для вложенных Map выполняется слияние по ключу с выбрасыванием нулевых элементов.
	  * Возвращает false, если результат получился нулевым.
	  */
	template<class TSortCursor>
	bool addRow(Row & row, TSortCursor & cursor)
	{
		/// merge nested maps
		for (const auto & map : maps_to_sum)
		{
			const auto value_count = map.val_col_nums.size();

			/// fetch key and val array references from accumulator-row
			auto & key_array = row[map.key_col_num].get<Array>();

			std::map<Field, std::vector<Field>> result;

			/// populate map from current row
			for (const auto i : ext::range(0, key_array.size()))
			{
				const auto it = result.find(key_array[i]);
				/// row for such key is not key present in the map, emplace
				if (it == std::end(result))
				{
					/// compose a vector of i-th elements of each one of `map.val_col_nums`
					result.emplace(key_array[i],
						ext::map<std::vector>(map.val_col_nums, [&] (const auto col_num) -> decltype(auto) {
							return row[col_num].get<Array>()[i];
						}));
				}
				else
				{
					/// row for requested key found, merge corresponding values
					for (const auto val_col_index : ext::range(0, value_count))
					{
						const auto col_num = map.val_col_nums[val_col_index];
						apply_visitor(FieldVisitorSum{row[col_num].get<Array>()[i]}, it->second[val_col_index]);
					}
				}
			}

			/// copy key and value fields from current row under cursor
			const auto key_field_rhs = (*cursor->all_columns[map.key_col_num])[cursor->pos];
			/// for each element of `map.val_col_nums` get corresponding array under cursor and put into vector
			const auto val_fields_rhs = ext::map<std::vector>(map.val_col_nums,
				[&] (const auto col_num) -> decltype(auto) {
					return (*cursor->all_columns[col_num])[cursor->pos];
				});

			/// fetch key array reference from current row
			const auto & key_array_rhs = key_field_rhs.get<Array>();

			/// merge current row into map
			for (const auto i : ext::range(0, key_array_rhs.size()))
			{
				const auto it = result.find(key_array_rhs[i]);
				/// row for such key is not key present in the map, emplace
				if (it == std::end(result))
				{
					/// compose a vector of i-th elements of each one of `map.val_col_nums`
					result.emplace(key_array_rhs[i],
						ext::map<std::vector>(val_fields_rhs, [&] (const auto & val_field) -> decltype(auto) {
							return val_field.get<Array>()[i];
						}));
				}
				else
				{
					/// row for requested key found, merge corresponding values
					for (const auto val_col_index : ext::range(0, value_count))
					{
						apply_visitor(FieldVisitorSum{val_fields_rhs[val_col_index].get<Array>()[i]},
							it->second[val_col_index]);
					}
				}
			}

			Array key_array_result;
			std::vector<Array> val_arrays_result(value_count);

			/// serialize result key-value pairs back into separate arrays of keys and values
			const auto zero_field = nearestFieldType(0);
			for (const auto & key_values_pair : result)
			{
				auto only_zeroes = true;

				for (const auto & value : key_values_pair.second)
					if (!apply_visitor(FieldVisitorAccurateEquals{}, value, zero_field))
						only_zeroes = false;

				if (only_zeroes)
					continue;

				key_array_result.emplace_back(key_values_pair.first);
				for (const auto val_col_index : ext::range(0, value_count))
					val_arrays_result[val_col_index].emplace_back(key_values_pair.second[val_col_index]);
			}

			/// replace accumulator row key and value arrays with the merged ones
			key_array = std::move(key_array_result);
			for (const auto val_col_index : ext::range(0, value_count))
				row[map.val_col_nums[val_col_index]].get<Array>() = std::move(val_arrays_result[val_col_index]);
		}

		bool res = false;	/// Есть ли хотя бы одно ненулевое число.

		for (size_t i = 0, size = column_numbers_to_sum.size(); i < size; ++i)
		{
			size_t j = column_numbers_to_sum[i];
			if (apply_visitor(FieldVisitorSum((*cursor->all_columns[j])[cursor->pos]), row[j]))
				res = true;
		}

		return res;
	}
};

}
