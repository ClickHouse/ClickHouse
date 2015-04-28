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

	/** Для вложенных Map выполняется слияние по ключу с выбрасыванием строк вложенных массивов, в которых
	  * все элементы - нулевые.
	  */
	template<class TSortCursor>
	void mergeMaps(Row & row, TSortCursor & cursor)
	{
		/// merge nested maps
		for (const auto & map : maps_to_sum)
		{
			const auto val_count = map.val_col_nums.size();

			/// fetch key array reference from accumulator-row
			auto & key_array_lhs = row[map.key_col_num].get<Array>();
			/// returns a Field for pos-th item of val_index-th value
			const auto val_getter_lhs = [&] (const auto val_index, const auto pos) -> decltype(auto) {
				return row[map.val_col_nums[val_index]].get<Array>()[pos];
			};

			/// we will be sorting key positions, not the entire rows, to minimize actions
			std::vector<std::size_t> key_pos_lhs(ext::range_iterator<std::size_t>{0},
				ext::range_iterator<std::size_t>{key_array_lhs.size()});
			std::sort(std::begin(key_pos_lhs), std::end(key_pos_lhs), [&] (const auto pos1, const auto pos2) {
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
			const auto val_getter_rhs = [&] (const auto val_index, const auto pos) -> decltype(auto) {
				return val_fields_rhs[val_index].get<Array>()[pos];
			};

			std::vector<std::size_t> key_pos_rhs(ext::range_iterator<std::size_t>{0},
				ext::range_iterator<std::size_t>{key_array_rhs.size()});
			std::sort(std::begin(key_pos_rhs), std::end(key_pos_rhs), [&] (const auto pos1, const auto pos2) {
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
			auto discard_prev = true;

			/// either insert or merge new element
			const auto insert_or_sum = [&] (std::size_t & index, const std::vector<std::size_t> & key_pos,
											const auto & key_array, auto && val_getter) {
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
						if (apply_visitor(FieldVisitorSum{val_getter(val_index, pos)},
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

			/// store results into accumulator-row
			key_array_lhs = std::move(key_array_result);
			for (const auto val_col_index : ext::range(0, val_count))
				row[map.val_col_nums[val_col_index]].get<Array>() = std::move(val_arrays_result[val_col_index]);
		}
	}

	/** Прибавить строчку под курсором к row.
	  * Возвращает false, если результат получился нулевым.
	  */
	template<class TSortCursor>
	bool addRow(Row & row, TSortCursor & cursor)
	{
		mergeMaps(row, cursor);

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
