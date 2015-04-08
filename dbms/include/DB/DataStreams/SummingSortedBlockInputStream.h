#pragma once

#include <Yandex/logger_useful.h>

#include <DB/Core/Row.h>
#include <DB/Core/ColumnNumbers.h>
#include <DB/DataStreams/MergingSortedBlockInputStream.h>
#include <statdaemons/ext/range.hpp>
#include <DB/Storages/MergeTree/PKCondition.h>


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

	struct map_description
	{
		std::size_t key_col_num;
		std::size_t val_col_num;
	};

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

	using map_merge_t = std::map<Field, Field>;
	class MapSumVisitor : public StaticVisitor<void>
	{
	public:
		map_merge_t & map;
		const Field & key;

	public:
		MapSumVisitor(map_merge_t & map, const Field & key) : map(map), key(key) {}

		void operator()(const UInt64 val) const
		{
			const auto it = map.find(key);
			if (it == std::end(map))
				map.emplace(key, Field{val});
			else
				it->second.get<UInt64>() += val;
		}

		void operator()(const Int64 val) const
		{
			const auto it = map.find(key);
			if (it == std::end(map))
				map.emplace(key, Field{val});
			else
				it->second.get<Int64>() += val;
		}

		void operator()(const Float64 val) const
		{
			const auto it = map.find(key);
			if (it == std::end(map))
				map.emplace(key, Field{val});
			else
				it->second.get<Float64>() += val;
		}

		void operator() (Null) const { throw Exception("Cannot merge Nulls", ErrorCodes::LOGICAL_ERROR); }
		void operator() (String) const { throw Exception("Cannot merge Strings", ErrorCodes::LOGICAL_ERROR); }
		void operator() (Array) const { throw Exception("Cannot merge Arrays", ErrorCodes::LOGICAL_ERROR); }
	};

	/** Прибавить строчку под курсором к row.
	  * Возвращает false, если результат получился нулевым.
	  */
	template<class TSortCursor>
	bool addRow(Row & row, TSortCursor & cursor)
	{
		/// merge maps
		for (const auto & map : maps_to_sum)
		{
			auto & key_array = row[map.key_col_num].get<Array>();
			auto & val_array = row[map.val_col_num].get<Array>();
			if (key_array.size() != val_array.size())
				throw Exception{"Nested arrays have different sizes", ErrorCodes::LOGICAL_ERROR};

			const auto key_field_rhs = (*cursor->all_columns[map.key_col_num])[cursor->pos];
			const auto val_field_rhs = (*cursor->all_columns[map.val_col_num])[cursor->pos];

			const auto & key_array_rhs = key_field_rhs.get<Array>();
			const auto & val_array_rhs = val_field_rhs.get<Array>();
			if (key_array_rhs.size() != val_array_rhs.size())
				throw Exception{"Nested arrays have different sizes", ErrorCodes::LOGICAL_ERROR};

			map_merge_t result;

			/// merge accumulator-row and current row from cursor using std::map
			for (const auto i : ext::range(0, key_array.size()))
				apply_visitor(MapSumVisitor{result, key_array[i]}, val_array[i]);

			for (const auto i : ext::range(0, key_array_rhs.size()))
				apply_visitor(MapSumVisitor{result, key_array_rhs[i]}, val_array_rhs[i]);

			Array key_array_result;
			Array val_array_result;

			/// skip items with value == 0
			for (const auto & pair : result)
			{
				if (!apply_visitor(FieldVisitorAccurateEquals{}, pair.second, nearestFieldType(0)))
				{
					key_array_result.emplace_back(pair.first);
					val_array_result.emplace_back(pair.second);
				}
			}

			key_array = std::move(key_array_result);
			val_array = std::move(val_array_result);
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
