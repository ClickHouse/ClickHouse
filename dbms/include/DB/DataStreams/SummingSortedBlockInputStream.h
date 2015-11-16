#pragma once

#include <common/logger_useful.h>

#include <DB/Core/FieldVisitors.h>
#include <DB/Core/Row.h>
#include <DB/Core/ColumnNumbers.h>
#include <DB/DataStreams/MergingSortedBlockInputStream.h>
#include <DB/Storages/MergeTree/PKCondition.h>
#include <ext/range.hpp>
#include <ext/map.hpp>


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

	String getName() const override { return "SummingSorted"; }

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
	template <class TSortCursor>
	void merge(ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue);

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
	template <class TSortCursor>
	bool mergeMaps(Row & row, TSortCursor & cursor);

	/** Прибавить строчку под курсором к row.
	  * Возвращает false, если результат получился нулевым.
	  */
	template <class TSortCursor>
	bool addRow(Row & row, TSortCursor & cursor);
};

}
