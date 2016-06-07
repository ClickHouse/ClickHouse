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

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
}


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

	String getID() const override;

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
	 *		- первый столбец, а также все столбцы, имена которых заканчиваются на ID, Key или Type - числовые ((U)IntN, Date, DateTime);
	 *        (кортеж из таких столбцов назовём keys)
	 *		- остальные столбцы - арифметические ((U)IntN, Float32/64), условно (values...).
	 *	Такая вложенная таблица воспринимается как отображение (keys...) => (values...) и при слиянии
	 *	ее строк выполняется слияние элементов двух множеств по (keys...) со сложением соответствующих (values...).
	 *
	 *	Пример:
	 *	[(1, 100)] + [(2, 150)] -> [(1, 100), (2, 150)]
	 *	[(1, 100)] + [(1, 150)] -> [(1, 250)]
	 *	[(1, 100)] + [(1, 150), (2, 150)] -> [(1, 250), (2, 150)]
	 *	[(1, 100), (2, 150)] + [(1, -100)] -> [(2, 150)]
	 *
	 *  Эта весьма необычная функциональность сделана исключительно для БК,
	 *   не предназначена для использования кем-либо ещё,
	 *   и может быть удалена в любой момент.
	 */

	/// Хранит номера столбцов-ключей и столбцов-значений.
	struct MapDescription
	{
		std::vector<size_t> key_col_nums;
		std::vector<size_t> val_col_nums;
	};

	/// Найденные вложенные Map-таблицы.
	std::vector<MapDescription> maps_to_sum;

	RowRef current_key;		/// Текущий первичный ключ.
	RowRef next_key;		/// Первичный ключ следующей строки.

	Row current_row;
	bool current_row_is_zero = true;	/// Текущая строчка просуммировалась в ноль, и её следует удалить.

	bool output_is_non_empty = false;	/// Отдали ли мы наружу хоть одну строку.

	/** Делаем поддержку двух разных курсоров - с Collation и без.
	 *  Шаблоны используем вместо полиморфных SortCursor'ов и вызовов виртуальных функций.
	 */
	template <class TSortCursor>
	void merge(ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue);

	/// Вставить в результат просуммированную строку для текущей группы.
	void insertCurrentRow(ColumnPlainPtrs & merged_columns);

	/** Для вложенных Map выполняется слияние по ключу с выбрасыванием строк вложенных массивов, в которых
	  * все элементы - нулевые.
	  */
	template <class TSortCursor>
	bool mergeMaps(Row & row, TSortCursor & cursor);

	template <class TSortCursor>
	bool mergeMap(const MapDescription & map, Row & row, TSortCursor & cursor);

	/** Прибавить строчку под курсором к row.
	  * Возвращает false, если результат получился нулевым.
	  */
	template <class TSortCursor>
	bool addRow(Row & row, TSortCursor & cursor);
};

}
