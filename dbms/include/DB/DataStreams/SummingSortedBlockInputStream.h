#pragma once

#include <Yandex/logger_useful.h>

#include <DB/Core/Row.h>
#include <DB/Core/ColumnNumbers.h>
#include <DB/DataStreams/MergingSortedBlockInputStream.h>


namespace DB
{

/** Соединяет несколько сортированных потоков в один.
  * При этом, для каждой группы идущих подряд одинаковых значений первичного ключа (столбцов, по которым сортируются данные),
  *  схлопывает их в одну строку, суммируя все числовые столбцы кроме первичного ключа.
  */
class SummingSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
	SummingSortedBlockInputStream(BlockInputStreams inputs_, SortDescription & description_, size_t max_block_size_)
		: MergingSortedBlockInputStream(inputs_, description_, max_block_size_),
		log(&Logger::get("SummingSortedBlockInputStream"))
	{
	}

	String getName() const { return "SummingSortedBlockInputStream"; }

	String getID() const
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
	Block readImpl();

private:
	Logger * log;

	/// Столбцы с какими номерами надо суммировать.
	ColumnNumbers column_numbers_to_sum;

	Row current_key;		/// Текущий первичный ключ.
	Row next_key;			/// Первичный ключ следующей строки.
	
	Row current_row;

	/** Делаем поддержку двух разных курсоров - с Collation и без.
	 *  Шаблоны используем вместо полиморфных SortCursor'ов и вызовов виртуальных функций.
	 */
	template<class TSortCursor>
	void merge(Block & merged_block, ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue);

	/// Вставить в результат просуммированную строку для текущей группы.
	void insertCurrentRow(ColumnPlainPtrs & merged_columns);


	/** Реализует операцию += */
	class FieldVisitorSum : public StaticVisitor<>
	{
	private:
		const Field & rhs;
	public:
		FieldVisitorSum(const Field & rhs_) : rhs(rhs_) {}

		void operator() (UInt64 	& x) const { x += get<UInt64>(rhs); }
		void operator() (Int64 		& x) const { x += get<Int64>(rhs); }
		void operator() (Float64 	& x) const { x += get<Float64>(rhs); }

		void operator() (Null 		& x) const { throw Exception("Cannot sum Nulls", ErrorCodes::LOGICAL_ERROR); }
		void operator() (String 	& x) const { throw Exception("Cannot sum Strings", ErrorCodes::LOGICAL_ERROR); }
		void operator() (Array 		& x) const { throw Exception("Cannot sum Arrays", ErrorCodes::LOGICAL_ERROR); }
	};

	/// Прибавить строчку под курсором к row.
	template<class TSortCursor>
	void addRow(Row & row, TSortCursor & cursor)
	{
		for (size_t i = 0, size = column_numbers_to_sum.size(); i < size; ++i)
		{
			size_t j = column_numbers_to_sum[i];
			apply_visitor(FieldVisitorSum((*cursor->all_columns[j])[cursor->pos]), row[j]);
		}
	}
};

}
