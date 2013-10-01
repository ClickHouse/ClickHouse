#pragma once

#include <Yandex/logger_useful.h>

#include <DB/DataStreams/MergingSortedBlockInputStream.h>


namespace DB
{

/** Соединяет несколько сортированных потоков в один.
  * При этом, для каждой группы идущих подряд одинаковых значений первичного ключа (столбцов, по которым сортируются данные),
  *  оставляет не более одной строки со значением столбца sign_column = -1 ("отрицательной строки")
  *  и не более одиной строки со значением столбца sign_column = 1 ("положительной строки").
  * То есть - производит схлопывание записей из лога изменений.
  *
  * Если количество положительных и отрицательных строк совпадает - то пишет первую отрицательную и последнюю положительную строку.
  * Если положительных на 1 больше, чем отрицательных - то пишет только последнюю положительную строку.
  * Если отрицательных на 1 больше, чем положительных - то пишет только первую отрицательную строку.
  * Иначе - логическая ошибка.
  */
class CollapsingSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
	CollapsingSortedBlockInputStream(BlockInputStreams inputs_, SortDescription & description_,
		const String & sign_column_, size_t max_block_size_)
		: MergingSortedBlockInputStream(inputs_, description_, max_block_size_),
		sign_column(sign_column_), sign_column_number(0),
		log(&Logger::get("CollapsingSortedBlockInputStream")),
		count_positive(0), count_negative(0), count_incorrect_data(0)
	{
	}

	String getName() const { return "CollapsingSortedBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "CollapsingSorted(inputs";

		for (size_t i = 0; i < children.size(); ++i)
			res << ", " << children[i]->getID();

		res << ", description";

		for (size_t i = 0; i < description.size(); ++i)
			res << ", " << description[i].getID();

		res << ", sign_column, " << sign_column << ")";
		return res.str();
	}

protected:
	/// Может возвращаться на 1 больше записей, чем max_block_size.
	Block readImpl();

private:
	String sign_column;
	size_t sign_column_number;

	Logger * log;

	Row current_key;		/// Текущий первичный ключ.
	Row next_key;			/// Первичный ключ следующей строки.
	
	Row first_negative;		/// Первая отрицательная строка для текущего первичного ключа.
	Row last_positive;		/// Последняя положительная строка для текущего первичного ключа.

	size_t count_positive;	/// Количество положительных строк для текущего первичного ключа.
	size_t count_negative;	/// Количество отрицательных строк для текущего первичного ключа.

	size_t count_incorrect_data;	/// Чтобы не писать в лог слишком много сообщений об ошибке.

	/** Делаем поддержку двух разных курсоров - с Collation и без.
	 *  Шаблоны используем вместо полиморфных SortCursor'ов и вызовов виртуальных функций.
	 */
	template<class TSortCursor>
	void merge(Block & merged_block, ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue);

	/// Вставить в результат строки для текущего идентификатора "визита".
	void insertRows(ColumnPlainPtrs & merged_columns, size_t & merged_rows);

	void reportIncorrectData();
};

}
