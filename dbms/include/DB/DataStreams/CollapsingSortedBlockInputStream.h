#pragma once

#include <DB/Core/Row.h>
#include <DB/DataStreams/MergingSortedBlockInputStream.h>


namespace DB
{

/** Соединяет несколько сортированных потоков в один.
  * При этом, для каждой группы идущих подряд одинаковых значений столбца id_column (например, идентификатора визита),
  *  оставляет не более одной строки со значением столбца sign_column = -1 ("положительной строки")
  *  и не более одиной строки со значением столбца sign_column = 1 ("отрицательной строки").
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
		const String & id_column_, const String & sign_column_, size_t max_block_size_)
		: MergingSortedBlockInputStream(inputs_, description_, max_block_size_),
		id_column(id_column_), sign_column(sign_column_),
		id_column_number(0), sign_column_number(0),
		log(&Logger::get("CollapsingSortedBlockInputStream")),
		current_id(0), count_positive(0), count_negative(0)
	{
	}

	/// Может возвращаться на 1 больше записей, чем max_block_size.
	Block readImpl();

	String getName() const { return "CollapsingSortedBlockInputStream"; }

	BlockInputStreamPtr clone() { return new CollapsingSortedBlockInputStream(inputs, description, id_column, sign_column, max_block_size); }

private:
	String id_column;
	String sign_column;

	size_t id_column_number;
	size_t sign_column_number;

	Logger * log;

	UInt64 current_id;		/// Текущий идентификатор "визита".
	
	Row first_negative;		/// Первая отрицательная строка для текущего идентификатора "визита".
	Row last_positive;		/// Последняя положительная строка для текущего идентификатора "визита".

	size_t count_positive;	/// Количество положительных строк для текущего идентификатора "визита".
	size_t count_negative;	/// Количество отрицательных строк для текущего идентификатора "визита".


	/// Сохранить строчку, на которую указывает cursor в row.
	void setRow(Row & row, SortCursor & cursor)
	{
		for (size_t i = 0; i < num_columns; ++i)
			row[i] = (*cursor->all_columns[i])[cursor->pos];
	}

	/// Вставить в результат строки для текущего идентификатора "визита".
	void insertRows(ColumnPlainPtrs & merged_columns, size_t & merged_rows);
};

}
