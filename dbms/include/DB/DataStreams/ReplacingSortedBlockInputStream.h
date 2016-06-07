#pragma once

#include <common/logger_useful.h>

#include <DB/DataStreams/MergingSortedBlockInputStream.h>


namespace DB
{

/** Соединяет несколько сортированных потоков в один.
  * При этом, для каждой группы идущих подряд одинаковых значений первичного ключа (столбцов, по которым сортируются данные),
  *  оставляет
  */
class ReplacingSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
	ReplacingSortedBlockInputStream(BlockInputStreams inputs_, const SortDescription & description_,
		const String & version_column_, size_t max_block_size_)
		: MergingSortedBlockInputStream(inputs_, description_, max_block_size_),
		version_column(version_column_)
	{
	}

	String getName() const override { return "ReplacingSorted"; }

	String getID() const override
	{
		std::stringstream res;
		res << "ReplacingSorted(inputs";

		for (size_t i = 0; i < children.size(); ++i)
			res << ", " << children[i]->getID();

		res << ", description";

		for (size_t i = 0; i < description.size(); ++i)
			res << ", " << description[i].getID();

		res << ", version_column, " << version_column << ")";
		return res.str();
	}

protected:
	/// Может возвращаться на 1 больше записей, чем max_block_size.
	Block readImpl() override;

private:
	String version_column;
	ssize_t version_column_number = -1;

	Logger * log = &Logger::get("ReplacingSortedBlockInputStream");

	/// Прочитали до конца.
	bool finished = false;

	RowRef current_key;			/// Текущий первичный ключ.
	RowRef next_key;			/// Первичный ключ следующей строки.

	RowRef selected_row;		/// Последняя строка с максимальной версией для текущего первичного ключа.

	UInt64 max_version = 0;		/// Максимальная версия для текущего первичного ключа.

	template<class TSortCursor>
	void merge(ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue);

	/// Вставить в результат строки для текущего первичного ключа.
	void insertRow(ColumnPlainPtrs & merged_columns, size_t & merged_rows);
};

}
