#pragma once

#include <queue>

#include <Yandex/logger_useful.h>

#include <DB/Core/Row.h>
#include <DB/Core/SortDescription.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** Соединяет несколько сортированных потоков в один.
  */
class MergingSortedBlockInputStream : public IProfilingBlockInputStream
{
public:
	/// limit - если не 0, то можно выдать только первые limit строк в сортированном порядке.
	MergingSortedBlockInputStream(BlockInputStreams inputs_, const SortDescription & description_, size_t max_block_size_, size_t limit_ = 0)
		: description(description_), max_block_size(max_block_size_), limit(limit_), total_merged_rows(0), first(true), has_collation(false),
		num_columns(0), source_blocks(inputs_.size()), cursors(inputs_.size()), log(&Logger::get("MergingSortedBlockInputStream"))
	{
		children.insert(children.end(), inputs_.begin(), inputs_.end());
	}

	String getName() const { return "MergingSortedBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "MergingSorted(";

		Strings children_ids(children.size());
		for (size_t i = 0; i < children.size(); ++i)
			children_ids[i] = children[i]->getID();

		/// Порядок не имеет значения.
		std::sort(children_ids.begin(), children_ids.end());

		for (size_t i = 0; i < children_ids.size(); ++i)
			res << (i == 0 ? "" : ", ") << children_ids[i];

		for (size_t i = 0; i < description.size(); ++i)
			res << ", " << description[i].getID();

		res << ")";
		return res.str();
	}

protected:
	Block readImpl();
	void readSuffixImpl();
	
	/// Инициализирует очередь и следующий блок результата.
	void init(Block & merged_block, ColumnPlainPtrs & merged_columns);
	
	/// Достаёт из источника, соответствующего current следующий блок.
	template <typename TSortCursor>
	void fetchNextBlock(const TSortCursor & current, std::priority_queue<TSortCursor> & queue);
	
	
	SortDescription description;
	size_t max_block_size;
	size_t limit;
	size_t total_merged_rows;

	bool first;
	
	bool has_collation;

	/// Текущие сливаемые блоки.
	size_t num_columns;
	Blocks source_blocks;
	
	typedef std::vector<SortCursorImpl> CursorImpls;
	CursorImpls cursors;

	typedef std::priority_queue<SortCursor> Queue;
	Queue queue;
	
	typedef std::priority_queue<SortCursorWithCollation> QueueWithCollation;
	QueueWithCollation queue_with_collation;


	/// Эти методы используются в Collapsing/Summing SortedBlockInputStream-ах.

	/// Сохранить строчку, на которую указывает cursor в row.
	template<class TSortCursor>
	void setRow(Row & row, TSortCursor & cursor)
	{
		for (size_t i = 0; i < num_columns; ++i)
			cursor->all_columns[i]->get(cursor->pos, row[i]);
	}

	/// Сохранить первичный ключ, на который указывает cursor в row.
	template<class TSortCursor>
	void setPrimaryKey(Row & row, TSortCursor & cursor)
	{
		for (size_t i = 0; i < cursor->sort_columns_size; ++i)
			cursor->sort_columns[i]->get(cursor->pos, row[i]);
	}

private:
	
	/** Делаем поддержку двух разных курсоров - с Collation и без.
	 *  Шаблоны используем вместо полиморфных SortCursor'ов и вызовов виртуальных функций.
	 */
	template <typename TSortCursor>
	void initQueue(std::priority_queue<TSortCursor> & queue);	
	
	template <typename TSortCursor>
	void merge(Block & merged_block, ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue);
	
	Logger * log;
};

}
