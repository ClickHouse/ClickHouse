#pragma once

#include <queue>
#include <boost/intrusive_ptr.hpp>

#include <common/logger_useful.h>

#include <DB/Core/Row.h>
#include <DB/Core/SortDescription.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int CORRUPTED_DATA;
}


/// Позволяет ссылаться на строку в блоке и удерживать владение блоком,
///  и таким образом избежать создания временного объекта-строки.
/// Не используется std::shared_ptr, так как не нужно место для weak_count и deleter;
///  не используется Poco::SharedPtr, так как нужно выделять блок и refcount одним куском;
///  не используется Poco::AutoPtr, так как у него нет move конструктора и есть лишние проверки на nullptr;
/// Счётчик ссылок неатомарный, так как используется из одного потока.
namespace detail
{
	struct SharedBlock : Block
	{
		int refcount = 0;

		SharedBlock(Block && value_)
			: Block(std::move(value_)) {};
	};
}

using SharedBlockPtr = boost::intrusive_ptr<detail::SharedBlock>;

inline void intrusive_ptr_add_ref(detail::SharedBlock * ptr)
{
	++ptr->refcount;
}

inline void intrusive_ptr_release(detail::SharedBlock * ptr)
{
	if (0 == --ptr->refcount)
		delete ptr;
}


/** Соединяет несколько сортированных потоков в один.
  */
class MergingSortedBlockInputStream : public IProfilingBlockInputStream
{
public:
	/// limit - если не 0, то можно выдать только первые limit строк в сортированном порядке.
	MergingSortedBlockInputStream(BlockInputStreams inputs_, const SortDescription & description_, size_t max_block_size_, size_t limit_ = 0)
		: description(description_), max_block_size(max_block_size_), limit(limit_),
		source_blocks(inputs_.size()), cursors(inputs_.size())
	{
		children.insert(children.end(), inputs_.begin(), inputs_.end());
	}

	String getName() const override { return "MergingSorted"; }

	String getID() const override
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
	struct RowRef
	{
		ConstColumnPlainPtrs columns;
		size_t row_num;
		SharedBlockPtr shared_block;

		void swap(RowRef & other)
		{
			std::swap(columns, other.columns);
			std::swap(row_num, other.row_num);
			std::swap(shared_block, other.shared_block);
		}

		/// Количество и типы столбцов обязаны соответствовать.
		bool operator==(const RowRef & other) const
		{
			size_t size = columns.size();
			for (size_t i = 0; i < size; ++i)
				if (0 != columns[i]->compareAt(row_num, other.row_num, *other.columns[i], 1))
					return false;
			return true;
		}

		bool operator!=(const RowRef & other) const
		{
			return !(*this == other);
		}

		bool empty() const { return columns.empty(); }
		size_t size() const { return columns.size(); }
	};


	Block readImpl() override;

	void readSuffixImpl() override;

	/// Инициализирует очередь и следующий блок результата.
	void init(Block & merged_block, ColumnPlainPtrs & merged_columns);

	/// Достаёт из источника, соответствующего current следующий блок.
	template <typename TSortCursor>
	void fetchNextBlock(const TSortCursor & current, std::priority_queue<TSortCursor> & queue);


	SortDescription description;
	const size_t max_block_size;
	size_t limit;
	size_t total_merged_rows = 0;

	bool first = true;
	bool has_collation = false;

	/// May be smaller or equal to max_block_size. To do 'reserve' for columns.
	size_t expected_block_size = 0;

	/// Текущие сливаемые блоки.
	size_t num_columns = 0;
	std::vector<SharedBlockPtr> source_blocks;

	using CursorImpls = std::vector<SortCursorImpl>;
	CursorImpls cursors;

	using Queue = std::priority_queue<SortCursor>;
	Queue queue;

	using QueueWithCollation = std::priority_queue<SortCursorWithCollation>;
	QueueWithCollation queue_with_collation;


	/// Эти методы используются в Collapsing/Summing/Aggregating... SortedBlockInputStream-ах.

	/// Сохранить строчку, на которую указывает cursor, в row.
	template <class TSortCursor>
	void setRow(Row & row, TSortCursor & cursor)
	{
		for (size_t i = 0; i < num_columns; ++i)
		{
			try
			{
				cursor->all_columns[i]->get(cursor->pos, row[i]);
			}
			catch (...)
			{
				tryLogCurrentException(__PRETTY_FUNCTION__);

				/// Узнаем имя столбца и бросим исключение поинформативней.

				String column_name;
				for (const auto & block : source_blocks)
				{
					if (i < block->columns())
					{
						column_name = block->getByPosition(i).name;
						break;
					}
				}

				throw Exception("MergingSortedBlockInputStream failed to read row " + toString(cursor->pos)
					+ " of column " + toString(i) + (column_name.empty() ? "" : " (" + column_name + ")"),
					ErrorCodes::CORRUPTED_DATA);
			}
		}
	}

	template <class TSortCursor>
	void setRowRef(RowRef & row_ref, TSortCursor & cursor)
	{
		row_ref.row_num = cursor.impl->pos;
		row_ref.shared_block = source_blocks[cursor.impl->order];

		for (size_t i = 0; i < num_columns; ++i)
			row_ref.columns[i] = cursor->all_columns[i];
	}

	template <class TSortCursor>
	void setPrimaryKeyRef(RowRef & row_ref, TSortCursor & cursor)
	{
		row_ref.row_num = cursor.impl->pos;
		row_ref.shared_block = source_blocks[cursor.impl->order];

		for (size_t i = 0; i < cursor->sort_columns_size; ++i)
			row_ref.columns[i] = cursor->sort_columns[i];
	}

private:

	/** Делаем поддержку двух разных курсоров - с Collation и без.
	 *  Шаблоны используем вместо полиморфных SortCursor'ов и вызовов виртуальных функций.
	 */
	template <typename TSortCursor>
	void initQueue(std::priority_queue<TSortCursor> & queue);

	template <typename TSortCursor>
	void merge(Block & merged_block, ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue);

	Logger * log = &Logger::get("MergingSortedBlockInputStream");

	/// Прочитали до конца.
	bool finished = false;
};

}
