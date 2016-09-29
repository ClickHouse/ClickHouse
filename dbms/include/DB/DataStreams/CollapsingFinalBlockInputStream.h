#pragma once
#include <common/logger_useful.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Core/SortDescription.h>
#include <DB/Columns/ColumnsNumber.h>
#include <queue>

namespace DB
{

/// Схлопывает одинаковые строки с противоположным знаком примерно как CollapsingSortedBlockInputStream.
/// Выдает строки в произвольном порядке (входные потоки по-прежнему должны быть упорядочены).
/// Выдает только строки с положительным знаком.
class CollapsingFinalBlockInputStream : public IProfilingBlockInputStream
{
public:
	CollapsingFinalBlockInputStream(BlockInputStreams inputs_, const SortDescription & description_,
									 const String & sign_column_name_)
		: description(description_), sign_column_name(sign_column_name_)
	{
		children.insert(children.end(), inputs_.begin(), inputs_.end());
	}

	~CollapsingFinalBlockInputStream();

	String getName() const override { return "CollapsingFinal"; }

	String getID() const override
	{
		std::stringstream res;
		res << "CollapsingFinal(inputs";

		for (size_t i = 0; i < children.size(); ++i)
			res << ", " << children[i]->getID();

		res << ", description";

		for (size_t i = 0; i < description.size(); ++i)
			res << ", " << description[i].getID();

		res << ", sign_column, " << sign_column_name << ")";
		return res.str();
	}

protected:
	Block readImpl() override;

private:
	struct MergingBlock;
	using BlockPlainPtrs = std::vector<MergingBlock*>;

	struct MergingBlock : boost::noncopyable
	{
		MergingBlock(Block block_,
					 size_t stream_index_,
					 const SortDescription & desc,
					 String sign_column_name,
					 BlockPlainPtrs * output_blocks)
			: block(block_), stream_index(stream_index_), output_blocks(output_blocks)
		{
			sort_columns.resize(desc.size());
			for (size_t i = 0; i < desc.size(); ++i)
			{
				size_t column_number = !desc[i].column_name.empty()
					? block.getPositionByName(desc[i].column_name)
					: desc[i].column_number;

				sort_columns[i] = block.getByPosition(column_number).column.get();
			}

			const IColumn * sign_icolumn = block.getByName(sign_column_name).column.get();

			sign_column = typeid_cast<const ColumnInt8 *>(sign_icolumn);

			if (!sign_column)
				throw Exception("Sign column must have type Int8", ErrorCodes::BAD_TYPE_OF_FIELD);

			rows = sign_column->size();
			/// Заполняется целиком нулями. Потом выставляются единички в позициях строчек, которых нужно оставить.
			filter.resize_fill(rows);
		}

		Block block;

		/// Строки с одинаковым ключом будут упорядочены по возрастанию stream_index.
		size_t stream_index;
		size_t rows;

		/// Какие строки нужно оставить. Заполняется при слиянии потоков.
		IColumn::Filter filter;

		/// Указывают в block.
		ConstColumnPlainPtrs sort_columns;
		const ColumnInt8 * sign_column;

		/// Когда достигает нуля, блок можно выдавать в ответ.
		int refcount = 0;

		/// Куда положить блок, когда он готов попасть в ответ.
		BlockPlainPtrs * output_blocks;
	};

	/// При удалении последней ссылки на блок, добавляет блок в output_blocks.
	class MergingBlockPtr
	{
	public:
		MergingBlockPtr() : ptr() {}

		explicit MergingBlockPtr(MergingBlock * ptr_) : ptr(ptr_)
		{
			if (ptr)
				++ptr->refcount;
		}

		MergingBlockPtr(const MergingBlockPtr & rhs) : ptr(rhs.ptr)
		{
			if (ptr)
				++ptr->refcount;
		}

		MergingBlockPtr & operator=(const MergingBlockPtr & rhs)
		{
			destroy();
			ptr = rhs.ptr;
			if (ptr)
				++ptr->refcount;
			return *this;
		}

		~MergingBlockPtr()
		{
			destroy();
		}

		/// Обнулить указатель и не добавлять блок в output_blocks.
		void cancel()
		{
			if (ptr)
			{
				--ptr->refcount;
				if (!ptr->refcount)
					delete ptr;
				ptr = nullptr;
			}
		}

		MergingBlock & operator*() const { return *ptr; }
		MergingBlock * operator->() const { return ptr; }
		operator bool() const { return !!ptr; }
		bool operator!() const { return !ptr; }

	private:
		MergingBlock * ptr;

		void destroy()
		{
			if (ptr)
			{
				--ptr->refcount;
				if (!ptr->refcount)
				{
					if (std::uncaught_exception())
						delete ptr;
					else
						ptr->output_blocks->push_back(ptr);
				}
				ptr = nullptr;
			}
		}
	};

	struct Cursor
	{
		MergingBlockPtr block;
		size_t pos;

		Cursor() {}
		explicit Cursor(MergingBlockPtr block_, size_t pos_ = 0) : block(block_), pos(pos_) {}

		bool operator< (const Cursor & rhs) const
		{
			for (size_t i = 0; i < block->sort_columns.size(); ++i)
			{
				int res = block->sort_columns[i]->compareAt(pos, rhs.pos, *(rhs.block->sort_columns[i]), 1);
				if (res > 0)
					return true;
				if (res < 0)
					return false;
			}

			return block->stream_index > rhs.block->stream_index;
		}

		/// Не согласован с operator< : не учитывает order.
		bool equal(const Cursor & rhs) const
		{
			if (!block || !rhs.block)
				return false;

			for (size_t i = 0; i < block->sort_columns.size(); ++i)
			{
				int res = block->sort_columns[i]->compareAt(pos, rhs.pos, *(rhs.block->sort_columns[i]), 1);
				if (res != 0)
					return false;
			}

			return true;
		}

		Int8 getSign()
		{
			return block->sign_column->getData()[pos];
		}

		/// Помечает, что эту строку нужно взять в ответ.
		void addToFilter()
		{
			block->filter[pos] = 1;
		}

		bool isLast()
		{
			return pos + 1 == block->rows;
		}

		void next()
		{
			++pos;
		}
	};

	using Queue = std::priority_queue<Cursor>;

	const SortDescription description;
	String sign_column_name;

	Logger * log = &Logger::get("CollapsingFinalBlockInputStream");

	bool first = true;

	BlockPlainPtrs output_blocks;

	Queue queue;

	Cursor previous;		/// Текущий первичный ключ.
	Cursor last_positive;	/// Последняя положительная строка для текущего первичного ключа.

	size_t count_positive = 0;		/// Количество положительных строк для текущего первичного ключа.
	size_t count_negative = 0;		/// Количество отрицательных строк для текущего первичного ключа.
	bool last_is_positive = false;	/// true, если последняя строка для текущего первичного ключа положительная.

	size_t count_incorrect_data = 0;	/// Чтобы не писать в лог слишком много сообщений об ошибке.

	/// Посчитаем, сколько блоков получили на вход и отдали на выход.
	size_t blocks_fetched = 0;
	size_t blocks_output = 0;

	void fetchNextBlock(size_t input_index);
	void commitCurrent();

	void reportBadCounts();
	void reportBadSign(Int8 sign);
};

}
