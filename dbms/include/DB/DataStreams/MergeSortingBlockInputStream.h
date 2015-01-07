#pragma once

#include <queue>

#include <Yandex/logger_useful.h>

#include <DB/Core/SortDescription.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** Соединяет поток сортированных по отдельности блоков в сортированный целиком поток.
  */

/** Часть реализации. Сливает набор готовых (уже прочитанных откуда-то) блоков.
  * Возвращает результат слияния в виде потока блоков не более max_merged_block_size строк.
  */
class MergeSortingBlocksBlockInputStream : public IProfilingBlockInputStream
{
public:
	/// limit - если не 0, то можно выдать только первые limit строк в сортированном порядке.
	MergeSortingBlocksBlockInputStream(Blocks & blocks_, SortDescription & description_,
		size_t max_merged_block_size_, size_t limit_ = 0);

	String getName() const override { return "MergeSortingBlocksBlockInputStream"; }
	String getID() const override { return getName(); }

protected:
	Block readImpl() override;

private:
	Blocks & blocks;
	SortDescription description;
	size_t max_merged_block_size;
	size_t limit;
	size_t total_merged_rows = 0;

	using CursorImpls = std::vector<SortCursorImpl>;
	CursorImpls cursors;

	bool has_collation = false;

	std::priority_queue<SortCursor> queue;
	std::priority_queue<SortCursorWithCollation> queue_with_collation;

	/** Делаем поддержку двух разных курсоров - с Collation и без.
	 *  Шаблоны используем вместо полиморфных SortCursor'ов и вызовов виртуальных функций.
	 */
	template <typename TSortCursor>
	Block mergeImpl(std::priority_queue<TSortCursor> & queue);
};


class MergeSortingBlockInputStream : public IProfilingBlockInputStream
{
public:
	/// limit - если не 0, то можно выдать только первые limit строк в сортированном порядке.
	MergeSortingBlockInputStream(BlockInputStreamPtr input_, SortDescription & description_, size_t limit_ = 0)
		: description(description_), limit(limit_)
	{
		children.push_back(input_);
	}

	String getName() const override { return "MergeSortingBlockInputStream"; }

	String getID() const override
	{
		std::stringstream res;
		res << "MergeSorting(" << children.back()->getID();

		for (size_t i = 0; i < description.size(); ++i)
			res << ", " << description[i].getID();

		res << ")";
		return res.str();
	}

protected:
	Block readImpl() override;

private:
	SortDescription description;
	size_t limit;

	Logger * log = &Logger::get("MergeSortingBlockInputStream");

	Blocks blocks;
	std::unique_ptr<MergeSortingBlocksBlockInputStream> impl;
};

}
