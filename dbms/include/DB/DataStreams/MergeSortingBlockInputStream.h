#pragma once

#include <Yandex/logger_useful.h>

#include <DB/Core/SortDescription.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** Соединяет поток сортированных по отдельности блоков в сортированный целиком поток.
  */
class MergeSortingBlockInputStream : public IProfilingBlockInputStream
{
public:
	/// limit - если не 0, то можно выдать только первые limit строк в сортированном порядке.
	MergeSortingBlockInputStream(BlockInputStreamPtr input_, SortDescription & description_, size_t limit_ = 0)
		: description(description_), limit(limit_), has_been_read(false), log(&Logger::get("MergeSortingBlockInputStream"))
	{
		children.push_back(input_);
	}

	String getName() const { return "MergeSortingBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "MergeSorting(" << children.back()->getID();
		
		for (size_t i = 0; i < description.size(); ++i)
			res << ", " << description[i].getID();
		
		res << ")";
		return res.str();
	}

protected:
	Block readImpl();

private:
	SortDescription description;
	size_t limit;

	/// Всё было прочитано.
	bool has_been_read;

	Logger * log;
	
	/** Слить сразу много блоков с помощью priority queue. 
	  */
	Block merge(Blocks & blocks);
	
	typedef std::vector<SortCursorImpl> CursorImpls;
	
	/** Делаем поддержку двух разных курсоров - с Collation и без.
	 *  Шаблоны используем вместо полиморфных SortCursor'ов и вызовов виртуальных функций.
	 */
	template <typename TSortCursor>
	Block mergeImpl(Blocks & block, CursorImpls & cursors);
};

}
