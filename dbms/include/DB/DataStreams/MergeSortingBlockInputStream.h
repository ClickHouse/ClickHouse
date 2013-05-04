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
	MergeSortingBlockInputStream(BlockInputStreamPtr input_, SortDescription & description_)
		: description(description_), has_been_read(false), log(&Logger::get("MergeSortingBlockInputStream"))
	{
		children.push_back(input_);
		input = &*children.back();
	}

	String getName() const { return "MergeSortingBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "MergeSorting(" << input->getID();
		
		for (size_t i = 0; i < description.size(); ++i)
			res << ", " << description[i].getID();
		
		res << ")";
		return res.str();
	}

protected:
	Block readImpl();

private:
	IBlockInputStream * input;
	SortDescription description;

	/// Всё было прочитано.
	bool has_been_read;

	Logger * log;
	
	/** Слить сразу много блоков с помощью priority queue. 
	  */
	Block merge(Blocks & blocks);
};

}
