#pragma once

#include <DB/Core/SortDescription.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** Сортирует каждый блок по отдельности по значениям указанных столбцов.
  * На данный момент, используется не очень оптимальный алгоритм.
  */
class PartialSortingBlockInputStream : public IProfilingBlockInputStream
{
public:
	PartialSortingBlockInputStream(BlockInputStreamPtr input_, SortDescription & description_)
		: input(input_), description(description_)
	{
		children.push_back(input);
	}

	String getName() const { return "PartialSortingBlockInputStream"; }

	BlockInputStreamPtr clone() { return new PartialSortingBlockInputStream(input, description); }

	String getID() const
	{
		std::stringstream res;
		res << "PartialSorting(" << input->getID();

		for (size_t i = 0; i < description.size(); ++i)
			res << ", " << description[i].getID();

		res << ")";
		return res.str();
	}

protected:
	Block readImpl();

private:
	BlockInputStreamPtr input;
	SortDescription description;
};

}
