#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Объединяет несколько источников в один.
  * В отличие от UnionBlockInputStream, делает это последовательно.
  * Блоки разных источников не перемежаются друг с другом.
  */
class ConcatBlockInputStream : public IProfilingBlockInputStream
{
public:
	ConcatBlockInputStream(BlockInputStreams inputs_)
	{
		children.insert(children.end(), inputs_.begin(), inputs_.end());
		current_stream = children.begin();
	}

	String getName() const { return "ConcatBlockInputStream"; }

	BlockInputStreamPtr clone() { return new ConcatBlockInputStream(children); }

protected:
	Block readImpl()
	{
		Block res;

		while (current_stream != children.end())
		{
			res = (*current_stream)->read();

			if (res)
				break;
			else
				++current_stream;
		}

		return res;
	}

private:
	BlockInputStreams::iterator current_stream;
};

}
