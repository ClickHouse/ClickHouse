#pragma once

#include <Poco/SharedPtr.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Реализует реляционную операцию LIMIT.
  */
class LimitBlockInputStream : public IProfilingBlockInputStream
{
public:
	LimitBlockInputStream(BlockInputStreamPtr input_, size_t limit_, size_t offset_ = 0);
	
	String getName() const { return "LimitBlockInputStream"; }

	BlockInputStreamPtr clone() { return new LimitBlockInputStream(input, limit, offset); }

protected:
	Block readImpl();

private:
	BlockInputStreamPtr input;
	size_t limit;
	size_t offset;
	size_t pos;
};

}
