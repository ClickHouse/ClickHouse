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

	String getID() const
	{
		std::stringstream res;
		res << "Limit(" << children.back()->getID() << ", " << limit << ", " << offset << ")";
		return res.str();
	}

protected:
	Block readImpl();

private:
	size_t limit;
	size_t offset;
	size_t pos;
};

}
