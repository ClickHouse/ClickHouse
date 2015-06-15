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

	String getName() const override { return "Limit"; }

	String getID() const override
	{
		std::stringstream res;
		res << "Limit(" << children.back()->getID() << ", " << limit << ", " << offset << ")";
		return res.str();
	}

protected:
	Block readImpl() override;

private:
	size_t limit;
	size_t offset;
	size_t pos;
};

}
