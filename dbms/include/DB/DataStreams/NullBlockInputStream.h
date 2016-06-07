#pragma once

#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{

/** Пустой поток блоков.
  */
class NullBlockInputStream : public IBlockInputStream
{
public:
	Block read() override { return Block(); }
	String getName() const override { return "Null"; }

	String getID() const override
	{
		std::stringstream res;
		res << this;
		return res.str();
	}
};

}
