#pragma once

#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{

/** Пустой поток блоков.
  */
class NullBlockInputStream : public IBlockInputStream
{
public:
	Block read() { return Block(); }
	String getName() const { return "NullBlockInputStream"; }
	BlockInputStreamPtr clone() { return new NullBlockInputStream(); }

	String getID() const
	{
		std::stringstream res;
		res << this;
		return res.str();
	}
};

}
