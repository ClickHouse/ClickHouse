#pragma once

#include <DB/IO/IBlockInputStream.h>

namespace DB
{

class TypeCheckingBlockInputStream : public IBlockInputStream
{
public:
	TypeCheckingBlockInputStream(const BlockInputStreamPtr & source, Block sample_block)
	{

	}

};

}
