#pragma once

#include <DB/DataStreams/IBlockOutputStream.h>


namespace DB
{


/** При попытке записать в этот поток блоков, кидает исключение.
  * Используется там, где, в общем случае, нужно передать поток блоков, но в некоторых случаях, он не должен быть использован.
  */
class EmptyBlockOutputStream : public IBlockOutputStream
{
public:
	void write(const Block & block) override
	{
		throw Exception("Cannot write to EmptyBlockOutputStream", ErrorCodes::CANNOT_WRITE_TO_EMPTY_BLOCK_OUTPUT_STREAM);
	}
};

}
