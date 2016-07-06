#include <DB/DataStreams/SquashingBlockInputStream.h>


namespace DB
{

SquashingBlockInputStream::SquashingBlockInputStream(BlockInputStreamPtr & src, size_t min_block_size_rows, size_t min_block_size_bytes)
	: transform(min_block_size_rows, min_block_size_bytes)
{
	children.emplace_back(src);
}


Block SquashingBlockInputStream::readImpl()
{
	while (true)
	{
		SquashingTransform::Result result = transform.add(children[0]->read());
		if (result.ready)
			return result.block;
	}
}

}
