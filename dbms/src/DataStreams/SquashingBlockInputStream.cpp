#include <DB/DataStreams/SquashingBlockInputStream.h>


namespace DB
{

SquashingBlockInputStream::SquashingBlockInputStream(BlockInputStreamPtr & src, size_t min_block_size_rows, size_t min_block_size_bytes)
	: min_block_size_rows(min_block_size_rows), min_block_size_bytes(min_block_size_bytes)
{
	children.emplace_back(src);
}


Block SquashingBlockInputStream::readImpl()
{
	if (all_read)
		return {};

	while (Block block = children[0]->read())
	{
		/// Just read block is alredy enough.
		if (isEnoughSize(block.rowsInFirstColumn(), block.bytes()))
		{
			/// If no accumulated data, return just read block.
			if (!accumulated_block)
				return block;

			/// Return accumulated data (may be it has small size) and place new block to accumulated data.
			accumulated_block.swap(block);
			return block;
		}

		/// Accumulated block is already enough.
		if (accumulated_block && isEnoughSize(accumulated_block.rowsInFirstColumn(), accumulated_block.bytes()))
		{
			/// Return accumulated data and place new block to accumulated data.
			accumulated_block.swap(block);
			return block;
		}

		append(std::move(block));

		if (isEnoughSize(accumulated_block.rowsInFirstColumn(), accumulated_block.bytes()))
		{
			Block res;
			res.swap(accumulated_block);
			return res;
		}
	}

	all_read = true;
	return accumulated_block;
}


void SquashingBlockInputStream::append(Block && block)
{
	if (!accumulated_block)
	{
		accumulated_block = std::move(block);
		return;
	}

	size_t columns = block.columns();
	size_t rows = block.rowsInFirstColumn();

	for (size_t i = 0; i < columns; ++i)
		accumulated_block.unsafeGetByPosition(i).column->insertRangeFrom(
			*block.unsafeGetByPosition(i).column, 0, rows);
}


bool SquashingBlockInputStream::isEnoughSize(size_t rows, size_t bytes) const
{
	return (!min_block_size_rows && !min_block_size_bytes)
		|| (min_block_size_rows && rows >= min_block_size_rows)
		|| (min_block_size_bytes && bytes >= min_block_size_bytes);
}

}
