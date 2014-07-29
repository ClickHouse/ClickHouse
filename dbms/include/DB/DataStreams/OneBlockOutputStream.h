#pragma once

#include <Poco/SharedPtr.h>

#include <DB/DataStreams/IBlockOutputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Поток блоков, конкатенирующий все записываемые в него блоки в один блок.
  */
class OneBlockOutputStream : public IBlockOutputStream
{
public:
	OneBlockOutputStream(const Block & block_) : block(block_), has_been_read(false) {}

	String getName() const { return "OneBlockOutputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << this;
		return res.str();
	}

	const Block & getBlock()
	{
		return block;
	}

	void write(const Block & new_block) override
	{
		if (!block)
		{
			block = new_block;
			return;
		}

		if (!blocksHaveEqualStructure(block, new_block))
			throw Exception("Blocks with different structure passed to OneBlockOutputStream", ErrorCodes::BLOCKS_HAS_DIFFERENT_STRUCTURE);

		size_t rows = new_block.rows();
		size_t columns = new_block.columns();

		for (size_t col = 0; col < columns; ++col)
		{
			IColumn * column = &*block.getByPosition(col).column;
			IColumn * new_column = &*new_block.getByPosition(col).column;

			for (size_t row = 0; row < rows; ++row)
			{
				column.insertFrom(new_column, row);
			}
		}
	}

private:
	Block block;
};

}
