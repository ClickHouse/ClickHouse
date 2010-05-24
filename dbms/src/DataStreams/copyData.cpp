#include <DB/DataStreams/RowInputStreamFromBlockInputStream.h>
#include <DB/DataStreams/BlockInputStreamFromRowInputStream.h>

#include <DB/DataStreams/copyData.h>


namespace DB
{

void copyData(IBlockInputStream & from, IBlockOutputStream & to)
{
	while (Block block = from.read())
		to.write(block);
}


void copyData(IRowInputStream & from, IRowOutputStream & to)
{
	while (1)
	{
		Row row = from.read();
		if (row.empty())
			break;
		to.write(row);
	}
}


void copyData(IBlockInputStream & from, IRowOutputStream & to)
{
	RowInputStreamFromBlockInputStream row_input(from);
	copyData(row_input, to);
}


void copyData(IRowInputStream & from, IBlockOutputStream & to, const Block & sample)
{
	BlockInputStreamFromRowInputStream block_input(from, sample);
	copyData(block_input, to);
}

}
