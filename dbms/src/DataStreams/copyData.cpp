#include <DB/DataStreams/RowInputStreamFromBlockInputStream.h>
#include <DB/DataStreams/BlockInputStreamFromRowInputStream.h>

#include <DB/DataStreams/copyData.h>


namespace DB
{

void copyData(IBlockInputStream & from, IBlockOutputStream & to)
{
	from.readPrefix();
	to.writePrefix();

	while (Block block = from.read())
		to.write(block);

	from.readSuffix();
	to.writeSuffix();
}


void copyData(IRowInputStream & from, IRowOutputStream & to)
{
	from.readPrefix();
	to.writePrefix();

	bool first = true;
	while (1)
	{
		if (first)
			first = false;
		else
		{
			from.readRowBetweenDelimiter();
			to.writeRowBetweenDelimiter();
		}
		
		Row row;
		bool has_rows = from.read(row);
		if (!has_rows)
			break;
		to.write(row);
	}

	from.readSuffix();
	to.writeSuffix();
}

}
