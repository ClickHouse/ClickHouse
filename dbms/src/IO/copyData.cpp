#include <DB/IO/copyData.h>


namespace DB
{

void copyData(ReadBuffer & from, WriteBuffer & to)
{
	while (!from.eof())
	{
		to.write(from.buffer().begin(), from.buffer().end() - from.buffer().begin());
		from.position() = from.buffer().end();
	}
}
	
}
