#include <DB/Core/Block.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/DataStreams/BinaryRowInputStream.h>


namespace DB
{

BinaryRowInputStream::BinaryRowInputStream(ReadBuffer & istr_)
	: istr(istr_)
{
}


bool BinaryRowInputStream::read(Block & block)
{
	if (istr.eof())
		return false;

	size_t columns = block.columns();
	for (size_t i = 0; i < columns; ++i)
		block.unsafeGetByPosition(i).type.get()->deserializeBinary(*block.unsafeGetByPosition(i).column.get(), istr);

	return true;
}

}
