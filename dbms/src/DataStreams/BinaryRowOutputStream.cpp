#include <DB/DataStreams/BinaryRowOutputStream.h>


namespace DB
{

BinaryRowOutputStream::BinaryRowOutputStream(WriteBuffer & ostr_)
	: ostr(ostr_)
{
}

void BinaryRowOutputStream::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
	type.serializeBinary(column, row_num, ostr);
}

}
