#include <DB/IO/WriteBuffer.h>
#include <DB/Columns/IColumn.h>
#include <DB/DataTypes/IDataType.h>
#include <DB/DataStreams/BinaryRowOutputStream.h>


namespace DB
{

BinaryRowOutputStream::BinaryRowOutputStream(WriteBuffer & ostr_)
	: ostr(ostr_)
{
}

void BinaryRowOutputStream::flush()
{
	ostr.next();
}

void BinaryRowOutputStream::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
	type.serializeBinary(column, row_num, ostr);
}

}
