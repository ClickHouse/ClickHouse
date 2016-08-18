#include <DB/DataStreams/ValuesRowOutputStream.h>

#include <DB/IO/WriteHelpers.h>
#include <DB/Columns/IColumn.h>
#include <DB/DataTypes/IDataType.h>


namespace DB
{


ValuesRowOutputStream::ValuesRowOutputStream(WriteBuffer & ostr_)
	: ostr(ostr_)
{
}

void ValuesRowOutputStream::flush()
{
	ostr.next();
}

void ValuesRowOutputStream::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
	type.serializeTextQuoted(column, row_num, ostr);
}

void ValuesRowOutputStream::writeFieldDelimiter()
{
	writeChar(',', ostr);
}

void ValuesRowOutputStream::writeRowStartDelimiter()
{
	writeChar('(', ostr);
}

void ValuesRowOutputStream::writeRowEndDelimiter()
{
	writeChar(')', ostr);
}

void ValuesRowOutputStream::writeRowBetweenDelimiter()
{
	writeCString(",", ostr);
}


}
