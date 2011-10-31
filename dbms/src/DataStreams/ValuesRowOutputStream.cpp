#include <DB/DataStreams/ValuesRowOutputStream.h>

#include <DB/IO/WriteHelpers.h>


namespace DB
{

using Poco::SharedPtr;


ValuesRowOutputStream::ValuesRowOutputStream(WriteBuffer & ostr_, SharedPtr<DataTypes> data_types_)
	: ostr(ostr_), data_types(data_types_), field_number(0)
{
}


void ValuesRowOutputStream::writeField(const Field & field)
{
	data_types->at(field_number)->serializeTextQuoted(field, ostr);
	++field_number;
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
	writeString(",", ostr);
	field_number = 0;
}


}
