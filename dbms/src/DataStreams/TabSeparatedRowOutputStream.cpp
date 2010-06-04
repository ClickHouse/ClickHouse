#include <DB/DataStreams/TabSeparatedRowOutputStream.h>

#include <DB/IO/WriteHelpers.h>


namespace DB
{

using Poco::SharedPtr;


TabSeparatedRowOutputStream::TabSeparatedRowOutputStream(WriteBuffer & ostr_, SharedPtr<DataTypes> data_types_)
	: ostr(ostr_), data_types(data_types_), field_number(0)
{
}


void TabSeparatedRowOutputStream::writeField(const Field & field)
{
	data_types->at(field_number)->serializeTextEscaped(field, ostr);
	++field_number;
}


void TabSeparatedRowOutputStream::writeFieldDelimiter()
{
	writeChar('\t', ostr);
}


void TabSeparatedRowOutputStream::writeRowEndDelimiter()
{
	writeChar('\n', ostr);
	field_number = 0;
}

}
