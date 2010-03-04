#include <DB/DataStreams/TabSeparatedRowOutputStream.h>


namespace DB
{

using Poco::SharedPtr;


TabSeparatedRowOutputStream::TabSeparatedRowOutputStream(std::ostream & ostr_, SharedPtr<ColumnTypes> column_types_)
	: ostr(ostr_), column_types(column_types_), field_number(0)
{
}


void TabSeparatedRowOutputStream::writeField(const Field & field)
{
	column_types->at(field_number)->serializeTextEscaped(field, ostr);
	++field_number;
}


void TabSeparatedRowOutputStream::writeFieldDelimiter()
{
	ostr.put('\t');
}


void TabSeparatedRowOutputStream::writeRowEndDelimiter()
{
	ostr.put('\n');
	field_number = 0;
}

}
