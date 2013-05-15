#include <DB/DataStreams/ValuesRowOutputStream.h>

#include <DB/IO/WriteHelpers.h>


namespace DB
{

using Poco::SharedPtr;


ValuesRowOutputStream::ValuesRowOutputStream(WriteBuffer & ostr_, const Block & sample_)
	: ostr(ostr_), sample(sample_), field_number(0)
{
	size_t columns = sample.columns();
	data_types.resize(columns);
	for (size_t i = 0; i < columns; ++i)
		data_types[i] = sample.getByPosition(i).type;
}


void ValuesRowOutputStream::writeField(const Field & field)
{
	data_types[field_number]->serializeTextQuoted(field, ostr);
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
	writeCString(",", ostr);
	field_number = 0;
}


}
