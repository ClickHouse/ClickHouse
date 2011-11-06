#include <DB/DataStreams/TabSeparatedRowOutputStream.h>

#include <DB/IO/WriteHelpers.h>


namespace DB
{

using Poco::SharedPtr;


TabSeparatedRowOutputStream::TabSeparatedRowOutputStream(WriteBuffer & ostr_, const Block & sample_)
	: ostr(ostr_), sample(sample_), field_number(0)
{
	size_t columns = sample.columns();
	data_types.resize(columns);
	for (size_t i = 0; i < columns; ++i)
		data_types[i] = sample.getByPosition(i).type;
}


void TabSeparatedRowOutputStream::writeField(const Field & field)
{
	data_types[field_number]->serializeTextEscaped(field, ostr);
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
