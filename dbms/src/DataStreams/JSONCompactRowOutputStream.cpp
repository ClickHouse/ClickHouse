#include <DB/DataStreams/JSONCompactRowOutputStream.h>

#include <DB/IO/WriteHelpers.h>


namespace DB
{

using Poco::SharedPtr;


JSONCompactRowOutputStream::JSONCompactRowOutputStream(WriteBuffer & ostr_, const Block & sample_, const BlockInputStreamPtr & input_stream_)
	: JSONRowOutputStream(ostr_, sample_, input_stream_)
{
}


void JSONCompactRowOutputStream::writeField(const Field & field)
{
	fields[field_number].second->serializeTextJSON(field, ostr);
	++field_number;
}


void JSONCompactRowOutputStream::writeFieldDelimiter()
{
	writeCString(", ", ostr);
}


void JSONCompactRowOutputStream::writeRowStartDelimiter()
{
	if (row_count > 0)
		writeCString(",\n", ostr);
	writeCString("\t\t[", ostr);
}


void JSONCompactRowOutputStream::writeRowEndDelimiter()
{
	writeChar(']', ostr);
	field_number = 0;
	++row_count;
}

}
