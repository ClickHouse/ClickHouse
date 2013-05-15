#include <DB/DataStreams/JSONCompactRowOutputStream.h>

#include <DB/IO/WriteHelpers.h>


namespace DB
{

using Poco::SharedPtr;


JSONCompactRowOutputStream::JSONCompactRowOutputStream(WriteBuffer & ostr_, const Block & sample_)
	: JSONRowOutputStream(ostr_, sample_)
{
}


void JSONCompactRowOutputStream::writeField(const Field & field)
{
	fields[field_number].second->serializeTextQuoted(field, ostr);
	++field_number;
}


void JSONCompactRowOutputStream::writeFieldDelimiter()
{
	writeString(", ", ostr);
}


void JSONCompactRowOutputStream::writeRowStartDelimiter()
{
	if (row_count > 0)
		writeString(",\n", ostr);
	writeString("\t\t\t[", ostr);
}


void JSONCompactRowOutputStream::writeRowEndDelimiter()
{
	writeChar(']', ostr);
	field_number = 0;
	++row_count;
}

}
