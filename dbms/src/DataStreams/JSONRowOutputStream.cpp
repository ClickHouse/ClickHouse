#include <DB/DataStreams/JSONRowOutputStream.h>

#include <DB/IO/WriteHelpers.h>


namespace DB
{

using Poco::SharedPtr;


JSONRowOutputStream::JSONRowOutputStream(WriteBuffer & ostr_, const Block & sample_)
	: ostr(ostr_), field_number(0), row_count(0)
{
	NamesAndTypesList columns(sample_.getColumnsList());
	fields.assign(columns.begin(), columns.end());
}


void JSONRowOutputStream::writePrefix()
{
	writeString("{\n", ostr);
	writeString("\t\"meta\":\n", ostr);
	writeString("\t[\n", ostr);
	
	for (size_t i = 0; i < fields.size(); ++i)
	{
		writeString("\t\t{\n", ostr);
		
		writeString("\t\t\t\"name\": ", ostr);
		writeDoubleQuotedString(fields[i].first, ostr);
		writeString(",\n", ostr);
		writeString("\t\t\t\"type\": ", ostr);
		writeDoubleQuotedString(fields[i].second->getName(), ostr);
		writeChar('\n', ostr);
		
		writeString("\t\t}", ostr);
		if (i + 1 < fields.size())
			writeChar(',', ostr);
		writeChar('\n', ostr);
	}
	
	writeString("\t],\n", ostr);
	writeChar('\n', ostr);
	writeString("\t\"data\":\n", ostr);
	writeString("\t[\n", ostr);
}


void JSONRowOutputStream::writeField(const Field & field)
{
	writeString("\t\t\t", ostr);
	writeDoubleQuotedString(fields[field_number].first, ostr);
	writeString(": ", ostr);
	fields[field_number].second->serializeTextQuoted(field, ostr);
	++field_number;
}


void JSONRowOutputStream::writeFieldDelimiter()
{
	writeString(",\n", ostr);
}


void JSONRowOutputStream::writeRowStartDelimiter()
{
	if (row_count > 0)
		writeString(",\n", ostr);		
	writeString("\t\t{\n", ostr);
}


void JSONRowOutputStream::writeRowEndDelimiter()
{
	writeChar('\n', ostr);
	writeString("\t\t}", ostr);
	field_number = 0;
	++row_count;
}


void JSONRowOutputStream::writeSuffix()
{
	writeChar('\n', ostr);
	writeString("\t],\n", ostr);
	writeChar('\n', ostr);
	writeString("\t\"rows\": ", ostr);
	writeIntText(row_count, ostr);
	writeChar('\n', ostr);
	writeString("}\n", ostr);
}

}
