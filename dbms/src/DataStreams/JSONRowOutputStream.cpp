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
	writeCString("{\n", ostr);
	writeCString("\t\"meta\":\n", ostr);
	writeCString("\t[\n", ostr);
	
	for (size_t i = 0; i < fields.size(); ++i)
	{
		writeCString("\t\t{\n", ostr);
		
		writeCString("\t\t\t\"name\": ", ostr);
		writeDoubleQuotedString(fields[i].first, ostr);
		writeCString(",\n", ostr);
		writeCString("\t\t\t\"type\": ", ostr);
		writeDoubleQuotedString(fields[i].second->getName(), ostr);
		writeChar('\n', ostr);
		
		writeCString("\t\t}", ostr);
		if (i + 1 < fields.size())
			writeChar(',', ostr);
		writeChar('\n', ostr);
	}
	
	writeCString("\t],\n", ostr);
	writeChar('\n', ostr);
	writeCString("\t\"data\":\n", ostr);
	writeCString("\t[\n", ostr);
}


void JSONRowOutputStream::writeField(const Field & field)
{
	writeCString("\t\t\t", ostr);
	writeDoubleQuotedString(fields[field_number].first, ostr);
	writeCString(": ", ostr);
	fields[field_number].second->serializeTextQuoted(field, ostr);
	++field_number;
}


void JSONRowOutputStream::writeFieldDelimiter()
{
	writeCString(",\n", ostr);
}


void JSONRowOutputStream::writeRowStartDelimiter()
{
	if (row_count > 0)
		writeCString(",\n", ostr);
	writeCString("\t\t{\n", ostr);
}


void JSONRowOutputStream::writeRowEndDelimiter()
{
	writeChar('\n', ostr);
	writeCString("\t\t}", ostr);
	field_number = 0;
	++row_count;
}


void JSONRowOutputStream::writeSuffix()
{
	writeChar('\n', ostr);
	writeCString("\t],\n", ostr);
	writeChar('\n', ostr);
	writeCString("\t\"rows\": ", ostr);
	writeIntText(row_count, ostr);
	writeChar('\n', ostr);
	writeCString("}\n", ostr);
}

}
