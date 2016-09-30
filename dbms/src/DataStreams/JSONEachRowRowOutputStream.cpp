#include <DB/IO/WriteHelpers.h>
#include <DB/IO/WriteBufferValidUTF8.h>
#include <DB/DataStreams/JSONEachRowRowOutputStream.h>


namespace DB
{


JSONEachRowRowOutputStream::JSONEachRowRowOutputStream(WriteBuffer & ostr_, const Block & sample, bool force_quoting_)
	: ostr(ostr_), force_quoting(force_quoting_)
{
	size_t columns = sample.columns();
	fields.resize(columns);

	for (size_t i = 0; i < columns; ++i)
	{
		WriteBufferFromString out(fields[i]);
		writeJSONString(sample.unsafeGetByPosition(i).name, out);
	}
}


void JSONEachRowRowOutputStream::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
	writeString(fields[field_number], ostr);
	writeChar(':', ostr);
	type.serializeTextJSON(column, row_num, ostr, force_quoting);
	++field_number;
}


void JSONEachRowRowOutputStream::writeFieldDelimiter()
{
	writeChar(',', ostr);
}


void JSONEachRowRowOutputStream::writeRowStartDelimiter()
{
	writeChar('{', ostr);
}


void JSONEachRowRowOutputStream::writeRowEndDelimiter()
{
	writeCString("}\n", ostr);
	field_number = 0;
}

}
