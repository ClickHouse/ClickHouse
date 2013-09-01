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


void JSONCompactRowOutputStream::writeTotals()
{
	if (totals)
	{
		writeCString(",\n", ostr);
		writeChar('\n', ostr);
		writeCString("\t\"totals\": [", ostr);

		size_t totals_columns = totals.columns();
		for (size_t i = 0; i < totals_columns; ++i)
		{
			if (i != 0)
				writeChar(',', ostr);

			const ColumnWithNameAndType & column = totals.getByPosition(i);
			column.type->serializeTextJSON((*column.column)[0], ostr);
		}

		writeChar(']', ostr);
	}
}


}
