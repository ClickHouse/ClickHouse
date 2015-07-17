#include <DB/IO/WriteHelpers.h>
#include <DB/IO/WriteBufferValidUTF8.h>
#include <DB/DataStreams/JSONRowOutputStream.h>


namespace DB
{

using Poco::SharedPtr;


JSONRowOutputStream::JSONRowOutputStream(WriteBuffer & ostr_, const Block & sample_)
	: dst_ostr(ostr_)
{
	NamesAndTypesList columns(sample_.getColumnsList());
	fields.assign(columns.begin(), columns.end());

	bool have_non_numeric_columns = false;
	for (size_t i = 0; i < sample_.columns(); ++i)
	{
		if (!sample_.unsafeGetByPosition(i).type->isNumeric())
		{
			have_non_numeric_columns = true;
			break;
		}
	}

	if (have_non_numeric_columns)
	{
		validating_ostr.reset(new WriteBufferValidUTF8(dst_ostr));
		ostr = validating_ostr.get();
	}
	else
		ostr = &dst_ostr;
}


void JSONRowOutputStream::writePrefix()
{
	writeCString("{\n", *ostr);
	writeCString("\t\"meta\":\n", *ostr);
	writeCString("\t[\n", *ostr);

	for (size_t i = 0; i < fields.size(); ++i)
	{
		writeCString("\t\t{\n", *ostr);

		writeCString("\t\t\t\"name\": ", *ostr);
		writeDoubleQuotedString(fields[i].name, *ostr);
		writeCString(",\n", *ostr);
		writeCString("\t\t\t\"type\": ", *ostr);
		writeDoubleQuotedString(fields[i].type->getName(), *ostr);
		writeChar('\n', *ostr);

		writeCString("\t\t}", *ostr);
		if (i + 1 < fields.size())
			writeChar(',', *ostr);
		writeChar('\n', *ostr);
	}

	writeCString("\t],\n", *ostr);
	writeChar('\n', *ostr);
	writeCString("\t\"data\":\n", *ostr);
	writeCString("\t[\n", *ostr);
}


void JSONRowOutputStream::writeField(const Field & field)
{
	writeCString("\t\t\t", *ostr);
	writeDoubleQuotedString(fields[field_number].name, *ostr);
	writeCString(": ", *ostr);
	fields[field_number].type->serializeTextJSON(field, *ostr);
	++field_number;
}


void JSONRowOutputStream::writeFieldDelimiter()
{
	writeCString(",\n", *ostr);
}


void JSONRowOutputStream::writeRowStartDelimiter()
{
	if (row_count > 0)
		writeCString(",\n", *ostr);
	writeCString("\t\t{\n", *ostr);
}


void JSONRowOutputStream::writeRowEndDelimiter()
{
	writeChar('\n', *ostr);
	writeCString("\t\t}", *ostr);
	field_number = 0;
	++row_count;
}


void JSONRowOutputStream::writeSuffix()
{
	writeChar('\n', *ostr);
	writeCString("\t]", *ostr);

	writeTotals();
	writeExtremes();

	writeCString(",\n\n", *ostr);
	writeCString("\t\"rows\": ", *ostr);
	writeIntText(row_count, *ostr);

	writeRowsBeforeLimitAtLeast();

	writeChar('\n', *ostr);
	writeCString("}\n", *ostr);
	ostr->next();
}

void JSONRowOutputStream::writeRowsBeforeLimitAtLeast()
{
	if (applied_limit)
	{
		writeCString(",\n\n", *ostr);
		writeCString("\t\"rows_before_limit_at_least\": ", *ostr);
		writeIntText(rows_before_limit, *ostr);
	}
}

void JSONRowOutputStream::writeTotals()
{
	if (totals)
	{
		writeCString(",\n", *ostr);
		writeChar('\n', *ostr);
		writeCString("\t\"totals\":\n", *ostr);
		writeCString("\t{\n", *ostr);

		size_t totals_columns = totals.columns();
		for (size_t i = 0; i < totals_columns; ++i)
		{
			const ColumnWithTypeAndName & column = totals.getByPosition(i);

			if (i != 0)
				writeCString(",\n", *ostr);

			writeCString("\t\t", *ostr);
			writeDoubleQuotedString(column.name, *ostr);
			writeCString(": ", *ostr);
			column.type->serializeTextJSON((*column.column)[0], *ostr);
		}

		writeChar('\n', *ostr);
		writeCString("\t}", *ostr);
	}
}


static void writeExtremesElement(const char * title, const Block & extremes, size_t row_num, WriteBuffer & ostr)
{
	writeCString("\t\t\"", ostr);
	writeCString(title, ostr);
	writeCString("\":\n", ostr);
	writeCString("\t\t{\n", ostr);

	size_t extremes_columns = extremes.columns();
	for (size_t i = 0; i < extremes_columns; ++i)
	{
		const ColumnWithTypeAndName & column = extremes.getByPosition(i);

		if (i != 0)
			writeCString(",\n", ostr);

		writeCString("\t\t\t", ostr);
		writeDoubleQuotedString(column.name, ostr);
		writeCString(": ", ostr);
		column.type->serializeTextJSON((*column.column)[row_num], ostr);
	}

	writeChar('\n', ostr);
	writeCString("\t\t}", ostr);
}

void JSONRowOutputStream::writeExtremes()
{
	if (extremes)
	{
		writeCString(",\n", *ostr);
		writeChar('\n', *ostr);
		writeCString("\t\"extremes\":\n", *ostr);
		writeCString("\t{\n", *ostr);

		writeExtremesElement("min", extremes, 0, *ostr);
		writeCString(",\n", *ostr);
		writeExtremesElement("max", extremes, 1, *ostr);

		writeChar('\n', *ostr);
		writeCString("\t}", *ostr);
	}
}

}
