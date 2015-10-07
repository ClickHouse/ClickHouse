#include <DB/Functions/FunctionsMiscellaneous.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/DataStreams/VerticalRowOutputStream.h>


namespace DB
{

using Poco::SharedPtr;


VerticalRowOutputStream::VerticalRowOutputStream(WriteBuffer & ostr_, const Block & sample_)
	: ostr(ostr_), sample(sample_), field_number(0), row_number(0)
{
	size_t columns = sample.columns();
	data_types.resize(columns);
	names.resize(columns);

	typedef std::vector<size_t> Widths_t;
	Widths_t name_widths(columns);
	size_t max_name_width = 0;

	for (size_t i = 0; i < columns; ++i)
	{
		data_types[i] = sample.getByPosition(i).type;
		names[i] = sample.getByPosition(i).name;
		stringWidthConstant(names[i], name_widths[i]);
		if (name_widths[i] > max_name_width)
			max_name_width = name_widths[i];
	}

	pads.resize(columns);
	for (size_t i = 0; i < columns; ++i)
		pads[i] = String(max_name_width - name_widths[i], ' ');
}


void VerticalRowOutputStream::writeField(const Field & field)
{
	writeEscapedString(names[field_number], ostr);
	writeCString(": ", ostr);
	writeString(pads[field_number], ostr);

	writeValue(field);

	writeChar('\n', ostr);
	++field_number;
}


void VerticalRowOutputStream::writeValue(const Field & field) const
{
	data_types[field_number]->serializeTextEscaped(field, ostr);
}

void VerticalRawRowOutputStream::writeValue(const Field & field) const
{
	data_types[field_number]->serializeText(field, ostr);
}


void VerticalRowOutputStream::writeRowStartDelimiter()
{
	++row_number;
	writeCString("Row ", ostr);
	writeIntText(row_number, ostr);
	writeCString(":\n", ostr);

	size_t width = log10(row_number + 1) + 1 + strlen("Row :");
	for (size_t i = 0; i < width; ++i)
		writeCString("─", ostr);
	writeChar('\n', ostr);
}


void VerticalRowOutputStream::writeRowBetweenDelimiter()
{
	writeCString("\n", ostr);
	field_number = 0;
}


}
