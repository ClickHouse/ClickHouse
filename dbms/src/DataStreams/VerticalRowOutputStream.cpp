#include <DB/Functions/FunctionsMiscellaneous.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/DataStreams/VerticalRowOutputStream.h>


namespace DB
{

VerticalRowOutputStream::VerticalRowOutputStream(WriteBuffer & ostr_, const Block & sample_)
	: ostr(ostr_), sample(sample_), field_number(0), row_number(0)
{
	size_t columns = sample.columns();
	names.resize(columns);

	using Widths_t = std::vector<size_t>;
	Widths_t name_widths(columns);
	size_t max_name_width = 0;

	for (size_t i = 0; i < columns; ++i)
	{
		names[i] = sample.getByPosition(i).name;
		stringWidthConstant(names[i], name_widths[i]);
		if (name_widths[i] > max_name_width)
			max_name_width = name_widths[i];
	}

	pads.resize(columns);
	for (size_t i = 0; i < columns; ++i)
		pads[i] = String(max_name_width - name_widths[i], ' ');
}


void VerticalRowOutputStream::flush()
{
	ostr.next();
}


void VerticalRowOutputStream::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
	writeEscapedString(names[field_number], ostr);
	writeCString(": ", ostr);
	writeString(pads[field_number], ostr);

	writeValue(column, type, row_num);

	writeChar('\n', ostr);
	++field_number;
}


void VerticalRowOutputStream::writeValue(const IColumn & column, const IDataType & type, size_t row_num) const
{
	type.serializeTextEscaped(column, row_num, ostr);
}

void VerticalRawRowOutputStream::writeValue(const IColumn & column, const IDataType & type, size_t row_num) const
{
	type.serializeText(column, row_num, ostr);
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
