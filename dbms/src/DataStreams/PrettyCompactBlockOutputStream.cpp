#include <sys/ioctl.h>
#include <unistd.h>

#include <DB/Functions/FunctionsMiscellaneous.h>

#include <DB/DataStreams/PrettyCompactBlockOutputStream.h>


namespace DB
{


void PrettyCompactBlockOutputStream::write(const Block & block_)
{
	if (total_rows >= max_rows)
	{
		total_rows += block_.rows();
		return;
	}
	
	/// Будем вставлять суда столбцы с вычисленными значениями видимых длин.
	Block block = block_;
	
	size_t rows = block.rows();
	size_t columns = block.columns();

	Widths_t max_widths;
	Widths_t name_widths;
	calculateWidths(block, max_widths, name_widths);

	/// Создадим разделители
	std::stringstream bottom_separator;

	bottom_separator 		<< "└";
	for (size_t i = 0; i < columns; ++i)
	{
		if (i != 0)
			bottom_separator 		<< "┴";

		for (size_t j = 0; j < max_widths[i] + 2; ++j)
			bottom_separator 		<< "─";
	}
	bottom_separator 		<< "┘\n";

	std::string bottom_separator_s = bottom_separator.str();

	/// Имена
	writeString("┌─", ostr);
	for (size_t i = 0; i < columns; ++i)
	{
		if (i != 0)
			writeString("─┬─", ostr);

		const ColumnWithNameAndType & col = block.getByPosition(i);

		if (col.type->isNumeric())
		{
			for (size_t k = 0; k < max_widths[i] - name_widths[i]; ++k)
				writeString("─", ostr);

			if (!no_escapes)
				writeString("\033[1;37m", ostr);
			writeEscapedString(col.name, ostr);
			if (!no_escapes)
				writeString("\033[0m", ostr);
		}
		else
		{
			if (!no_escapes)
				writeString("\033[1;37m", ostr);
			writeEscapedString(col.name, ostr);
			if (!no_escapes)
				writeString("\033[0m", ostr);

			for (size_t k = 0; k < max_widths[i] - name_widths[i]; ++k)
				writeString("─", ostr);
		}
	}
	writeString("─┐\n", ostr);

	for (size_t i = 0; i < rows && total_rows + i < max_rows; ++i)
	{
		writeString("│ ", ostr);

		for (size_t j = 0; j < columns; ++j)
		{
			if (j != 0)
				writeString(" │ ", ostr);

			const ColumnWithNameAndType & col = block.getByPosition(j);

			if (col.type->isNumeric())
			{
				size_t width = get<UInt64>((*block.getByPosition(columns + j).column)[i]);
				for (size_t k = 0; k < max_widths[j] - width; ++k)
					writeChar(' ', ostr);
					
				col.type->serializeTextEscaped((*col.column)[i], ostr);
			}
			else
			{
				col.type->serializeTextEscaped((*col.column)[i], ostr);

				size_t width = get<UInt64>((*block.getByPosition(columns + j).column)[i]);
				for (size_t k = 0; k < max_widths[j] - width; ++k)
					writeChar(' ', ostr);
			}
		}

		writeString(" │\n", ostr);
	}

	writeString(bottom_separator_s, ostr);

	total_rows += rows;
}

}
