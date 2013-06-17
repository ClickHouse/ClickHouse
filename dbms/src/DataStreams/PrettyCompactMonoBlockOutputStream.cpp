#include <sys/ioctl.h>
#include <unistd.h>

#include <DB/Functions/FunctionsMiscellaneous.h>

#include <DB/DataStreams/PrettyCompactMonoBlockOutputStream.h>


namespace DB
{

void PrettyCompactMonoBlockOutputStream::write(const Block & block)
{
	if (total_rows < max_rows)
		blocks.push_back(block);
	
	total_rows += block.rows();
}

void PrettyCompactMonoBlockOutputStream::writeSuffix()
{
	if (blocks.empty())
		return;
	
	size_t columns = blocks.front().columns();
	
	Widths_t max_widths;
	Widths_t name_widths;
	
	for (size_t i = 0; i < blocks.size(); ++i)
	{
		/// Будем вставлять сюда столбцы с вычисленными значениями видимых длин.
		calculateWidths(blocks[i], max_widths, name_widths);
	}

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
	writeCString("┌─", ostr);
	for (size_t i = 0; i < columns; ++i)
	{
		if (i != 0)
			writeCString("─┬─", ostr);

		const ColumnWithNameAndType & col = blocks.front().getByPosition(i);

		if (col.type->isNumeric())
		{
			for (size_t k = 0; k < max_widths[i] - name_widths[i]; ++k)
				writeCString("─", ostr);

			if (!no_escapes)
				writeCString("\033[1;37m", ostr);
			writeEscapedString(col.name, ostr);
			if (!no_escapes)
				writeCString("\033[0m", ostr);
		}
		else
		{
			if (!no_escapes)
				writeCString("\033[1;37m", ostr);
			writeEscapedString(col.name, ostr);
			if (!no_escapes)
				writeCString("\033[0m", ostr);

			for (size_t k = 0; k < max_widths[i] - name_widths[i]; ++k)
				writeCString("─", ostr);
		}
	}
	writeCString("─┐\n", ostr);

	size_t row_count = 0;
	
	for (size_t block_id = 0; block_id < blocks.size() && row_count < max_rows; ++block_id)
	{
		const Block & block = blocks[block_id];
		size_t rows = block.rows();
		
		for (size_t i = 0; i < rows && row_count < max_rows; ++i)
		{
			writeCString("│ ", ostr);

			for (size_t j = 0; j < columns; ++j)
			{
				if (j != 0)
					writeCString(" │ ", ostr);

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

			writeCString(" │\n", ostr);
			
			++row_count;
		}
	}

	writeString(bottom_separator_s, ostr);
}

}
