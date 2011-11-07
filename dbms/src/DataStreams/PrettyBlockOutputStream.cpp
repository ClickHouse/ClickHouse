#include <DB/Functions/FunctionsMiscellaneous.h>

#include <DB/DataStreams/PrettyBlockOutputStream.h>


namespace DB
{

void PrettyBlockOutputStream::write(const Block & block_)
{
	/// Будем вставлять суда столбцы с вычисленными значениями видимых длин.
	Block block = block_;
	
	size_t rows = block.rows();
	size_t columns = block.columns();

	total_rows += rows;

	/// Вычислим ширину всех значений
	FunctionVisibleWidth visible_width_func;
	DataTypePtr visible_width_type = new DataTypeUInt64;

	typedef std::vector<size_t> Widths_t;
	Widths_t max_widths(columns);
	Widths_t name_widths(columns);

	for (size_t i = 0; i < columns; ++i)
	{
		ColumnWithNameAndType column;
		column.type = visible_width_type;
		column.name = "visibleWidth(" + block.getByPosition(i).name + ")";

		size_t result_number = block.columns();
		block.insert(column);

		ColumnNumbers arguments;
		arguments.push_back(i);

		visible_width_func.execute(block, arguments, result_number);

		column.column = block.getByPosition(i + columns).column;

		if (const ColumnUInt64 * col = dynamic_cast<const ColumnUInt64 *>(&*column.column))
		{
			const ColumnUInt64::Container_t res = col->getData();
			for (size_t j = 0; j < rows; ++j)
				if (res[j] > max_widths[i])
					max_widths[i] = res[j];
		}
		else if (const ColumnConstUInt64 * col = dynamic_cast<const ColumnConstUInt64 *>(&*column.column))
		{
			UInt64 res = col->getData();
			max_widths[i] = res;
		}
		else
			throw Exception("Illegal column " + column.column->getName() 
				+ " of result of function " + visible_width_func.getName(),
				ErrorCodes::ILLEGAL_COLUMN);

		/// И не только значений, но и их имён
		stringWidthConstant(block.getByPosition(i).name, name_widths[i]);
		if (name_widths[i] > max_widths[i])
			max_widths[i] = name_widths[i];
	}

	/// Создадим разделители
	std::stringstream top_separator;
	std::stringstream middle_names_separator;
	std::stringstream middle_values_separator;
	std::stringstream bottom_separator;

	top_separator 			<< "┏";
	middle_names_separator	<< "┡";
	middle_values_separator	<< "├";
	bottom_separator 		<< "└";
	for (size_t i = 0; i < columns; ++i)
	{
		if (i != 0)
		{
			top_separator 			<< "┳";
			middle_names_separator	<< "╇";
			middle_values_separator	<< "┼";
			bottom_separator 		<< "┴";
		}

		for (size_t j = 0; j < max_widths[i] + 2; ++j)
		{
			top_separator 			<< "━";
			middle_names_separator	<< "━";
			middle_values_separator	<< "─";
			bottom_separator 		<< "─";
		}
	}
	top_separator 			<< "┓\n";
	middle_names_separator	<< "┩\n";
	middle_values_separator	<< "┤\n";
	bottom_separator 		<< "┘\n";

	std::string top_separator_s = top_separator.str();
	std::string middle_names_separator_s = middle_names_separator.str();
	std::string middle_values_separator_s = middle_values_separator.str();
	std::string bottom_separator_s = bottom_separator.str();

	/// Выведем блок
	writeString(top_separator_s, ostr);

	/// Имена
	writeString("┃ ", ostr);
	for (size_t i = 0; i < columns; ++i)
	{
		if (i != 0)
			writeString(" ┃ ", ostr);

		const ColumnWithNameAndType & col = block.getByPosition(i);

		writeString("\033[1;37m", ostr);
		if (col.type->isNumeric())
		{
			for (size_t k = 0; k < max_widths[i] - name_widths[i]; ++k)
				writeChar(' ', ostr);

			writeEscapedString(col.name, ostr);
		}
		else
		{
			writeEscapedString(col.name, ostr);

			for (size_t k = 0; k < max_widths[i] - name_widths[i]; ++k)
				writeChar(' ', ostr);
		}
		writeString("\033[0m", ostr);
	}
	writeString(" ┃\n", ostr);

	writeString(middle_names_separator_s, ostr);

	for (size_t i = 0; i < rows; ++i)
	{
		if (i != 0)
			writeString(middle_values_separator_s, ostr);

		writeString("│ ", ostr);

		for (size_t j = 0; j < columns; ++j)
		{
			if (j != 0)
				writeString(" │ ", ostr);

			const ColumnWithNameAndType & col = block.getByPosition(j);

			if (col.type->isNumeric())
			{
				size_t width = boost::get<UInt64>((*block.getByPosition(columns + j).column)[i]);
				for (size_t k = 0; k < max_widths[j] - width; ++k)
					writeChar(' ', ostr);
					
				col.type->serializeTextEscaped((*col.column)[i], ostr);
			}
			else
			{
				col.type->serializeTextEscaped((*col.column)[i], ostr);

				size_t width = boost::get<UInt64>((*block.getByPosition(columns + j).column)[i]);
				for (size_t k = 0; k < max_widths[j] - width; ++k)
					writeChar(' ', ostr);
			}
		}

		writeString(" │\n", ostr);
	}
	
	writeString(bottom_separator_s, ostr);

	writeString("  ", ostr);
	writeIntText(total_rows, ostr);
	writeString(" rows in set.\n", ostr);
}

}
