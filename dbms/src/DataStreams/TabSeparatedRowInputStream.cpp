#include <DB/IO/ReadHelpers.h>
#include <DB/IO/Operators.h>

#include <DB/DataStreams/TabSeparatedRowInputStream.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>


namespace DB
{

using Poco::SharedPtr;

TabSeparatedRowInputStream::TabSeparatedRowInputStream(ReadBuffer & istr_, const Block & sample_, bool with_names_, bool with_types_)
	: istr(istr_), sample(sample_), with_names(with_names_), with_types(with_types_)
{
	size_t columns = sample.columns();
	data_types.resize(columns);
	for (size_t i = 0; i < columns; ++i)
		data_types[i] = sample.getByPosition(i).type;
}


void TabSeparatedRowInputStream::readPrefix()
{
	size_t columns = sample.columns();
	String tmp;

	if (with_names)
	{
		for (size_t i = 0; i < columns; ++i)
		{
			readEscapedString(tmp, istr);
			assertString(i == columns - 1 ? "\n" : "\t", istr);
		}
	}

	if (with_types)
	{
		for (size_t i = 0; i < columns; ++i)
		{
			readEscapedString(tmp, istr);
			assertString(i == columns - 1 ? "\n" : "\t", istr);
		}
	}
}


/** Проверка на распространённый случай ошибки - использование Windows перевода строки.
  */
static void checkForCarriageReturn(ReadBuffer & istr)
{
	if (istr.position()[0] == '\r' || (istr.position() != istr.buffer().begin() && istr.position()[-1] == '\r'))
		throw Exception("\nYou have carriage return (\\r, 0x0D, ASCII 13) at end of first row."
			"\nIt's like your input data has DOS/Windows style line separators, that are illegal in TabSeparated format."
			" You must transform your file to Unix format."
			"\nBut if you really need carriage return at end of string value of last column, you need to escape it as \\r.",
			ErrorCodes::INCORRECT_DATA);
}


bool TabSeparatedRowInputStream::read(Row & row)
{
	updateDiagnosticInfo();

	size_t size = data_types.size();
	row.resize(size);

	try
	{
		for (size_t i = 0; i < size; ++i)
		{
			if (i == 0 && istr.eof())
			{
				row.clear();
				return false;
			}

			data_types[i]->deserializeTextEscaped(row[i], istr);

			/// пропускаем разделители
			if (i + 1 == size)
			{
				if (!istr.eof())
				{
					if (unlikely(row_num == 1))
						checkForCarriageReturn(istr);

					assertString("\n", istr);
				}
			}
			else
				assertString("\t", istr);
		}
	}
	catch (Exception & e)
	{
		String verbose_diagnostic;
		{
			WriteBufferFromString diagnostic_out(verbose_diagnostic);
			printDiagnosticInfo(diagnostic_out);
		}

		e.addMessage("\n" + verbose_diagnostic);
		throw;
	}

	return true;
}


void TabSeparatedRowInputStream::printDiagnosticInfo(WriteBuffer & out)
{
	/// Вывести подробную диагностику возможно лишь если последняя и предпоследняя строка ещё находятся в буфере для чтения.
	size_t bytes_read_at_start_of_buffer = istr.count() - istr.offset();
	if (bytes_read_at_start_of_buffer != bytes_read_at_start_of_buffer_on_prev_row)
	{
		out << "Could not print diagnostic info because two last rows aren't in buffer (rare case)\n";
		return;
	}

	size_t max_length_of_column_name = 0;
	for (size_t i = 0; i < sample.columns(); ++i)
		if (sample.getByPosition(i).name.size() > max_length_of_column_name)
			max_length_of_column_name = sample.getByPosition(i).name.size();

	size_t max_length_of_data_type_name = 0;
	for (size_t i = 0; i < sample.columns(); ++i)
		if (sample.getByPosition(i).type->getName().size() > max_length_of_data_type_name)
			max_length_of_data_type_name = sample.getByPosition(i).type->getName().size();

	/// Откатываем курсор для чтения на начало предыдущей или текущей строки и парсим всё заново. Но теперь выводим подробную информацию.

	if (pos_of_prev_row)
	{
		istr.position() = pos_of_prev_row;

		out << "\nRow " << (row_num - 1) << ":\n";
		if (!parseRowAndPrintDiagnosticInfo(out, max_length_of_column_name, max_length_of_data_type_name))
			return;
	}
	else
	{
		if (!pos_of_current_row)
		{
			out << "Could not print diagnostic info because parsing of data hasn't started.\n";
			return;
		}

		istr.position() = pos_of_current_row;
	}

	out << "\nRow " << row_num << ":\n";
	parseRowAndPrintDiagnosticInfo(out, max_length_of_column_name, max_length_of_data_type_name);
	out << "\n";
}


static void verbosePrintString(BufferBase::Position begin, BufferBase::Position end, WriteBuffer & out)
{
	if (end == begin)
	{
		out << "<EMPTY>";
		return;
	}

	out << "\"";

	for (auto pos = begin; pos < end; ++pos)
	{
		switch (*pos)
		{
			case '\0':
				out << "<ASCII NUL>";
				break;
			case '\b':
				out << "<BACKSPACE>";
				break;
			case '\f':
				out << "<FORM FEED>";
				break;
			case '\n':
				out << "<LINE FEED>";
				break;
			case '\r':
				out << "<CARRIAGE RETURN>";
				break;
			case '\t':
				out << "<TAB>";
				break;
			case '\\':
				out << "<BACKSLASH>";
				break;
			case '"':
				out << "<DOUBLE QUOTE>";
				break;
			case '\'':
				out << "<SINGLE QUOTE>";
				break;

			default:
			{
				if (*pos >= 0 && *pos < 32)
				{
					static const char * hex = "0123456789ABCDEF";
					out << "<0x" << hex[*pos / 16] << hex[*pos % 16] << ">";
				}
				else
					out << *pos;
			}
		}
	}

	out << "\"";
}


bool TabSeparatedRowInputStream::parseRowAndPrintDiagnosticInfo(
	WriteBuffer & out, size_t max_length_of_column_name, size_t max_length_of_data_type_name)
{
	size_t size = data_types.size();
	for (size_t i = 0; i < size; ++i)
	{
		if (i == 0 && istr.eof())
		{
			out << "<End of stream>\n";
			return false;
		}

		out << "Column " << i << ", " << std::string((i < 10 ? 2 : i < 100 ? 1 : 0), ' ')
			<< "name: " << sample.getByPosition(i).name << ", " << std::string(max_length_of_column_name - sample.getByPosition(i).name.size(), ' ')
			<< "type: " << data_types[i]->getName() << ", " << std::string(max_length_of_data_type_name - data_types[i]->getName().size(), ' ');

		auto prev_position = istr.position();
		std::exception_ptr exception;

		Field field;
		try
		{
			data_types[i]->deserializeTextEscaped(field, istr);
		}
		catch (...)
		{
			exception = std::current_exception();
		}

		auto curr_position = istr.position();

		if (curr_position < prev_position)
			throw Exception("Logical error: parsing is non-deterministic.", ErrorCodes::LOGICAL_ERROR);

		if (data_types[i]->isNumeric())
		{
			/// Пустая строка вместо числа.
			if (curr_position == prev_position)
			{
				out << "ERROR: text ";
				verbosePrintString(prev_position, std::min(prev_position + 10, istr.buffer().end()), out);
				out << " is not like " << data_types[i]->getName() << "\n";
				return false;
			}
		}

		out << "parsed text: ";
		verbosePrintString(prev_position, curr_position, out);

		if (exception)
		{
			if (data_types[i]->getName() == "DateTime")
				out << "ERROR: DateTime must be in YYYY-MM-DD hh:mm:ss or NNNNNNNNNN (unix timestamp, exactly 10 digits) format.\n";
			else if (data_types[i]->getName() == "Date")
				out << "ERROR: Date must be in YYYY-MM-DD format.\n";
			else
				out << "ERROR\n";
			return false;
		}

		out << "\n";

		if (data_types[i]->isNumeric())
		{
			if (*curr_position != '\n' && *curr_position != '\t')
			{
				out << "ERROR: garbage after " << data_types[i]->getName() << ": ";
				verbosePrintString(curr_position, std::min(curr_position + 10, istr.buffer().end()), out);
				out << "\n";

				if (data_types[i]->getName() == "DateTime")
					out << "ERROR: DateTime must be in YYYY-MM-DD hh:mm:ss or NNNNNNNNNN (unix timestamp, exactly 10 digits) format.\n";
				else if (data_types[i]->getName() == "Date")
					out << "ERROR: Date must be in YYYY-MM-DD format.\n";

				return false;
			}
		}

		if (   (typeid_cast<const DataTypeUInt8  *>(data_types[i].get()) && field.get<UInt64>() > std::numeric_limits<UInt8>::max())
			|| (typeid_cast<const DataTypeUInt16 *>(data_types[i].get()) && field.get<UInt64>() > std::numeric_limits<UInt16>::max())
			|| (typeid_cast<const DataTypeUInt32 *>(data_types[i].get()) && field.get<UInt64>() > std::numeric_limits<UInt32>::max())
			|| (typeid_cast<const DataTypeInt8 *>(data_types[i].get())
				&& (field.get<Int64>() > std::numeric_limits<Int8>::max() || field.get<Int64>() < std::numeric_limits<Int8>::min()))
			|| (typeid_cast<const DataTypeInt16 *>(data_types[i].get())
				&& (field.get<Int64>() > std::numeric_limits<Int16>::max() || field.get<Int64>() < std::numeric_limits<Int16>::min()))
			|| (typeid_cast<const DataTypeInt32 *>(data_types[i].get())
				&& (field.get<Int64>() > std::numeric_limits<Int32>::max() || field.get<Int64>() < std::numeric_limits<Int32>::min())))
		{
			out << "ERROR: parsed number is out of range of data type.\n";
			return false;
		}

		/// Разделители
		if (i + 1 == size)
		{
			if (!istr.eof())
			{
				try
				{
					assertString("\n", istr);
				}
				catch (const DB::Exception &)
				{
					if (*istr.position() == '\t')
					{
						out << "ERROR: Tab found where line feed is expected."
							" It's like your file has more columns than expected.\n"
							"And if your file have right number of columns, maybe it have unescaped tab in value.\n";
					}
					else if (*istr.position() == '\r')
					{
						out << "ERROR: Carriage return found where line feed is expected."
							" It's like your file has DOS/Windows style line separators, that is illegal in TabSeparated format.\n";
					}
					else
					{
						out << "ERROR: There is no line feed. ";
						verbosePrintString(istr.position(), istr.position() + 1, out);
						out << " found instead.\n";
					}
					return false;
				}
			}
		}
		else
		{
			try
			{
				assertString("\t", istr);
			}
			catch (const DB::Exception &)
			{
				if (*istr.position() == '\n')
				{
					out << "ERROR: Line feed found where tab is expected."
						" It's like your file has less columns than expected.\n"
						"And if your file have right number of columns, maybe it have unescaped backslash in value before tab, which cause tab has escaped.\n";
				}
				else if (*istr.position() == '\r')
				{
					out << "ERROR: Carriage return found where tab is expected.\n";
				}
				else
				{
					out << "ERROR: There is no tab. ";
					verbosePrintString(istr.position(), istr.position() + 1, out);
					out << " found instead.\n";
				}
				return false;
			}
		}
	}

	return true;
}

}
