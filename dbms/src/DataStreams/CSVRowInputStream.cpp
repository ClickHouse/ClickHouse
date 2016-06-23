#include <DB/IO/ReadHelpers.h>
#include <DB/IO/Operators.h>

#include <DB/DataStreams/verbosePrintString.h>
#include <DB/DataStreams/CSVRowInputStream.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int INCORRECT_DATA;
}


CSVRowInputStream::CSVRowInputStream(ReadBuffer & istr_, const Block & sample_, const char delimiter_, bool with_names_, bool with_types_)
	: istr(istr_), sample(sample_), delimiter(delimiter_), with_names(with_names_), with_types(with_types_)
{
	size_t columns = sample.columns();
	data_types.resize(columns);
	for (size_t i = 0; i < columns; ++i)
		data_types[i] = sample.getByPosition(i).type;
}


static void skipEndOfLine(ReadBuffer & istr)
{
	/// \n (Unix) или \r\n (DOS/Windows) или \n\r (Mac OS Classic)

	if (*istr.position() == '\n')
	{
		++istr.position();
		if (!istr.eof() && *istr.position() == '\r')
			++istr.position();
	}
	else if (*istr.position() == '\r')
	{
		++istr.position();
		if (!istr.eof() && *istr.position() == '\n')
			++istr.position();
		else
			throw Exception("Cannot parse CSV format: found \\r (CR) not followed by \\n (LF)."
				" Line must end by \\n (LF) or \\r\\n (CR LF) or \\n\\r.", ErrorCodes::INCORRECT_DATA);
	}
	else if (!istr.eof())
		throw Exception("Expected end of line", ErrorCodes::INCORRECT_DATA);
}


static void skipDelimiter(ReadBuffer & istr, const char delimiter, bool is_last_column)
{
	if (is_last_column)
	{
		if (istr.eof())
			return;

		/// поддерживаем лишний разделитель на конце строки
		if (*istr.position() == delimiter)
		{
			++istr.position();
			if (istr.eof())
				return;
		}

		skipEndOfLine(istr);
	}
	else
		assertChar(delimiter, istr);
}


/// Пропустить допустимые в CSV пробельные символы.
static inline void skipWhitespacesAndTabs(ReadBuffer & buf)
{
	while (!buf.eof()
			&& (*buf.position() == ' '
				|| *buf.position() == '\t'))
		++buf.position();
}


static void skipRow(ReadBuffer & istr, const char delimiter, size_t columns)
{
	String tmp;
	for (size_t i = 0; i < columns; ++i)
	{
		skipWhitespacesAndTabs(istr);
		readCSVString(tmp, istr);
		skipWhitespacesAndTabs(istr);

		skipDelimiter(istr, delimiter, i + 1 == columns);
	}
}


void CSVRowInputStream::readPrefix()
{
	/// In this format, we assume, that if first string field contain BOM as value, it will be written in quotes,
	///  so BOM at beginning of stream cannot be confused with BOM in first string value, and it is safe to skip it.
	skipBOMIfExists(istr);

	size_t columns = sample.columns();
	String tmp;

	if (with_names)
		skipRow(istr, delimiter, columns);

	if (with_types)
		skipRow(istr, delimiter, columns);
}


bool CSVRowInputStream::read(Block & block)
{
	updateDiagnosticInfo();

	size_t size = data_types.size();

	try
	{
		if (istr.eof())
			return false;

		for (size_t i = 0; i < size; ++i)
		{
			skipWhitespacesAndTabs(istr);
			data_types[i].get()->deserializeTextCSV(*block.unsafeGetByPosition(i).column.get(), istr, delimiter);
			skipWhitespacesAndTabs(istr);

			skipDelimiter(istr, delimiter, i + 1 == size);
		}
	}
	catch (Exception & e)
	{
		String verbose_diagnostic;
		{
			WriteBufferFromString diagnostic_out(verbose_diagnostic);
			printDiagnosticInfo(block, diagnostic_out);
		}

		e.addMessage("\n" + verbose_diagnostic);
		throw;
	}

	return true;
}


void CSVRowInputStream::printDiagnosticInfo(Block & block, WriteBuffer & out)
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
		if (!parseRowAndPrintDiagnosticInfo(block, out, max_length_of_column_name, max_length_of_data_type_name))
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
	parseRowAndPrintDiagnosticInfo(block, out, max_length_of_column_name, max_length_of_data_type_name);
	out << "\n";
}


bool CSVRowInputStream::parseRowAndPrintDiagnosticInfo(Block & block,
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
		auto curr_position = istr.position();
		std::exception_ptr exception;

		try
		{
			skipWhitespacesAndTabs(istr);
			prev_position = istr.position();
			data_types[i]->deserializeTextCSV(*block.getByPosition(i).column, istr, delimiter);
			curr_position = istr.position();
			skipWhitespacesAndTabs(istr);
		}
		catch (...)
		{
			exception = std::current_exception();
		}

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
			if (*curr_position != '\n' && *curr_position != '\r' && *curr_position != delimiter)
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

		/// Разделители
		if (i + 1 == size)
		{
			if (istr.eof())
				return false;

			/// поддерживаем лишний разделитель на конце строки
			if (*istr.position() == delimiter)
			{
				++istr.position();
				if (istr.eof())
					break;
			}

			if (!istr.eof() && *istr.position() != '\n' && *istr.position() != '\r')
			{
				out << "ERROR: There is no line feed. ";
				verbosePrintString(istr.position(), istr.position() + 1, out);
				out << " found instead.\n"
					" It's like your file has more columns than expected.\n"
					"And if your file have right number of columns, maybe it have unquoted string value with comma.\n";

				return false;
			}

			skipEndOfLine(istr);
		}
		else
		{
			try
			{
				assertChar(delimiter, istr);
			}
			catch (const DB::Exception &)
			{
				if (*istr.position() == '\n' || *istr.position() == '\r')
				{
					out << "ERROR: Line feed found where delimiter (" << delimiter << ") is expected."
						" It's like your file has less columns than expected.\n"
						"And if your file have right number of columns, maybe it have unescaped quotes in values.\n";
				}
				else
				{
					out << "ERROR: There is no delimiter (" << delimiter << "). ";
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
