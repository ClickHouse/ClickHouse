#include <DB/IO/ReadHelpers.h>
#include <DB/Interpreters/evaluateConstantExpression.h>
#include <DB/Interpreters/convertFieldToType.h>
#include <DB/Parsers/ExpressionListParsers.h>
#include <DB/DataStreams/ValuesRowInputStream.h>
#include <DB/Core/FieldVisitors.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
	extern const int CANNOT_PARSE_QUOTED_STRING;
	extern const int CANNOT_PARSE_DATE;
	extern const int CANNOT_PARSE_DATETIME;
	extern const int CANNOT_READ_ARRAY_FROM_TEXT;
	extern const int CANNOT_PARSE_DATE;
	extern const int SYNTAX_ERROR;
	extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}


ValuesRowInputStream::ValuesRowInputStream(ReadBuffer & istr_, const Block & sample_, const Context & context_)
	: istr(istr_), sample(sample_), context(context_)
{
	size_t columns = sample.columns();
	data_types.resize(columns);
	for (size_t i = 0; i < columns; ++i)
		data_types[i] = sample.getByPosition(i).type;
}


bool ValuesRowInputStream::read(Row & row)
{
	size_t size = data_types.size();
	row.resize(size);

	skipWhitespaceIfAny(istr);

	if (istr.eof() || *istr.position() == ';')
	{
		row.clear();
		return false;
	}

	/** Как правило, это обычный формат для потокового парсинга.
	  * Но в качестве исключения, поддерживается также обработка произвольных выражений вместо значений.
	  * Это очень неэффективно. Но если выражений нет, то оверхед отсутствует.
	  */
	ParserExpressionWithOptionalAlias parser(false);

	assertChar('(', istr);

	for (size_t i = 0; i < size; ++i)
	{
		skipWhitespaceIfAny(istr);

		char * prev_istr_position = istr.position();
		size_t prev_istr_bytes = istr.count() - istr.offset();

		try
		{
			data_types[i]->deserializeTextQuoted(row[i], istr);
			skipWhitespaceIfAny(istr);

			if (i != size - 1)
				assertChar(',', istr);
			else
				assertChar(')', istr);
		}
		catch (const Exception & e)
		{
			/** Обычный потоковый парсер не смог распарсить значение.
			  * Попробуем распарсить его SQL-парсером как константное выражение.
			  * Это исключительный случай.
			  */
			if (e.code() == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED
				|| e.code() == ErrorCodes::CANNOT_PARSE_QUOTED_STRING
				|| e.code() == ErrorCodes::CANNOT_PARSE_DATE
				|| e.code() == ErrorCodes::CANNOT_PARSE_DATETIME
				|| e.code() == ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT
				|| e.code() == ErrorCodes::CANNOT_PARSE_DATE)
			{
				/// TODO Работоспособность, если выражение не помещается целиком до конца буфера.

				/// Если начало значения уже не лежит в буфере.
				if (istr.count() - istr.offset() != prev_istr_bytes)
					throw;

				IParser::Pos pos = prev_istr_position;

				Expected expected = "";
				IParser::Pos max_parsed_pos = pos;

				ASTPtr ast;
				if (!parser.parse(pos, istr.buffer().end(), ast, max_parsed_pos, expected))
					throw Exception("Cannot parse expression of type " + data_types[i]->getName() + " here: "
						+ String(prev_istr_position, std::min(SHOW_CHARS_ON_SYNTAX_ERROR, istr.buffer().end() - prev_istr_position)),
						ErrorCodes::SYNTAX_ERROR);

				istr.position() = const_cast<char *>(max_parsed_pos);

				row[i] = convertFieldToType(evaluateConstantExpression(ast, context), *data_types[i]);

				/// TODO После добавления поддержки NULL, добавить сюда проверку на data type is nullable.
				if (row[i].isNull())
					throw Exception("Expression returns value " + apply_visitor(FieldVisitorToString(), row[i])
						+ ", that is out of range of type " + data_types[i]->getName()
						+ ", at: " + String(prev_istr_position, std::min(SHOW_CHARS_ON_SYNTAX_ERROR, istr.buffer().end() - prev_istr_position)),
						ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE);

				skipWhitespaceIfAny(istr);

				if (i != size - 1)
					assertChar(',', istr);
				else
					assertChar(')', istr);
			}
			else
				throw;
		}
	}

	skipWhitespaceIfAny(istr);
	if (!istr.eof() && *istr.position() == ',')
		++istr.position();

	return true;
}

}
