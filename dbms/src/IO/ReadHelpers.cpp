#if defined(__x86_64__)
	#include <emmintrin.h>
#endif

#include <sstream>

#include <mysqlxx/Manip.h>

#include <DB/Core/Defines.h>
#include <DB/Common/PODArray.h>
#include <DB/IO/ReadHelpers.h>
#include <common/find_first_symbols.h>

namespace DB
{

namespace ErrorCodes
{
	extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
	extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
	extern const int CANNOT_PARSE_QUOTED_STRING;
}


static void __attribute__((__noinline__)) throwAtAssertionFailed(const char * s, ReadBuffer & buf)
{
	std::stringstream message;
	message <<  "Cannot parse input: expected " << mysqlxx::escape << s;

	if (buf.eof())
		message << " at end of stream.";
	else
		message << " before: " << mysqlxx::escape << String(buf.position(), std::min(SHOW_CHARS_ON_SYNTAX_ERROR, buf.buffer().end() - buf.position()));

	throw Exception(message.str(), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
}


bool checkString(const char * s, ReadBuffer & buf)
{
	for (; *s; ++s)
	{
		if (buf.eof() || *buf.position() != *s)
			return false;
		++buf.position();
	}
	return true;
}

void assertString(const char * s, ReadBuffer & buf)
{
	if (!checkString(s, buf))
		throwAtAssertionFailed(s, buf);
}

void assertChar(char symbol, ReadBuffer & buf)
{
	if (buf.eof() || *buf.position() != symbol)
	{
		char err[2] = {symbol, '\0'};
		throwAtAssertionFailed(err, buf);
	}
	++buf.position();
}

void assertEOF(ReadBuffer & buf)
{
	if (!buf.eof())
		throwAtAssertionFailed("eof", buf);
}


template <typename T>
static void appendToStringOrVector(T & s, const char * begin, const char * end);

template <>
inline void appendToStringOrVector(String & s, const char * begin, const char * end)
{
	s.append(begin, end - begin);
}

template <>
inline void appendToStringOrVector(PODArray<UInt8> & s, const char * begin, const char * end)
{
	s.insert(begin, end);
}


template <typename Vector>
void readStringInto(Vector & s, ReadBuffer & buf)
{
	while (!buf.eof())
	{
		size_t bytes = 0;
		for (; buf.position() + bytes != buf.buffer().end(); ++bytes)
			if (buf.position()[bytes] == '\t' || buf.position()[bytes] == '\n')
				break;

		appendToStringOrVector(s, buf.position(), buf.position() + bytes);
		buf.position() += bytes;

		if (buf.hasPendingData())
			return;
	}
}

void readString(String & s, ReadBuffer & buf)
{
	s.clear();
	readStringInto(s, buf);
}

template void readStringInto<PODArray<UInt8>>(PODArray<UInt8> & s, ReadBuffer & buf);


template <typename Vector>
void readStringUntilEOFInto(Vector & s, ReadBuffer & buf)
{
	while (!buf.eof())
	{
		size_t bytes = buf.buffer().end() - buf.position();

		appendToStringOrVector(s, buf.position(), buf.position() + bytes);
		buf.position() += bytes;

		if (buf.hasPendingData())
			return;
	}
}

void readStringUntilEOF(String & s, ReadBuffer & buf)
{
	s.clear();
	readStringUntilEOFInto(s, buf);
}

template void readStringUntilEOFInto<PODArray<UInt8>>(PODArray<UInt8> & s, ReadBuffer & buf);


/** Распарсить escape-последовательность, которая может быть простой (один символ после бэкслеша) или более сложной (несколько символов).
  * Предполагается, что курсор расположен на символе \
  */
template <typename Vector>
static void parseComplexEscapeSequence(Vector & s, ReadBuffer & buf)
{
	++buf.position();
	if (buf.eof())
		throw Exception("Cannot parse escape sequence", ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);

	if (*buf.position() == 'x')
	{
		++buf.position();
		/// escape-последовательность вида \xAA
		UInt8 c1;
		UInt8 c2;
		readPODBinary(c1, buf);
		readPODBinary(c2, buf);
		s.push_back(static_cast<char>(unhex(c1) * 16 + unhex(c2)));
	}
	else
	{
		/// Обычная escape-последовательность из одного символа.
		s.push_back(parseEscapeSequence(*buf.position()));
		++buf.position();
	}
}


template <typename Vector>
void readEscapedStringInto(Vector & s, ReadBuffer & buf)
{
	while (!buf.eof())
	{
		const char * next_pos = find_first_symbols<'\t', '\n', '\\'>(buf.position(), buf.buffer().end());

		appendToStringOrVector(s, buf.position(), next_pos);
		buf.position() += next_pos - buf.position();

		if (!buf.hasPendingData())
			continue;

		if (*buf.position() == '\t' || *buf.position() == '\n')
			return;

		if (*buf.position() == '\\')
			parseComplexEscapeSequence(s, buf);
	}
}

void readEscapedString(String & s, ReadBuffer & buf)
{
	s.clear();
	readEscapedStringInto(s, buf);
}

template void readEscapedStringInto<PODArray<UInt8>>(PODArray<UInt8> & s, ReadBuffer & buf);


template <char quote, typename Vector>
static void readAnyQuotedStringInto(Vector & s, ReadBuffer & buf)
{
	if (buf.eof() || *buf.position() != quote)
		throw Exception("Cannot parse quoted string: expected opening quote",
			ErrorCodes::CANNOT_PARSE_QUOTED_STRING);
	++buf.position();

	while (!buf.eof())
	{
		const char * next_pos = find_first_symbols<'\\', quote>(buf.position(), buf.buffer().end());

		appendToStringOrVector(s, buf.position(), next_pos);
		buf.position() += next_pos - buf.position();

		if (!buf.hasPendingData())
			continue;

		if (*buf.position() == quote)
		{
			++buf.position();
			return;
		}

		if (*buf.position() == '\\')
			parseComplexEscapeSequence(s, buf);
	}

	throw Exception("Cannot parse quoted string: expected closing quote",
		ErrorCodes::CANNOT_PARSE_QUOTED_STRING);
}

template <typename Vector>
void readQuotedStringInto(Vector & s, ReadBuffer & buf)
{
	readAnyQuotedStringInto<'\''>(s, buf);
}

template <typename Vector>
void readDoubleQuotedStringInto(Vector & s, ReadBuffer & buf)
{
	readAnyQuotedStringInto<'"'>(s, buf);
}

template <typename Vector>
void readBackQuotedStringInto(Vector & s, ReadBuffer & buf)
{
	readAnyQuotedStringInto<'`'>(s, buf);
}


void readQuotedString(String & s, ReadBuffer & buf)
{
	s.clear();
	readQuotedStringInto(s, buf);
}

template void readQuotedStringInto<PODArray<UInt8>>(PODArray<UInt8> & s, ReadBuffer & buf);

void readDoubleQuotedString(String & s, ReadBuffer & buf)
{
	s.clear();
	readDoubleQuotedStringInto(s, buf);
}

template void readDoubleQuotedStringInto<PODArray<UInt8>>(PODArray<UInt8> & s, ReadBuffer & buf);

void readBackQuotedString(String & s, ReadBuffer & buf)
{
	s.clear();
	readBackQuotedStringInto(s, buf);
}

template void readBackQuotedStringInto<PODArray<UInt8>>(PODArray<UInt8> & s, ReadBuffer & buf);


template <typename Vector>
void readCSVStringInto(Vector & s, ReadBuffer & buf, const char delimiter)
{
	if (buf.eof())
		throwReadAfterEOF();

	char maybe_quote = *buf.position();

	/// Пустота и даже не в кавычках.
	if (maybe_quote == delimiter)
		return;

	if (maybe_quote == '\'' || maybe_quote == '"')
	{
		++buf.position();

		/// Закавыченный случай. Ищем следующую кавычку.
		while (!buf.eof())
		{
			const char * next_pos = reinterpret_cast<const char *>(memchr(buf.position(), maybe_quote, buf.buffer().end() - buf.position()));

			if (nullptr == next_pos)
				next_pos = buf.buffer().end();

			appendToStringOrVector(s, buf.position(), next_pos);
			buf.position() += next_pos - buf.position();

			if (!buf.hasPendingData())
				continue;

			/// Сейчас под курсором кавычка. Есть ли следующая?
			++buf.position();
			if (buf.eof())
				return;

			if (*buf.position() == maybe_quote)
			{
				s.push_back(maybe_quote);
				++buf.position();
				continue;
			}

			return;
		}
	}
	else
	{
		/// Незакавыченный случай. Ищем delimiter или \r или \n.
		while (!buf.eof())
		{
			const char * next_pos = buf.position();
			while (next_pos < buf.buffer().end()
				&& *next_pos != delimiter && *next_pos != '\r' && *next_pos != '\n')	/// NOTE Можно сделать SIMD версию.
				++next_pos;

			appendToStringOrVector(s, buf.position(), next_pos);
			buf.position() += next_pos - buf.position();

			if (!buf.hasPendingData())
				continue;

			/** CSV формат может содержать незначащие пробелы и табы.
			  * Обычно задача их пропускать - у вызывающего кода.
			  * Но в данном случае, сделать это будет сложно, поэтому удаляем концевые пробельные символы самостоятельно.
			  */
			size_t size = s.size();
			while (size > 0
				&& (s[size - 1] == ' ' || s[size - 1] == '\t'))
				--size;

			s.resize(size);
			return;
		}
	}
}

void readCSVString(String & s, ReadBuffer & buf, const char delimiter)
{
	s.clear();
	readCSVStringInto(s, buf, delimiter);
}

template void readCSVStringInto<PODArray<UInt8>>(PODArray<UInt8> & s, ReadBuffer & buf, const char delimiter);


void readDateTimeTextFallback(time_t & datetime, ReadBuffer & buf)
{
	static constexpr auto DATE_TIME_BROKEN_DOWN_LENGTH = 19;
	static constexpr auto UNIX_TIMESTAMP_MAX_LENGTH = 10;

	char s[DATE_TIME_BROKEN_DOWN_LENGTH];
	char * s_pos = s;

	/// Кусок, похожий на unix timestamp.
	while (s_pos < s + UNIX_TIMESTAMP_MAX_LENGTH && !buf.eof() && *buf.position() >= '0' && *buf.position() <= '9')
	{
		*s_pos = *buf.position();
		++s_pos;
		++buf.position();
	}

	/// 2015-01-01 01:02:03
	if (s_pos == s + 4 && !buf.eof() && (*buf.position() < '0' || *buf.position() > '9'))
	{
		const size_t remaining_size = DATE_TIME_BROKEN_DOWN_LENGTH - (s_pos - s);
		size_t size = buf.read(s_pos, remaining_size);
		if (remaining_size != size)
		{
			s_pos[size] = 0;
			throw Exception(std::string("Cannot parse datetime ") + s, ErrorCodes::CANNOT_PARSE_DATETIME);
		}

		UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
		UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
		UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');

		UInt8 hour = (s[11] - '0') * 10 + (s[12] - '0');
		UInt8 minute = (s[14] - '0') * 10 + (s[15] - '0');
		UInt8 second = (s[17] - '0') * 10 + (s[18] - '0');

		if (unlikely(year == 0))
			datetime = 0;
		else
			datetime = DateLUT::instance().makeDateTime(year, month, day, hour, minute, second);
	}
	else
		datetime = parse<time_t>(s, s_pos - s);
}


void readException(Exception & e, ReadBuffer & buf, const String & additional_message)
{
	int code = 0;
	String name;
	String message;
	String stack_trace;
	bool has_nested = false;

	readBinary(code, buf);
	readBinary(name, buf);
	readBinary(message, buf);
	readBinary(stack_trace, buf);
	readBinary(has_nested, buf);

	std::stringstream message_stream;

	if (!additional_message.empty())
		message_stream << additional_message << ". ";

	if (name != "DB::Exception")
		message_stream << name << ". ";

	message_stream << message
		<< ". Stack trace:\n\n" << stack_trace;

	if (has_nested)
	{
		Exception nested;
		readException(nested, buf);
		e = Exception(message_stream.str(), nested, code);
	}
	else
		e = Exception(message_stream.str(), code);
}

void readAndThrowException(ReadBuffer & buf, const String & additional_message)
{
	Exception e;
	readException(e, buf, additional_message);
	e.rethrow();
}

}
