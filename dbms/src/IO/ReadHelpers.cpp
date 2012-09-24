#include <sstream>

#include <mysqlxx/Manip.h>

#include <DB/Core/Defines.h>
#include <DB/IO/ReadHelpers.h>


namespace DB
{


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


void assertString(const char * s, ReadBuffer & buf)
{
	for (; *s; ++s)
	{
		if (buf.eof() || *buf.position() != *s)
			throwAtAssertionFailed(s, buf);
		++buf.position();
	}
}

void readString(String & s, ReadBuffer & buf)
{
	s = "";
	while (!buf.eof())
	{
		size_t bytes = 0;
		for (; buf.position() + bytes != buf.buffer().end(); ++bytes)
			if (buf.position()[bytes] == '\t' || buf.position()[bytes] == '\n')
				break;

		s.append(buf.position(), bytes);
		buf.position() += bytes;

		if (buf.position() != buf.buffer().end())
			return;
	}
}

void readEscapedString(String & s, ReadBuffer & buf)
{
	s = "";
	while (!buf.eof())
	{
		size_t bytes = 0;
		for (; buf.position() + bytes != buf.buffer().end(); ++bytes)
			if (buf.position()[bytes] == '\\' || buf.position()[bytes] == '\t' || buf.position()[bytes] == '\n')
				break;

		s.append(buf.position(), bytes);
		buf.position() += bytes;

		if (buf.position() == buf.buffer().end())
			continue;

		if (*buf.position() == '\t' || *buf.position() == '\n')
			return;

		if (*buf.position() == '\\')
		{
			++buf.position();
			if (buf.eof())
				throw Exception("Cannot parse escape sequence", ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);
			s += parseEscapeSequence(*buf.position());
			++buf.position();
		}
	}
}


template <char quote>
static void readAnyQuotedString(String & s, ReadBuffer & buf)
{
	s = "";

	if (buf.eof() || *buf.position() != quote)
		throw Exception("Cannot parse quoted string: expected opening quote",
			ErrorCodes::CANNOT_PARSE_QUOTED_STRING);
	++buf.position();

	while (!buf.eof())
	{
		size_t bytes = 0;
		for (; buf.position() + bytes != buf.buffer().end(); ++bytes)
			if (buf.position()[bytes] == '\\' || buf.position()[bytes] == quote)
				break;

		s.append(buf.position(), bytes);
		buf.position() += bytes;

		if (buf.position() == buf.buffer().end())
			continue;

		if (*buf.position() == quote)
		{
			++buf.position();
			return;
		}

		if (*buf.position() == '\\')
		{
			++buf.position();
			if (buf.eof())
				throw Exception("Cannot parse escape sequence", ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);
			s += parseEscapeSequence(*buf.position());
			++buf.position();
		}
	}

	throw Exception("Cannot parse quoted string: expected closing quote",
		ErrorCodes::CANNOT_PARSE_QUOTED_STRING);
}


void readQuotedString(String & s, ReadBuffer & buf)
{
	readAnyQuotedString<'\''>(s, buf);
}

void readDoubleQuotedString(String & s, ReadBuffer & buf)
{
	readAnyQuotedString<'"'>(s, buf);
}

void readBackQuotedString(String & s, ReadBuffer & buf)
{
	readAnyQuotedString<'`'>(s, buf);
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
