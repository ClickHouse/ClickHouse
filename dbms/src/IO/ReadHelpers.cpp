#include <DB/IO/ReadHelpers.h>

namespace DB
{

void assertString(const char * s, ReadBuffer & buf)
{
	for (; *s; ++s)
	{
		if (buf.eof() || *buf.position() != *s)
			throw Exception(String("Cannot parse input: expected ") + s, ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
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

}
