#include <DB/Core/ReadBuffer.h>

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

void readQuotedString(String & s, ReadBuffer & buf)
{
	s = "";

	if (buf.eof() || *buf.position() != '\'')
		throw Exception("Cannot parse quoted string: expected opening single quote",
			ErrorCodes::CANNOT_PARSE_QUOTED_STRING);
	++buf.position();

	while (!buf.eof())
	{
		size_t bytes = 0;
		for (; buf.position() + bytes != buf.buffer().end(); ++bytes)
			if (buf.position()[bytes] == '\\' || buf.position()[bytes] == '\'')
				break;

		s.append(buf.position(), bytes);
		buf.position() += bytes;

		if (*buf.position() == '\'')
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

	throw Exception("Cannot parse quoted string: expected closing single quote",
		ErrorCodes::CANNOT_PARSE_QUOTED_STRING);
}

}
