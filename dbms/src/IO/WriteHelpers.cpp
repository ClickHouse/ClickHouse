#include <DB/IO/WriteHelpers.h>

namespace DB
{

void writeEscapedString(const String & s, WriteBuffer & buf)
{
	for (String::const_iterator it = s.begin(); it != s.end(); ++it)
	{
		switch (*it)
		{
			case '\b':
				writeChar('\\', buf);
				writeChar('b', buf);
				break;
			case '\f':
				writeChar('\\', buf);
				writeChar('f', buf);
				break;
			case '\n':
				writeChar('\\', buf);
				writeChar('n', buf);
				break;
			case '\r':
				writeChar('\\', buf);
				writeChar('r', buf);
				break;
			case '\t':
				writeChar('\\', buf);
				writeChar('t', buf);
				break;
			case '\0':
				writeChar('\\', buf);
				writeChar('0', buf);
				break;
			case '\'':
				writeChar('\\', buf);
				writeChar('\'', buf);
				break;
			case '"':
				writeChar('\\', buf);
				writeChar('"', buf);
				break;
			case '\\':
				writeChar('\\', buf);
				writeChar('\\', buf);
				break;
			default:
				writeChar(*it, buf);
		}
	}
}

}
