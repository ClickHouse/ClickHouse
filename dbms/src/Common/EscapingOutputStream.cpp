#include <DB/Common/EscapingOutputStream.h>

#define ESCAPING_STREAM_BUFFER_SIZE 4096

namespace DB
{


EscapingStreamBuf::EscapingStreamBuf(std::ostream & ostr)
	: p_ostr(&ostr)
{
}


int EscapingStreamBuf::writeToDevice(const char * buffer, std::streamsize length)
{
	if (length == 0 || !p_ostr)
		return 0;

	for (std::streamsize pos = 0; pos < length; ++pos)
	{
		char c = buffer[pos];
		switch(c)
		{
			case '\\':
				p_ostr->write("\\\\", 2);
				break;
			case '\b':
				p_ostr->write("\\b", 2);
				break;
			case '\f':
				p_ostr->write("\\f", 2);
				break;
			case '\n':
				p_ostr->write("\\n", 2);
				break;
			case '\r':
				p_ostr->write("\\r", 2);
				break;
			case '\t':
				p_ostr->write("\\t", 2);
				break;
			case '\'':
				p_ostr->write("\\'", 2);
				break;
			case '"':
				p_ostr->write("\\\"", 2);
				break;
			default:
				p_ostr->put(c);
		}
	}

	if (!p_ostr->good())
	{
		p_ostr = 0;
		return -1;
	}

	return static_cast<int>(length);
}


EscapingIOS::EscapingIOS(std::ostream & ostr)
	: buf(ostr)
{
	poco_ios_init(&buf);
}


EscapingStreamBuf * EscapingIOS::rdbuf()
{
	return &buf;
}


EscapingOutputStream::EscapingOutputStream(std::ostream & ostr)
	: EscapingIOS(ostr),
	std::ostream(&buf)
{
}


}
