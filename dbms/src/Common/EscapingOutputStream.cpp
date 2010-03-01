#include <DB/Common/EscapingOutputStream.h>

#define ESCAPING_STREAM_BUFFER_SIZE 4096

namespace DB
{


EscapingStreamBuf::EscapingStreamBuf(std::ostream & ostr)
	: p_ostr(&ostr)
{
}


int EscapingStreamBuf::writeToDevice(char c)
{
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

	return 1;
}


EscapingIOS::EscapingIOS(std::ostream & ostr)
	: buf(ostr)
{
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
