#include <DB/Common/UnescapingInputStream.h>


namespace DB
{


UnescapingStreamBuf::UnescapingStreamBuf(std::istream & istr)
	: p_istr(&istr), state(Normal)
{
}


int UnescapingStreamBuf::readFromDevice(char * buffer, std::streamsize length)
{
	if (length == 0 || !p_istr)
		return 0;

	for (std::streamsize pos = 0; pos < length;)
	{
		char c = p_istr->get();
		if (!p_istr->good())
		{
			p_istr = 0;
			return pos;
		}
		
		switch (state)
		{
			case EscapeSequence:
				switch (c)
				{
					case '\\':
						buffer[pos] = '\\';
						break;
					case 'b':
						buffer[pos] = '\b';
						break;
					case 'f':
						buffer[pos] = '\f';
						break;
					case 'n':
						buffer[pos] = '\n';
						break;
					case 'r':
						buffer[pos] = '\r';
						break;
					case 't':
						buffer[pos] = '\t';
						break;
					case '\'':
						buffer[pos] = '\'';
						break;
					case '"':
						buffer[pos] = '"';
						break;
					default:
						buffer[pos] = c;
				}
				++pos;
				state = Normal;
				break;
				
			case Normal:
			default:
				switch (c)
				{
					case '\\':
						state = EscapeSequence;
						break;
					default:
						buffer[pos] = c;
						++pos;
				}
		}
	}

	return static_cast<int>(length);
}


UnescapingIOS::UnescapingIOS(std::istream & istr)
	: buf(istr)
{
	poco_ios_init(&buf);
}


UnescapingStreamBuf * UnescapingIOS::rdbuf()
{
	return &buf;
}


UnescapingInputStream::UnescapingInputStream(std::istream & istr)
	: UnescapingIOS(istr),
	std::istream(&buf)
{
}


}
