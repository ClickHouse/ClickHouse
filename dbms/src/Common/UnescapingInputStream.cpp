#include <DB/Common/UnescapingInputStream.h>


namespace DB
{


UnescapingStreamBuf::UnescapingStreamBuf(std::istream & istr)
	: p_istr(&istr), state(Normal)
{
}


int UnescapingStreamBuf::readFromDevice()
{
	int res = p_istr->get();
	if (!p_istr->good())
		return res;

	char c = res;
	switch (c)
	{
		case '\\':
			res = p_istr->get();
			if (!p_istr->good())
				return res;
			c = res;
			switch (c)
			{
				case '\\':
					return '\\';
					break;
				case 'b':
					return '\b';
					break;
				case 'f':
					return '\f';
					break;
				case 'n':
					return '\n';
					break;
				case 'r':
					return '\r';
					break;
				case 't':
					return '\t';
					break;
				default:
					return c;
			}
			break;
		default:
			return c;
	}
}


UnescapingIOS::UnescapingIOS(std::istream & istr)
	: buf(istr)
{
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
