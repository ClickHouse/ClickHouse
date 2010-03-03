#include <DB/Common/UnescapingInputStream.h>


namespace DB
{


UnescapingStreamBuf::UnescapingStreamBuf(std::istream & istr, char delimiter_)
	: p_istr(&istr), delimiter(delimiter_)
{
}


int UnescapingStreamBuf::readFromDevice()
{
	int res = p_istr->get();
	if (!p_istr->good())
		return res;

	char c = res;
	if (c == '\\')
	{
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
	}
	else if (c == delimiter)
	{
		return std::char_traits<char>::eof();
	}
	else
	{
		return c;
	}
}


UnescapingIOS::UnescapingIOS(std::istream & istr, char delimiter_)
	: buf(istr, delimiter_)
{
}


UnescapingStreamBuf * UnescapingIOS::rdbuf()
{
	return &buf;
}


UnescapingInputStream::UnescapingInputStream(std::istream & istr, char delimiter_)
	: UnescapingIOS(istr, delimiter_),
	std::istream(&buf)
{
}


}
