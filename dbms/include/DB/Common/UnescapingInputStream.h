#ifndef DBMS_COMMON_UNESCAPING_INPUT_STREAM_H
#define DBMS_COMMON_UNESCAPING_INPUT_STREAM_H

#include <istream>

#include <Poco/UnbufferedStreamBuf.h>

namespace DB
{


/** Поток, который unescape-ит всё, что из него читают.
  */
class UnescapingStreamBuf : public Poco::UnbufferedStreamBuf
{
public:
	UnescapingStreamBuf(std::istream & istr);

protected:
	int readFromDevice(char * buffer, std::streamsize length);

private:
	std::istream * p_istr;

	enum State
	{
		Normal = 0,
		EscapeSequence
	};

	State state;
};


class UnescapingIOS : public virtual std::ios
{
public:
	UnescapingIOS(std::istream & istr);
	UnescapingStreamBuf * rdbuf();

protected:
	UnescapingStreamBuf buf;
};


class UnescapingInputStream : public UnescapingIOS, public std::istream
{
public:
	UnescapingInputStream(std::istream & istr);
};


}


#endif
