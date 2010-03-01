#ifndef DBMS_COMMON_ESCAPING_OUTPUT_STREAM_H
#define DBMS_COMMON_ESCAPING_OUTPUT_STREAM_H

#include <ostream>

#include <Poco/UnbufferedStreamBuf.h>


namespace DB
{


/** Поток, который эскейпит всё, что в него пишут.
  */
class EscapingStreamBuf : public Poco::UnbufferedStreamBuf
{
public:
	EscapingStreamBuf(std::ostream & ostr);

protected:
	int writeToDevice(const char * buffer, std::streamsize length);

private:
	std::ostream * p_ostr;
};


class EscapingIOS : public virtual std::ios
{
public:
	EscapingIOS(std::ostream & ostr);
	EscapingStreamBuf * rdbuf();

protected:
	EscapingStreamBuf buf;
};


class EscapingOutputStream : public EscapingIOS, public std::ostream
{
public:
	EscapingOutputStream(std::ostream & ostr);
};


}


#endif
