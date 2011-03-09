#ifndef MYSQLXX_EXCEPTION_H
#define MYSQLXX_EXCEPTION_H

#include <mysql/mysql.h>

#include <Poco/Exception.h>


namespace mysqlxx
{

struct Exception : public Poco::Exception
{
    Exception(const std::string & msg, int code = 0) : Poco::Exception(msg, code) {}
	int errnum() { return code(); }
};

struct ConnectionFailed : public Exception
{
	ConnectionFailed(const std::string & msg, int code = 0) : Exception(msg, code) {}
};

struct BadQuery : public Exception
{
	BadQuery(const std::string & msg, int code = 0) : Exception(msg, code) {}
};


inline void checkError(MYSQL * driver)
{
	unsigned num = mysql_errno(driver);

	if (num)
		throw Exception(mysql_error(driver), num);
}

inline void onError(MYSQL * driver)
{
	throw Exception(mysql_error(driver), mysql_errno(driver));
}

}

#endif
