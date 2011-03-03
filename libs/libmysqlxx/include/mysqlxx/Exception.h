#ifndef MYSQLXX_EXCEPTION_H
#define MYSQLXX_EXCEPTION_H

#include <mysql/mysql.h>

#include <Poco/Exception.h>


namespace mysqlxx
{

POCO_DECLARE_EXCEPTION(Foundation_API, Exception, Poco::Exception);


inline void checkError(MYSQL & driver)
{
	unsigned num = mysql_errno(&driver);

	if (num)
		throw Exception(mysql_error(&driver), num);
}

inline void onError(MYSQL & driver)
{
	throw Exception(mysql_error(&driver), mysql_errno(&driver));
}

}

#endif
