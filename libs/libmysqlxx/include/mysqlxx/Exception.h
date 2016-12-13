#pragma once

#include <sstream>
#include <mysql.h>

#include <Poco/Exception.h>


namespace mysqlxx
{

/** Общий класс исключений, которые могут быть выкинуты функциями из библиотеки.
  * Функции code() и errnum() возвращают номер ошибки MySQL. (см. mysqld_error.h)
  */
struct Exception : public Poco::Exception
{
    Exception(const std::string & msg, int code = 0) : Poco::Exception(msg, code) {}
	int errnum() const { return code(); }
	const char * name() const throw() { return "mysqlxx::Exception"; }
	const char * className() const throw() { return "mysqlxx::Exception"; }
};


/// Не удалось соединиться с сервером.
struct ConnectionFailed : public Exception
{
	ConnectionFailed(const std::string & msg, int code = 0) : Exception(msg, code) {}
	const char * name() const throw() { return "mysqlxx::ConnectionFailed"; }
	const char * className() const throw() { return "mysqlxx::ConnectionFailed"; }
};


/// Запрос содержит ошибку.
struct BadQuery : public Exception
{
	BadQuery(const std::string & msg, int code = 0) : Exception(msg, code) {}
	const char * name() const throw() { return "mysqlxx::BadQuery"; }
	const char * className() const throw() { return "mysqlxx::BadQuery"; }
};


/// Невозможно распарсить значение.
struct CannotParseValue : public Exception
{
	CannotParseValue(const std::string & msg, int code = 0) : Exception(msg, code) {}
	const char * name() const throw() { return "mysqlxx::CannotParseValue"; }
	const char * className() const throw() { return "mysqlxx::CannotParseValue"; }
};


inline std::string errorMessage(MYSQL * driver)
{
	std::stringstream res;
	res << mysql_error(driver) << " (" << driver->host << ":" << driver->port << ")";
	return res.str();
}


/// Для внутренних нужд библиотеки.
inline void checkError(MYSQL * driver)
{
	unsigned num = mysql_errno(driver);

	if (num)
		throw Exception(errorMessage(driver), num);
}


/// Для внутренних нужд библиотеки.
inline void onError(MYSQL * driver)
{
	throw Exception(errorMessage(driver), mysql_errno(driver));
}

}
