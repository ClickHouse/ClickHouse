#if __has_include(<mariadb/mysql.h>)
#include <mariadb/mysql.h>
#else
#include <mysql/mysql.h>
#endif
#include <mysqlxx/Exception.h>


namespace mysqlxx
{

std::string errorMessage(MYSQL * driver)
{
    std::stringstream res;
    res << mysql_error(driver)
        << " (" << (driver->host ? driver->host : "(nullptr)")
        << ":" << driver->port << ")";
    return res.str();
}


/// Для внутренних нужд библиотеки.
void checkError(MYSQL * driver)
{
    unsigned num = mysql_errno(driver);

    if (num)
        throw Exception(errorMessage(driver), num);
}


/// Для внутренних нужд библиотеки.
void onError(MYSQL * driver)
{
    throw Exception(errorMessage(driver), mysql_errno(driver));
}

}
