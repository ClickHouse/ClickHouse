#if USE_MYSQL
#include <mysql/mysql.h>
#endif
#include <mysqlxx/Exception.h>


namespace mysqlxx
{

std::string errorMessage(MYSQL * driver)
{
    std::stringstream res;
#if USE_MYSQL
    res << mysql_error(driver) << " (" << driver->host << ":" << driver->port << ")";
#else
    res << "Mysql support not compiled";
#endif
    return res.str();
}


/// Для внутренних нужд библиотеки.
void checkError(MYSQL * driver)
{
#if USE_MYSQL
    unsigned num = mysql_errno(driver);
#else
    unsigned num = 1;
#endif

    if (num)
        throw Exception(errorMessage(driver), num);
}


/// Для внутренних нужд библиотеки.
void onError(MYSQL * driver)
{
#if USE_MYSQL
    throw Exception(errorMessage(driver), mysql_errno(driver));
#else
    throw Exception(errorMessage(driver), 1);
#endif
}

}
