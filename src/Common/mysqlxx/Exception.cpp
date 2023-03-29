#if __has_include(<mysql.h>)
#include <mysql.h>
#else
#include <mysql/mysql.h>
#endif
#include <mysqlxx/Exception.h>
#include <fmt/format.h>


namespace mysqlxx
{

std::string errorMessage(MYSQL * driver, const std::string & query)
{
    return fmt::format("{}{} ({}:{})", mysql_error(driver),
        query.empty() ? "" : " while executing query: '" + query + "'",
        driver->host ? driver->host : "(nullptr)", driver->port);
}

void checkError(MYSQL * driver)
{
    unsigned num = mysql_errno(driver);

    if (num)
        throw Exception(errorMessage(driver), num);
}

void onError(MYSQL * driver)
{
    throw Exception(errorMessage(driver), mysql_errno(driver));
}

}
