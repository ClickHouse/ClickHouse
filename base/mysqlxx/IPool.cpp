#include "IPool.h"

#if __has_include(<mysql.h>)
#include <mysql.h>
#include <mysqld_error.h>
#else
#include <mysql/mysql.h>
#include <mysql/mysqld_error.h>
#endif


namespace
{
    struct MySQLThreadInitializer
    {
    public:
        MySQLThreadInitializer()
        {
            mysql_thread_init();
        }

        ~MySQLThreadInitializer()
        {
            mysql_thread_end();
        }
    };

}

namespace mysqlxx
{

    void IPool::Entry::initializeMySQLThread()
    {
        static thread_local MySQLThreadInitializer initializer [[maybe_unused]];
    }
}
