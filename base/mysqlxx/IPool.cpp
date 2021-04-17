#if __has_include(<mysql.h>)
#include <mysql.h>
#else
#include <mysql/mysql.h>
#endif

#include "IPool.h"

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
