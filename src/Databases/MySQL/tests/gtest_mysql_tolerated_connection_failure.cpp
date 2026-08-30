#include "config.h"

#if USE_MYSQL

#if __has_include(<mysql.h>)
#include <errmsg.h>
#include <mysqld_error.h>
#else
#include <mysql/errmsg.h>
#include <mysql/mysqld_error.h>
#endif

#include <gtest/gtest.h>

#include <stdexcept>

#include <Common/Exception.h>
#include <Databases/MySQL/DatabaseMySQL.h>
#include <mysqlxx/Exception.h>

using DB::LogsLevel;
using DB::mysqlToleratedConnectionFailureLogLevel;

namespace DB::ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    /// The classifier reads the exception that is currently being handled, so every case has to be
    /// thrown first.
    template <typename Thrower>
    LogsLevel classify(Thrower && thrower)
    {
        try
        {
            thrower();
        }
        /// Ok to not report anything here: classifying the active exception is the point of the test.
        catch (...)
        {
            return mysqlToleratedConnectionFailureLogLevel();
        }
        ADD_FAILURE() << "The thrower did not throw";
        return LogsLevel::error;
    }
}

/// A connect that never succeeded, thrown as is by a direct `mysqlxx::Pool` probe.
TEST(MySQLToleratedConnectionFailure, ConnectionFailedIsTolerated)
{
    EXPECT_EQ(
        classify([] { throw mysqlxx::ConnectionFailed("Can't connect to MySQL server on '127.0.0.1:1'", CR_CONN_HOST_ERROR); }),
        LogsLevel::warning);
}

/// `mysqlxx::PoolWithFailover::get` swallows the per-replica failure and rethrows this instead.
TEST(MySQLToleratedConnectionFailure, RewrappedConnectionFailureIsTolerated)
{
    EXPECT_EQ(
        classify([] { throw DB::Exception(DB::ErrorCodes::ALL_CONNECTION_TRIES_FAILED, "Connections to mysql failed"); }),
        LogsLevel::warning);
}

/// The server accepted the socket and then went away mid-query - `mysqlxx::Query` turns
/// `CR_SERVER_LOST` / `CR_SERVER_GONE_ERROR` into `ConnectionLost`, which is a sibling of
/// `ConnectionFailed`, not a subclass, so it needs its own branch in the classifier.
TEST(MySQLToleratedConnectionFailure, ConnectionLostIsTolerated)
{
    EXPECT_EQ(
        classify([] { throw mysqlxx::ConnectionLost("Lost connection to MySQL server during query", CR_SERVER_LOST); }),
        LogsLevel::warning);
    EXPECT_EQ(
        classify([] { throw mysqlxx::ConnectionLost("MySQL server has gone away", CR_SERVER_GONE_ERROR); }),
        LogsLevel::warning);
}

/// The remote responded and rejected us, or the metadata query itself failed - both must stay
/// visible even when the caller tolerates the failure.
TEST(MySQLToleratedConnectionFailure, OtherFailuresKeepError)
{
    EXPECT_EQ(
        classify([] { throw mysqlxx::BadQuery("SELECT command denied to user", ER_TABLEACCESS_DENIED_ERROR); }),
        LogsLevel::error);
    EXPECT_EQ(
        classify([] { throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unexpected"); }),
        LogsLevel::error);
    EXPECT_EQ(classify([] { throw std::runtime_error("something else entirely"); }), LogsLevel::error);
}

#endif
