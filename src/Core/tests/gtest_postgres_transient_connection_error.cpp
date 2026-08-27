#include "config.h"

#if USE_LIBPQXX

#include <gtest/gtest.h>

#include <Core/PostgreSQL/Utils.h>

using postgres::isTransientConnectionError;

/// The `127.0.0.1:1` message is the exact libpq wording captured on 26.7.1.1.
TEST(PostgresTransientConnectionError, TransportFailuresAreTransient)
{
    EXPECT_TRUE(isTransientConnectionError(
        "connection to server at \"127.0.0.1\", port 1 failed: Connection refused\n"
        "\tIs the server running on that host and accepting TCP/IP connections?\n"));
    EXPECT_TRUE(isTransientConnectionError(
        "connection to server at \"192.0.2.1\", port 5432 failed: timeout expired"));
    EXPECT_TRUE(isTransientConnectionError("... failed: Connection timed out"));
    EXPECT_TRUE(isTransientConnectionError("... failed: No route to host"));
    EXPECT_TRUE(isTransientConnectionError("... failed: Network is unreachable"));
    EXPECT_TRUE(isTransientConnectionError("... failed: Connection reset by peer"));
    EXPECT_TRUE(isTransientConnectionError(
        "could not translate host name \"host\" to address: Temporary failure in name resolution"));
}

/// A host that does not resolve is a permanent misconfiguration, unlike a resolver that is
/// temporarily unreachable, so it must not be silenced from Error.
TEST(PostgresTransientConnectionError, UnknownHostIsNotTransient)
{
    EXPECT_FALSE(isTransientConnectionError(
        "could not translate host name \"nosuchhost\" to address: Name or service not known"));
}

TEST(PostgresTransientConnectionError, ServerRejectionsAreNotTransient)
{
    EXPECT_FALSE(isTransientConnectionError(
        "connection to server at \"127.0.0.1\", port 5432 failed: FATAL:  password authentication failed for user \"u\""));
    EXPECT_FALSE(isTransientConnectionError(
        "connection to server at \"127.0.0.1\", port 5432 failed: FATAL:  database \"nope\" does not exist"));
    EXPECT_FALSE(isTransientConnectionError(
        "connection to server at \"127.0.0.1\", port 5432 failed: FATAL:  no pg_hba.conf entry for host \"10.0.0.1\""));
}

TEST(PostgresTransientConnectionError, UnknownDefaultsToNonTransient)
{
    EXPECT_FALSE(isTransientConnectionError(""));
    EXPECT_FALSE(isTransientConnectionError("some future libpq wording we do not recognize"));
}

#endif
