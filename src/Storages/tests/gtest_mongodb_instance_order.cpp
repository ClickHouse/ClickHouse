#include <gtest/gtest.h>
#include <config.h>

#if USE_MONGODB

#include <Storages/StorageMongoDB.h>

#include <mongocxx/instance.hpp>

using namespace DB;

/// The driver requires `mongocxx::instance` to exist before any other driver object, and the URI of a
/// configuration is built right after the configuration itself. `mongocxx::instance::current` creates a
/// default instance when none exists, after which the holder's instance can never be created.
TEST(StorageMongoDB, ConfigurationCreatesDriverInstanceFirst)
{
    MongoDBConfiguration configuration;
    mongocxx::instance::current();
    EXPECT_NO_THROW(MongoDBInstanceHolder::instance());
}

#endif
