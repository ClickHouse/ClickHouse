#pragma once
#if USE_FDB
#include <string>
#include <gtest/gtest.h>
#include <Common/FoundationDB/fdb_c_fwd.h>

namespace DB
{
class FDBFixture : public ::testing::Test
{
public:
    static void SetUpTestSuite();
    static void TearDownTestSuite();
    static void clear();
    static bool hasFDB() { return db; }

    static std::string cluster_file;
    static std::string key_prefix;

private:
    static FDBDatabase * db;
};
};

#define SKIP_IF_NO_FDB() \
    if (!hasFDB()) \
    GTEST_SKIP() << "Skipped because CLICKHOUSE_UT_FDB_CLUSTER_FILE is not defined or empty"
#endif
