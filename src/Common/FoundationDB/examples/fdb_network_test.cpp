#include <cstdint>
#include <cstring>
#include <foundationdb/fdb_c.h>
#include <foundationdb/fdb_c_options.g.h>
#include <gtest/gtest.h>
#include <gtest/internal/gtest-internal.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <Common/FoundationDB/FoundationDBCommon.h>
#include <Common/FoundationDB/FoundationDBHelpers.h>
#include <Common/FoundationDB/fdb_error_definitions.h>
#include <Common/logger_useful.h>

#include "common.h"

#define FDB_TRANSACTION_DEFAULT_TIMEOUT_MS 5000

using namespace DB;

/*
Config example:
<fdb>
    <cluster_file>/etc/foundationdb/fdb.cluster</cluster_file>
    <key_prefix>fdb_network_test/</key_prefix>
    <start_command>fdbctl up</start_command>
    <stop_command>fdbctl down<stop_command>
</fdb>
*/
class Config
{
public:
    static void init(const char * config_file)
    {
        ConfigProcessor config_processor(config_file, false, true);
        config = config_processor.loadConfig().configuration;
    }

    static bool hasConfig() { return !config.isNull(); }
    static std::string clusterFile() { return config->getString("cluster_file", "/etc/foundationdb/fdb.cluster"); }
    static std::string keyPrefix() { return config->getString("key_prefix", "fdb_network_test/"); }
    static std::string startCommand() { return config->getString("start_command", ""); }
    static std::string stopCommand() { return config->getString("stop_command", ""); }

private:
    static Poco::AutoPtr<Poco::Util::AbstractConfiguration> config;
};

Poco::AutoPtr<Poco::Util::AbstractConfiguration> Config::config;

class FDBNetworkTest : public testing::Test
{
public:
    static void SetUpTestSuite() { FoundationDBNetwork::ensureStarted(); }

    void SetUp() override
    {
        if (!Config::hasConfig())
            GTEST_SKIP() << "No config file";
    }

    static void shell(const std::string & cmd) { ASSERT_EQ(system(cmd.c_str()), 0) << "cmd return non-zero"; }

    static void startFDB()
    {
        auto cmd = Config::startCommand();
        ASSERT_NE(cmd, "") << "start_command is empty";
        shell(cmd);
    }

    static void stopFDB()
    {
        auto cmd = Config::stopCommand();
        ASSERT_NE(cmd, "") << "stop_command is empty";
        shell(cmd);
    }

    static void wait(std::shared_ptr<FDBFuture> f)
    {
        throwIfFDBError(fdb_future_block_until_ready(f.get()));
        throwIfFDBError(fdb_future_get_error(f.get()));
    }

    static std::shared_ptr<FDBDatabase> createDatabase()
    {
        FDBDatabase * db;
        throwIfFDBError(fdb_create_database(Config::clusterFile().c_str(), &db));
        return fdb_manage_object(db);
    }

    static std::shared_ptr<FDBTransaction> createTransaction(FDBDatabase * db)
    {
        FDBTransaction * tr;
        throwIfFDBError(fdb_database_create_transaction(db, &tr));
        return fdb_manage_object(tr);
    }

    template <class TrxFunc>
    static auto execTrx(FDBTransaction & tr, TrxFunc tr_func) -> decltype(tr_func())
    {
        while (true)
        {
            try
            {
                return tr_func();
            }
            catch (FoundationDBException & e)
            {
                /// Handling transaction errors.
                /// If the transaction can be retried, it will wait for a while to
                /// continue the next loop. Otherwise, it will throw an exception.
                wait(fdb_manage_object(fdb_transaction_on_error(&tr, e.code)));
                /// TODO: log retry
            }
        }
    }
};

void printUsage()
{
    std::cerr << "Usage: fdb_network_test [--config config_file] [-h|--help] [...gtest args]" << std::endl;
}

TEST_F(FDBNetworkTest, TransactionShouldTimeoutIfFDBUnavailable)
{
    stopFDB();
    auto db = createDatabase();
    auto tr = createTransaction(db.get());

    int64_t timeout = FDB_TRANSACTION_DEFAULT_TIMEOUT_MS; // ms
    throwIfFDBError(fdb_transaction_set_option(tr.get(), FDB_TR_OPTION_TIMEOUT, reinterpret_cast<const uint8_t *>(&timeout), 8));

    try
    {
        execTrx(*tr, [&]() {
            auto key = Config::keyPrefix() + "test";
            std::string value = "value";
            fdb_transaction_set(tr.get(), FDB_KEY_FROM_STRING(key), FDB_VALUE_FROM_STRING(value));
            wait(fdb_manage_object(fdb_transaction_commit(tr.get())));
        });
        GTEST_FAIL() << "No execption is throw";
    }
    catch (FoundationDBException & e)
    {
        ASSERT_EQ(e.code, FDBErrorCode::transaction_timed_out);
    }
}

TEST_F(FDBNetworkTest, TransactionShouldTimeoutIfFDBUnavailableInTransaction)
{
    startFDB();
    auto db = createDatabase();
    auto tr = createTransaction(db.get());

    int64_t timeout = FDB_TRANSACTION_DEFAULT_TIMEOUT_MS; // ms
    throwIfFDBError(fdb_transaction_set_option(tr.get(), FDB_TR_OPTION_TIMEOUT, reinterpret_cast<const uint8_t *>(&timeout), 8));

    try
    {
        execTrx(*tr, [&]() {
            auto key = Config::keyPrefix() + "test";
            std::string value = "value";
            wait(fdb_manage_object(fdb_transaction_get(tr.get(), FDB_KEY_FROM_STRING(key), false)));
            fdb_transaction_set(tr.get(), FDB_KEY_FROM_STRING(key), FDB_VALUE_FROM_STRING(value));
            stopFDB();
            wait(fdb_manage_object(fdb_transaction_commit(tr.get())));
        });
        GTEST_FAIL() << "No execption is throw";
    }
    catch (FoundationDBException & e)
    {
        ASSERT_EQ(e.code, FDBErrorCode::transaction_timed_out);
    }
}

TEST_F(FDBNetworkTest, RWTransactionShouldRetryIfFDBTemporarilyUnavailable)
{
    startFDB();
    auto db = createDatabase();
    auto tr = createTransaction(db.get());

    int64_t timeout = FDB_TRANSACTION_DEFAULT_TIMEOUT_MS; // ms
    throwIfFDBError(fdb_transaction_set_option(tr.get(), FDB_TR_OPTION_TIMEOUT, reinterpret_cast<const uint8_t *>(&timeout), 8));

    int tries = 0;

    execTrx(*tr, [&]() {
        auto key = Config::keyPrefix() + "test";
        std::string value = "value";
        wait(fdb_manage_object(fdb_transaction_get(tr.get(), FDB_KEY_FROM_STRING(key), false)));
        fdb_transaction_set(tr.get(), FDB_KEY_FROM_STRING(key), FDB_VALUE_FROM_STRING(value));

        if (tries == 0)
        {
            stopFDB();
            startFDB();
        }

        tries++;
        wait(fdb_manage_object(fdb_transaction_commit(tr.get())));
    });
    ASSERT_EQ(tries, 2);
}

TEST_F(FDBNetworkTest, RWConflitTransactionShouldRetry)
{
    startFDB();
    auto db = createDatabase();
    auto tr1 = createTransaction(db.get());
    auto tr2 = createTransaction(db.get());

    auto key = Config::keyPrefix() + "test";
    std::string value = "value";

    wait(fdb_manage_object(fdb_transaction_get(tr1.get(), FDB_KEY_FROM_STRING(key), false)));
    wait(fdb_manage_object(fdb_transaction_get(tr2.get(), FDB_KEY_FROM_STRING(key), false)));

    fdb_transaction_set(tr1.get(), FDB_KEY_FROM_STRING(key), FDB_VALUE_FROM_STRING(value));
    fdb_transaction_set(tr2.get(), FDB_KEY_FROM_STRING(key), FDB_VALUE_FROM_STRING(value));

    wait(fdb_manage_object(fdb_transaction_commit(tr1.get())));

    auto tr2_commit_future = fdb_manage_object(fdb_transaction_commit(tr2.get()));
    throwIfFDBError(fdb_future_block_until_ready(tr2_commit_future.get()));
    ASSERT_EQ(fdb_future_get_error(tr2_commit_future.get()), FDBErrorCode::not_committed);
    wait(fdb_manage_object(fdb_transaction_on_error(tr2.get(), FDBErrorCode::not_committed)));
}

TEST_F(FDBNetworkTest, ClientSideExceptionShouldNotBreakNetwork)
{
    startFDB();
    auto db = createDatabase();
    auto tr = createTransaction(db.get());

    int64_t timeout = FDB_TRANSACTION_DEFAULT_TIMEOUT_MS; // ms
    throwIfFDBError(fdb_transaction_set_option(tr.get(), FDB_TR_OPTION_TIMEOUT, reinterpret_cast<const uint8_t *>(&timeout), 8));

    try
    {
        execTrx(*tr, [&]() {
            auto key = Config::keyPrefix() + "test";
            std::string value = "value";
            wait(fdb_manage_object(fdb_transaction_get(tr.get(), FDB_KEY_FROM_STRING(key), false)));
            fdb_transaction_set(tr.get(), FDB_KEY_FROM_STRING(key), FDB_VALUE_FROM_STRING(value));

            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Excepection for test");
        });
        GTEST_FAIL() << "Unreachable";
    }
    catch (Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Create another transaction
    tr = createTransaction(db.get());
    execTrx(*tr, [&]() {
        auto key = Config::keyPrefix() + "test";
        std::string value = "value";
        wait(fdb_manage_object(fdb_transaction_get(tr.get(), FDB_KEY_FROM_STRING(key), false)));
        fdb_transaction_set(tr.get(), FDB_KEY_FROM_STRING(key), FDB_VALUE_FROM_STRING(value));

        wait(fdb_manage_object(fdb_transaction_commit(tr.get())));
    });
}

TEST_F(FDBNetworkTest, ServerSideExceptionShouldNotBreakNetwork)
{
    startFDB();
    auto db = createDatabase();
    auto tr = createTransaction(db.get());

    int64_t timeout = FDB_TRANSACTION_DEFAULT_TIMEOUT_MS; // ms
    throwIfFDBError(fdb_transaction_set_option(tr.get(), FDB_TR_OPTION_TIMEOUT, reinterpret_cast<const uint8_t *>(&timeout), 8));

    try
    {
        execTrx(*tr, [&]() {
            std::string key = "\xfftest"; /// Illegal key
            std::string value = "value";
            wait(fdb_manage_object(fdb_transaction_get(tr.get(), FDB_KEY_FROM_STRING(key), false)));
            fdb_transaction_set(tr.get(), FDB_KEY_FROM_STRING(key), FDB_VALUE_FROM_STRING(value));

            wait(fdb_manage_object(fdb_transaction_commit(tr.get())));
        });
        GTEST_FAIL() << "Unreachable";
    }
    catch (FoundationDBException & e)
    {
        ASSERT_EQ(e.code, FDBErrorCode::key_outside_legal_range);
    }

    /// Create another transaction
    tr = createTransaction(db.get());
    execTrx(*tr, [&]() {
        auto key = Config::keyPrefix() + "test";
        std::string value = "value";
        wait(fdb_manage_object(fdb_transaction_get(tr.get(), FDB_KEY_FROM_STRING(key), false)));
        fdb_transaction_set(tr.get(), FDB_KEY_FROM_STRING(key), FDB_VALUE_FROM_STRING(value));

        wait(fdb_manage_object(fdb_transaction_commit(tr.get())));
    });
}


int main(int argc, char ** argv)
try
{
    initLogger();
    for (int i = 1; i < argc; i++)
    {
        if (strcmp("--config", argv[i]) == 0)
        {
            if (i == argc - 1)
            {
                printUsage();
                return 1;
            }

            Config::init(argv[i + 1]);
        }
        else if (strcmp("-h", argv[i]) == 0 || strcmp("--help", argv[i]) == 0)
        {
            printUsage();
            std::cerr << std::endl;
            break;
        }
    }

    ::testing::InitGoogleTest(&argc, argv);
    auto run_test_res = RUN_ALL_TESTS();
    FoundationDBNetwork::shutdownIfNeed();
    return run_test_res;
}
catch (...)
{
    tryLogCurrentException(__PRETTY_FUNCTION__);
    return 1;
}
