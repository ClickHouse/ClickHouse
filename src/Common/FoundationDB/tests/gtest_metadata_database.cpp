#include <gtest/gtest.h>
#include <MetadataDatabase.pb.h>
#include "common.h"

using namespace DB;
using namespace std::string_literals;

using MetadataDatabase = FoundationDB::Proto::MetadataDatabase;
class MetadataDatabaseTest : public testing::Test
{
public:
    void SetUp() override
    {
        const char * config_file = getConfigFile();
        if (!config_file)
        {
            GTEST_SKIP() << "If no foundationdb is used, skip this test";
        }
    }
    static void SetUpTestSuite()
    {
        const char * config_file = getConfigFile();
        meta_store = createMetadataStore(config_file);
        test_data = new MetadataDatabase();
        test_data->set_name("test_name");
        test_data->set_sql("test");
        uuid.toUnderType().items[0] = 0;
        uuid.toUnderType().items[1] = 1;
        if (meta_store->isExistsDatabase(test_data->name()))
        {
            meta_store->removeDatabaseMeta(test_data->name());
        }
    }

    static void TearDownTestSuite()
    {
        delete test_data;
        test_data = nullptr;
    }
    static UUID uuid;
    static MetadataDatabase * test_data;
    static std::shared_ptr<DB::MetadataStoreFoundationDB> meta_store;
};
FoundationDB::Proto::MetadataDatabase * MetadataDatabaseTest::test_data = nullptr;
std::shared_ptr<DB::MetadataStoreFoundationDB> MetadataDatabaseTest::meta_store;
UUID MetadataDatabaseTest::uuid;
TEST_F(MetadataDatabaseTest, addDatabaseMeta)
{
    meta_store->addDatabaseMeta(*test_data, {test_data->name()}, uuid);
    EXPECT_TRUE(meta_store->isExistsDatabase(test_data->name()));

    meta_store->removeDatabaseMeta(test_data->name());
}
TEST_F(MetadataDatabaseTest, getDatabaseMeta)
{
    meta_store->addDatabaseMeta(*test_data, {test_data->name()}, uuid);
    auto ret_test_data = meta_store->getDatabaseMeta({test_data->name()});
    std::string old_;
    test_data->SerializeToString(&old_);
    std::string new_;
    ret_test_data->SerializeToString(&new_);
    EXPECT_EQ(old_, new_);

    meta_store->removeDatabaseMeta(test_data->name());
}

TEST_F(MetadataDatabaseTest, listDatabases)
{
    int test_count = 10;
    std::vector<MetadataDatabase> list_tests(test_count);
    std::string base_name = "list_test";
    for (int i = 0; i < test_count; i++)
    {
        std::string test_name = base_name + std::to_string(i);
        meta_store->removeDatabaseMeta(test_name);
        list_tests[i].set_name(test_name);
        list_tests[i].set_sql(test_name);
        meta_store->addDatabaseMeta(list_tests[i], {list_tests[i].name()}, uuid);
    }
    auto res = meta_store->listDatabases();
    for (int i = 0; i < test_count; i++)
    {
        EXPECT_TRUE(std::find_if(res.begin(), res.end(), [&](auto & s) { return s->name() == list_tests[i].name(); }) != res.end());
        meta_store->removeDatabaseMeta(list_tests[i].name());
    }
}
TEST_F(MetadataDatabaseTest, isExistsDatabase)
{
    meta_store->addDatabaseMeta(*test_data, {test_data->name()}, uuid);
    EXPECT_TRUE(meta_store->isExistsDatabase(test_data->name()));
    meta_store->removeDatabaseMeta(test_data->name());
    EXPECT_FALSE(meta_store->isExistsDatabase(test_data->name()));
}
TEST_F(MetadataDatabaseTest, updateDatabaseMeta)
{
    meta_store->addDatabaseMeta(*test_data, {test_data->name()}, uuid);
    test_data->set_sql("update");
    meta_store->updateDatabaseMeta(test_data->name(), *test_data);
    auto ret_test_data = meta_store->getDatabaseMeta({test_data->name()});
    std::string old_;
    test_data->SerializeToString(&old_);
    std::string new_;
    test_data->SerializeToString(&new_);
    EXPECT_EQ(old_, new_);
    test_data->set_sql("test");
    meta_store->removeDatabaseMeta(test_data->name());
}
TEST_F(MetadataDatabaseTest, renameDatabase)
{
    meta_store->addDatabaseMeta(*test_data, {test_data->name()}, uuid);
    std::string old_name = test_data->name();
    std::string new_name = "new_name";

    meta_store->renameDatabase(test_data->name(), new_name);
    EXPECT_FALSE(meta_store->isExistsDatabase(old_name));
    EXPECT_TRUE(meta_store->isExistsDatabase(new_name));

    meta_store->removeDatabaseMeta(new_name);
}
TEST_F(MetadataDatabaseTest, removeDatabaseMeta)
{
    meta_store->addDatabaseMeta(*test_data, {test_data->name()}, uuid);
    meta_store->removeDatabaseMeta(test_data->name());
    EXPECT_FALSE(meta_store->isExistsDatabase(test_data->name()));
}
