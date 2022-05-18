#include <gtest/gtest.h>
#include <MetadataACLEntry.pb.h>
#include "common.h"

using namespace DB;
using namespace std::string_literals;

using AccessEntity = FoundationDB::Proto::AccessEntity;
using MetadataUserEntry = FoundationDB::Proto::MetadataUserEntry;
using AccessEntityScope = DB::MetadataStoreFoundationDB::AccessEntityScope;
class AccessEntityTest : public testing::Test
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
        test_data = new AccessEntity();
        MetadataUserEntry user;
        user.set_default_database("test_database");
        test_data->set_name("test_name");
        *test_data->mutable_user() = user;
        uuid.toUnderType().items[0] = 0;
        uuid.toUnderType().items[1] = 0;

        if (meta_store->existsAccessEntity(scope, uuid))
        {
            meta_store->removeAccessEntity(scope, uuid, DB::AccessEntityType::USER, false);
        }
    }

    static void TearDownTestSuite()
    {
        delete test_data;
        test_data = nullptr;
    }
    static UUID uuid;
    static AccessEntityScope scope;
    static AccessEntity * test_data;
    static std::shared_ptr<DB::MetadataStoreFoundationDB> meta_store;
};
UUID AccessEntityTest::uuid;
FoundationDB::Proto::AccessEntity * AccessEntityTest::test_data = nullptr;
std::shared_ptr<DB::MetadataStoreFoundationDB> AccessEntityTest::meta_store;
AccessEntityScope AccessEntityTest::scope = DB::MetadataStoreFoundationDB::CONFIG;
TEST_F(AccessEntityTest, addAccessEntity)
{
    meta_store->addAccessEntity(scope, uuid, *test_data, false, false);
    EXPECT_TRUE(meta_store->existsAccessEntity(scope, uuid));

    meta_store->removeAccessEntity(scope, uuid, DB::AccessEntityType::USER, false);
}
TEST_F(AccessEntityTest, getAccessEntity)
{
    meta_store->addAccessEntity(scope, uuid, *test_data, false, false);
    auto ret_test_data = meta_store->getAccessEntity(scope, uuid, true);
    std::string old_;
    test_data->SerializeToString(&old_);
    std::string new_;
    ret_test_data->SerializeToString(&new_);
    EXPECT_EQ(old_, new_);

    meta_store->removeAccessEntity(scope, uuid, DB::AccessEntityType::USER, false);
}
TEST_F(AccessEntityTest, getAccessEntityListsByType)
{
    int test_count = 2;
    std::vector<AccessEntity> list_tests(test_count);
    std::string base_name = "list_test";
    for (int i = 0; i < test_count; i++)
    {
        std::string test_name = base_name + std::to_string(i);
        list_tests[i].set_name(test_name);
        MetadataUserEntry user;
        user.set_default_database(test_name + "_db");
        *list_tests[i].mutable_user() = user;
        UUID tmp_uuid;
        tmp_uuid.toUnderType().items[0] = i;
        tmp_uuid.toUnderType().items[1] = i;
        meta_store->removeAccessEntity(scope, tmp_uuid, DB::AccessEntityType::USER, false);
        meta_store->addAccessEntity(scope, tmp_uuid, list_tests[i], false, false);
    }
    auto res = meta_store->getAccessEntityListsByType(scope, DB::AccessEntityType::USER);
    for (int i = 0; i < test_count; i++)
    {
        UUID tmp_uuid;
        tmp_uuid.toUnderType().items[0] = i;
        tmp_uuid.toUnderType().items[1] = i;
        EXPECT_TRUE(
            std::find_if(res.begin(), res.end(), [&](auto & s) { return s.first == tmp_uuid && s.second == list_tests[i].name(); })
            != res.end());
        meta_store->removeAccessEntity(scope, tmp_uuid, DB::AccessEntityType::USER, false);
    }
}

TEST_F(AccessEntityTest, listAccessEntity)
{
    int test_count = 100;
    std::vector<AccessEntity> list_tests(test_count);
    std::string base_name = "list_test";
    for (int i = 0; i < test_count; i++)
    {
        std::string test_name = base_name + std::to_string(i);
        list_tests[i].set_name(test_name);
        MetadataUserEntry user;
        user.set_default_database(test_name + "_db");
        *list_tests[i].mutable_user() = user;
        UUID tmp_uuid;
        tmp_uuid.toUnderType().items[0] = i;
        tmp_uuid.toUnderType().items[1] = i;
        meta_store->removeAccessEntity(scope, tmp_uuid, DB::AccessEntityType::USER, false);
        meta_store->addAccessEntity(scope, tmp_uuid, list_tests[i], false, false);
    }
    auto res = meta_store->listAccessEntity(scope);
    for (int i = 0; i < test_count; i++)
    {
        UUID tmp_uuid;
        tmp_uuid.toUnderType().items[0] = i;
        tmp_uuid.toUnderType().items[1] = i;
        EXPECT_TRUE(std::find_if(res.begin(), res.end(), [&](auto & s) { return s == tmp_uuid; }) != res.end());
        meta_store->removeAccessEntity(scope, tmp_uuid, DB::AccessEntityType::USER, false);
    }
}
TEST_F(AccessEntityTest, existsAccessEntity)
{
    meta_store->addAccessEntity(scope, uuid, *test_data, false, false);
    EXPECT_TRUE(meta_store->existsAccessEntity(scope, uuid));
    meta_store->removeAccessEntity(scope, uuid, DB::AccessEntityType::USER, false);
    EXPECT_FALSE(meta_store->existsAccessEntity(scope, uuid));
}
TEST_F(AccessEntityTest, updateAccessEntity)
{
    meta_store->addAccessEntity(scope, uuid, *test_data, false, false);
    test_data->set_name("update");
    meta_store->updateAccessEntity(scope, uuid, *test_data, true);
    auto ret_test_data = meta_store->getAccessEntity(scope, uuid, true);
    std::string old_;
    test_data->SerializeToString(&old_);
    std::string new_;
    test_data->SerializeToString(&new_);
    EXPECT_EQ(old_, new_);
    test_data->set_name("test_name");
    meta_store->removeAccessEntity(scope, uuid, DB::AccessEntityType::USER, false);
}

TEST_F(AccessEntityTest, removeDatabaseMeta)
{
    meta_store->addAccessEntity(scope, uuid, *test_data, false, false);
    meta_store->removeAccessEntity(scope, uuid, DB::AccessEntityType::USER, false);
    EXPECT_FALSE(meta_store->existsAccessEntity(scope, uuid));
}
