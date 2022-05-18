#include <gtest/gtest.h>
#include <MetadataTable.pb.h>
#include "common.h"
using namespace DB;
using namespace std::string_literals;

using MetadataTable = FoundationDB::Proto::MetadataTable;
class MetadataTableTest : public testing::Test
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
        if (!config_file)
        {
            GTEST_SKIP() << "If no foundationdb is used, skip this test";
        }
        meta_store = createMetadataStore(config_file);
        test_data = new MetadataTable();
        db_uuid.toUnderType().items[0] = 0;
        db_uuid.toUnderType().items[1] = 1;
        table_name = "test";
        test_data->set_sql(table_name);

        if (meta_store->isExistsTable({db_uuid, table_name}))
        {
            meta_store->removeTableMeta({db_uuid, table_name});
        }
    }

    static void TearDownTestSuite()
    {
        delete test_data;
        test_data = nullptr;
    }
    static UUID db_uuid;
    static std::string table_name;
    static MetadataTable * test_data;
    static std::shared_ptr<DB::MetadataStoreFoundationDB> meta_store;
};
UUID MetadataTableTest::db_uuid;
std::string MetadataTableTest::table_name;
FoundationDB::Proto::MetadataTable * MetadataTableTest::test_data = nullptr;
std::shared_ptr<DB::MetadataStoreFoundationDB> MetadataTableTest::meta_store;


TEST_F(MetadataTableTest, addTableMeta)
{
    meta_store->addTableMeta(*test_data, {db_uuid, table_name});
    ASSERT_TRUE(meta_store->isExistsTable({db_uuid, table_name}));

    meta_store->removeTableMeta({db_uuid, table_name});
}

TEST_F(MetadataTableTest, getTableMeta)
{
    meta_store->addTableMeta(*test_data, {db_uuid, table_name});
    auto ret_test_data = meta_store->getTableMeta({db_uuid, table_name});
    std::string old_;
    test_data->SerializeToString(&old_);
    std::string new_;
    ret_test_data->SerializeToString(&new_);
    EXPECT_EQ(old_, new_);

    meta_store->removeTableMeta({db_uuid, table_name});
}

TEST_F(MetadataTableTest, updateTableMeta)
{
    meta_store->addTableMeta(*test_data, {db_uuid, table_name});
    test_data->set_sql(table_name + "update");
    meta_store->updateTableMeta(*test_data, {db_uuid, table_name});
    auto ret_test_data = meta_store->getTableMeta({db_uuid, table_name});
    std::string old_;
    test_data->SerializeToString(&old_);
    std::string new_;
    ret_test_data->SerializeToString(&new_);
    EXPECT_EQ(old_, new_);
    test_data->set_sql(table_name);
    meta_store->removeTableMeta({db_uuid, table_name});
}
TEST_F(MetadataTableTest, removeTableMeta)
{
    meta_store->addTableMeta(*test_data, {db_uuid, table_name});
    meta_store->removeTableMeta({db_uuid, table_name});
    EXPECT_FALSE(meta_store->isExistsTable({db_uuid, table_name}));
}

TEST_F(MetadataTableTest, renameTable)
{
    meta_store->addTableMeta(*test_data, {db_uuid, table_name});
    std::string old_name = table_name;
    std::string new_name = "new_name";

    meta_store->renameTable({db_uuid, old_name}, {db_uuid, new_name}, *test_data);
    EXPECT_FALSE(meta_store->isExistsTable({db_uuid, old_name}));
    EXPECT_TRUE(meta_store->isExistsTable({db_uuid, new_name}));

    meta_store->removeTableMeta({db_uuid, new_name});
}
TEST_F(MetadataTableTest, renameTableToDropped)
{
    UUID new_table_uuid;
    new_table_uuid.toUnderType().items[0] = 0;
    new_table_uuid.toUnderType().items[1] = 1;
    meta_store->addTableMeta(*test_data, {db_uuid, table_name});
    if (meta_store->isDropped(new_table_uuid))
        meta_store->removeDroppedTableMeta(new_table_uuid);
    meta_store->renameTableToDropped({db_uuid, table_name}, new_table_uuid);
    EXPECT_FALSE(meta_store->isExistsTable({db_uuid, table_name}));
    auto ret_test_data = meta_store->getDroppedTableMeta(new_table_uuid);
    std::string old_;
    test_data->SerializeToString(&old_);
    std::string new_;
    ret_test_data->SerializeToString(&new_);
    EXPECT_EQ(old_, new_);
    meta_store->removeDroppedTableMeta(new_table_uuid);
}
TEST_F(MetadataTableTest, exchangeTableMeta)
{
    std::string old_name = table_name;
    std::string new_name = "new_name";
    auto new_data = new MetadataTable();
    new_data->set_sql(new_name);
    if (meta_store->isExistsTable({db_uuid, new_name}))
    {
        meta_store->removeTableMeta({db_uuid, new_name});
    }
    meta_store->addTableMeta(*test_data, {db_uuid, old_name});
    meta_store->addTableMeta(*new_data, {db_uuid, new_name});
    meta_store->exchangeTableMeta({db_uuid, old_name}, {db_uuid, new_name}, *test_data, *new_data);
    auto ret_test_data_old = meta_store->getTableMeta({db_uuid, old_name});
    auto ret_test_data_new = meta_store->getTableMeta({db_uuid, new_name});
    std::string s_new;
    new_data->SerializeToString(&s_new);
    std::string s_old;
    test_data->SerializeToString(&s_old);
    std::string s_old_ret;
    ret_test_data_old->SerializeToString(&s_old_ret);
    std::string s_new_ret;
    ret_test_data_new->SerializeToString(&s_new_ret);
    EXPECT_EQ(s_new, s_old_ret);
    EXPECT_EQ(s_old, s_new_ret);


    meta_store->removeTableMeta({db_uuid, old_name});
    meta_store->removeTableMeta({db_uuid, new_name});
}

TEST_F(MetadataTableTest, isExistsTable)
{
    meta_store->addTableMeta(*test_data, {db_uuid, table_name});
    EXPECT_TRUE(meta_store->isExistsTable({db_uuid, table_name}));
    meta_store->removeTableMeta({db_uuid, table_name});
    EXPECT_FALSE(meta_store->isExistsTable({db_uuid, table_name}));
}
TEST_F(MetadataTableTest, updateDatachTableStatus_isDatached)
{
    meta_store->addTableMeta(*test_data, {db_uuid, table_name});
    EXPECT_FALSE(meta_store->isDetached({db_uuid, table_name}));
    meta_store->updateDetachTableStatus({db_uuid, table_name}, true);
    EXPECT_TRUE(meta_store->isDetached({db_uuid, table_name}));
    meta_store->removeTableMeta({db_uuid, table_name});
}
TEST_F(MetadataTableTest, getModificationTime)
{
    meta_store->addTableMeta(*test_data, {db_uuid, table_name});
    test_data->set_sql(table_name + "_update");
    auto first = meta_store->getModificationTime({db_uuid, table_name});
    sleep(1);
    meta_store->updateTableMeta(*test_data, {db_uuid, table_name});
    auto sec = meta_store->getModificationTime({db_uuid, table_name});
    EXPECT_NE(first, sec);
    test_data->set_sql(table_name);
    meta_store->removeTableMeta({db_uuid, table_name});
}
TEST_F(MetadataTableTest, listAllTableMeta)
{
    int test_count = 10;
    std::vector<MetadataTable> list_tests(test_count);
    std::string base_table = "list_test";
    for (int i = 0; i < test_count; i++)
    {
        std::string tmp_table_name = base_table + std::to_string(i);

        meta_store->removeTableMeta({db_uuid, tmp_table_name});
        list_tests[i].set_sql(tmp_table_name);
        meta_store->addTableMeta(list_tests[i], {db_uuid, tmp_table_name});
    }
    auto res = meta_store->listAllTableMeta(db_uuid);
    for (int i = 0; i < test_count; i++)
    {
        std::string tmp_table_name = base_table + std::to_string(i);
        std::string old_;
        list_tests[i].SerializeToString(&old_);
        EXPECT_TRUE(
            std::find_if(
                res.begin(),
                res.end(),
                [&](auto & s) {
                    std::string new_;
                    s->SerializeToString(&new_);
                    return new_ == old_;
                })
            != res.end());
        meta_store->removeTableMeta({db_uuid, tmp_table_name});
    }
}
TEST_F(MetadataTableTest, listAllDroppedTableMeta)
{
    int test_count = 10;
    std::vector<MetadataTable> list_tests(test_count);
    std::string base_table = "list_test";
    for (int i = 0; i < test_count; i++)
    {
        std::string tmp_table_name = base_table + std::to_string(i);
        if (meta_store->isExistsTable({db_uuid, tmp_table_name}))
            meta_store->removeTableMeta({db_uuid, tmp_table_name});
        list_tests[i].set_sql(tmp_table_name);
        meta_store->addTableMeta(list_tests[i], {db_uuid, tmp_table_name});
    }
    for (int i = 0; i < test_count; i++)
    {
        UUID tmp_table_uuid;
        tmp_table_uuid.toUnderType().items[0] = i;
        tmp_table_uuid.toUnderType().items[1] = i;
        std::string tmp_table_name = base_table + std::to_string(i);
        if (meta_store->isDropped(tmp_table_uuid))
            meta_store->removeDroppedTableMeta(tmp_table_uuid);
        meta_store->renameTableToDropped({db_uuid, tmp_table_name}, tmp_table_uuid);
    }
    auto res = meta_store->listAllDroppedTableMeta();
    for (int i = 0; i < test_count; i++)
    {
        UUID tmp_table_uuid;
        tmp_table_uuid.toUnderType().items[0] = i;
        tmp_table_uuid.toUnderType().items[1] = i;
        std::string tmp_table_name = base_table + std::to_string(i);
        EXPECT_TRUE(std::find_if(res.begin(), res.end(), [&](auto & s) { return s->sql() == tmp_table_name; }) != res.end());
        meta_store->removeDroppedTableMeta(tmp_table_uuid);
    }
}
