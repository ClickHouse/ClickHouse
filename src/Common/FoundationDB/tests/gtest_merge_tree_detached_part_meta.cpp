#include <gtest/gtest.h>
#include <MergeTreePartInfo.pb.h>
#include "common.h"

using namespace DB;
using namespace std::string_literals;
using MergeTreeDetachedPartMeta = FoundationDB::Proto::MergeTreeDetachedPartMeta;
class MergeTreeDetachedPartMetaTest : public testing::Test
{
protected:
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
        table_uuid.toUnderType().items[0] = 0;
        table_uuid.toUnderType().items[1] = 1;
        part_name = "test";
        test_data = new MergeTreeDetachedPartMeta();
        test_data->set_disk_name("test");
        test_data->set_dir_name("test");
        if (meta_store->isExistsDetachedPart({table_uuid, part_name}))
        {
            meta_store->removeDetachedPartMeta({table_uuid, part_name});
        }
    }

    static void TearDownTestSuite()
    {
        delete test_data;
        test_data = nullptr;
    }
    static UUID table_uuid;
    static std::string part_name;
    static MergeTreeDetachedPartMeta * test_data;

    static std::shared_ptr<DB::MetadataStoreFoundationDB> meta_store;
};
FoundationDB::Proto::MergeTreeDetachedPartMeta * MergeTreeDetachedPartMetaTest::test_data = nullptr;
std::shared_ptr<DB::MetadataStoreFoundationDB> MergeTreeDetachedPartMetaTest::meta_store;
UUID MergeTreeDetachedPartMetaTest::table_uuid;
std::string MergeTreeDetachedPartMetaTest::part_name;
TEST_F(MergeTreeDetachedPartMetaTest, addPartDetachedMeta)
{
    meta_store->removeDetachedPartMeta({table_uuid, part_name});
    meta_store->addPartDetachedMeta(*test_data, {table_uuid, part_name});
    EXPECT_TRUE(meta_store->isExistsDetachedPart({table_uuid, part_name}));

    meta_store->removeDetachedPartMeta({table_uuid, part_name});
}
TEST_F(MergeTreeDetachedPartMetaTest, getDetachedPartMeta)
{
    meta_store->addPartDetachedMeta(*test_data, {table_uuid, part_name});
    auto ret_test_data = meta_store->getDetachedPartMeta({table_uuid, part_name});
    std::string old_;
    test_data->SerializeToString(&old_);
    std::string new_;
    ret_test_data->SerializeToString(&new_);
    EXPECT_EQ(old_, new_);

    meta_store->removeDetachedPartMeta({table_uuid, part_name});
}
TEST_F(MergeTreeDetachedPartMetaTest, listDetachedParts)
{
    int test_count = 10;
    std::vector<MergeTreeDetachedPartMeta> list_tests(test_count);
    std::string base_part = "list_test";
    for (int i = 0; i < test_count; i++)
    {
        std::string tmp_name = base_part + std::to_string(i);

        meta_store->removeDetachedPartMeta({table_uuid, tmp_name});
        list_tests[i].set_disk_name(tmp_name);
        list_tests[i].set_dir_name(tmp_name + "_value");
        meta_store->addPartDetachedMeta(list_tests[i], {table_uuid, tmp_name});
    }
    auto res = meta_store->listDetachedParts(table_uuid);
    for (int i = 0; i < test_count; i++)
    {
        std::string tmp_name = base_part + std::to_string(i);
        EXPECT_TRUE(
            std::find_if(
                res.begin(),
                res.end(),
                [&](auto & s) { return s->disk_name() == list_tests[i].disk_name() && s->dir_name() == list_tests[i].dir_name(); })
            != res.end());
        meta_store->removeDetachedPartMeta({table_uuid, tmp_name});
    }
}
TEST_F(MergeTreeDetachedPartMetaTest, isExistsDetachedPart)
{
    meta_store->addPartDetachedMeta(*test_data, {table_uuid, part_name});
    EXPECT_TRUE(meta_store->isExistsDetachedPart({table_uuid, part_name}));
    meta_store->removeDetachedPartMeta({table_uuid, part_name});
    EXPECT_FALSE(meta_store->isExistsDetachedPart({table_uuid, part_name}));
}
TEST_F(MergeTreeDetachedPartMetaTest, removeDetachedPartMeta)
{
    meta_store->addPartDetachedMeta(*test_data, {table_uuid, part_name});
    meta_store->removeDetachedPartMeta({table_uuid, part_name});
    EXPECT_FALSE(meta_store->isExistsDetachedPart({table_uuid, part_name}));
}
