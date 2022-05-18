#include <gtest/gtest.h>
#include <MetadataConfigParam.pb.h>
#include "common.h"

using namespace DB;
using namespace std::string_literals;
using MetadataConfigParam = FoundationDB::Proto::MetadataConfigParam;
class MetadataConfigParamTest : public testing::Test
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
        meta_store = createMetadataStore(config_file);
        //Test data
        test_data = new MetadataConfigParam();
        test_data->set_name("test");
        test_data->set_value("test");
        //clear
        if (meta_store->isExistsConfigParamMeta(test_data->name()))
        {
            meta_store->removeConfigParamMeta(test_data->name());
        }
    }

    static void TearDownTestSuite()
    {
        delete test_data;
        test_data = nullptr;
    }
    static MetadataConfigParam * test_data;
    static std::shared_ptr<DB::MetadataStoreFoundationDB> meta_store;
};
FoundationDB::Proto::MetadataConfigParam * MetadataConfigParamTest::test_data = nullptr;
std::shared_ptr<DB::MetadataStoreFoundationDB> MetadataConfigParamTest::meta_store;

TEST_F(MetadataConfigParamTest, addConfigParamMeta)
{
    meta_store->addConfigParamMeta(*test_data, test_data->name());
    EXPECT_TRUE(meta_store->isExistsConfigParamMeta(test_data->name()));

    meta_store->removeConfigParamMeta(test_data->name());
}
TEST_F(MetadataConfigParamTest, addBunchConfigParamMeta)
{
    int test_count = 1000;
    std::vector<std::unique_ptr<MetadataConfigParam>> list_tests;
    std::string base_cofig = "list_test";
    for (int i = 0; i < test_count; i++)
    {
        std::string config_name = base_cofig + std::to_string(i);
        meta_store->removeConfigParamMeta(config_name);
        std::unique_ptr<MetadataConfigParam> tmp(new MetadataConfigParam());
        tmp->set_name(config_name);
        list_tests.push_back(std::move(tmp));
    }
    meta_store->addBunchConfigParamMeta(list_tests);
    for (int i = 0; i < test_count; i++)
    {
        std::string config_name = base_cofig + std::to_string(i);
        EXPECT_TRUE(meta_store->isExistsConfigParamMeta(config_name));
        meta_store->removeConfigParamMeta(config_name);
    }
}
TEST_F(MetadataConfigParamTest, getConfigMeta)
{
    meta_store->addConfigParamMeta(*test_data, test_data->name());
    auto ret_test_data = meta_store->getConfigParamMeta(test_data->name());
    std::string old_;
    test_data->SerializeToString(&old_);
    std::string new_;
    ret_test_data->SerializeToString(&new_);
    EXPECT_EQ(old_, new_);

    meta_store->removeConfigParamMeta(test_data->name());
}
TEST_F(MetadataConfigParamTest, listAllConfigParamMeta)
{
    int test_count = 10;
    std::vector<MetadataConfigParam> list_tests(test_count);
    std::string base_cofig = "list_test";
    for (int i = 0; i < test_count; i++)
    {
        std::string config_name = base_cofig + std::to_string(i);
        meta_store->removeConfigParamMeta(config_name);
        list_tests[i].set_name(config_name);
        list_tests[i].set_value(config_name + "_value");
        meta_store->addConfigParamMeta(list_tests[i], list_tests[i].name());
    }
    auto res = meta_store->listAllConfigParamMeta();
    for (int i = 0; i < test_count; i++)
    {
        EXPECT_TRUE(
            std::find_if(
                res.begin(), res.end(), [&](auto & s) { return s->value() == list_tests[i].value() && s->name() == list_tests[i].name(); })
            != res.end());
        meta_store->removeConfigParamMeta(list_tests[i].name());
    }
}
TEST_F(MetadataConfigParamTest, isExistsConfigParamMeta)
{
    meta_store->addConfigParamMeta(*test_data, test_data->name());
    EXPECT_TRUE(meta_store->isExistsConfigParamMeta(test_data->name()));
    meta_store->removeConfigParamMeta(test_data->name());
    EXPECT_FALSE(meta_store->isExistsConfigParamMeta(test_data->name()));
}
TEST_F(MetadataConfigParamTest, updateConfigParamMeta)
{
    meta_store->addConfigParamMeta(*test_data, test_data->name());
    test_data->set_value("new_test");
    meta_store->updateConfigParamMeta(*test_data, test_data->name());
    auto ret_test_data = meta_store->getConfigParamMeta(test_data->name());
    std::string old_;
    test_data->SerializeToString(&old_);
    std::string new_;
    ret_test_data->SerializeToString(&new_);
    EXPECT_EQ(old_, new_);
    test_data->set_value("test");
    meta_store->removeConfigParamMeta(test_data->name());
}

TEST_F(MetadataConfigParamTest, removeConfigParamMeta)
{
    meta_store->addConfigParamMeta(*test_data, test_data->name());
    meta_store->removeConfigParamMeta(test_data->name());
    EXPECT_FALSE(meta_store->isExistsConfigParamMeta(test_data->name()));
}
