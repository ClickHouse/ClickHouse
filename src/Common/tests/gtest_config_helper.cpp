#include <Common/Config/ConfigHelper.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/DOM/DOMParser.h>

#include <gtest/gtest.h>


using namespace DB;

TEST(Common, ConfigHelperGetBool)
{
    std::string xml(R"CONFIG(<clickhouse>
    <zero_as_false>0</zero_as_false>
    <one_as_true>1</one_as_true>
    <yes_as_true>Yes</yes_as_true>
    <empty_as_true_1/>
    <empty_as_true_2></empty_as_true_2>
    <has_empty_child_1><empty_child/></has_empty_child_1>
    <has_empty_child_2><empty_child/><child>1</child></has_empty_child_2>
    <has_child_1><child>1</child></has_child_1>
    <has_child_2><child0>Yes</child0><child>1</child></has_child_2>
</clickhouse>)CONFIG");

    Poco::XML::DOMParser dom_parser;
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    EXPECT_EQ(ConfigHelper::getBool(*config, "zero_as_false", false, true), false);
    EXPECT_EQ(ConfigHelper::getBool(*config, "one_as_true", false, true), true);
    EXPECT_EQ(ConfigHelper::getBool(*config, "yes_as_true", false, true), true);
    EXPECT_EQ(ConfigHelper::getBool(*config, "empty_as_true_1", false, true), true);
    EXPECT_EQ(ConfigHelper::getBool(*config, "empty_as_true_2", false, true), true);
    ASSERT_THROW(ConfigHelper::getBool(*config, "has_empty_child_1", false, true), Poco::Exception);
    EXPECT_EQ(ConfigHelper::getBool(*config, "has_empty_child_2", false, true), true);
    EXPECT_EQ(ConfigHelper::getBool(*config, "has_child_1", false, true), true);
    ASSERT_THROW(ConfigHelper::getBool(*config, "has_child_2", false, true), Poco::Exception);
}
