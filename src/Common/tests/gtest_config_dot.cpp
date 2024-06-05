#include <Common/Config/ConfigHelper.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/DOM/DOMParser.h>

#include <gtest/gtest.h>


using namespace DB;

TEST(Common, ConfigWithDotInKeys)
{
    std::string xml(R"CONFIG(<clickhouse>
    <foo.bar>1</foo.bar>
</clickhouse>)CONFIG");

    Poco::XML::DOMParser dom_parser;
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);

    /// directly
    EXPECT_EQ(ConfigHelper::getBool(*config, "foo.bar", false, false), false);
    EXPECT_EQ(ConfigHelper::getBool(*config, "foo\\.bar", false, false), true);

    /// via keys()
    Poco::Util::AbstractConfiguration::Keys keys;
    config->keys("", keys);
    ASSERT_EQ(1, keys.size());
    ASSERT_EQ("foo\\.bar", keys[0]);
}
