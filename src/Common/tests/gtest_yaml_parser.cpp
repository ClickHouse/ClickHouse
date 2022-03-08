#include <Common/config.h>

#if USE_YAML_CPP
#include "gtest_helper_functions.h"
#include <Common/Config/YAMLParser.h>

#include <Common/Config/ConfigHelper.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>

#include <gtest/gtest.h>

#include "Poco/DOM/Document.h"


using namespace DB;

TEST(Common, YamlParserProcessList)
{
    ASSERT_THROW(YAMLParser::parse("some-non-existing-file.yaml"), Exception);

    Poco::AutoPtr<Poco::XML::Document> xml = YAMLParser::parse(resolvePath("src/Common/tests/gtest_yaml_test_config.yaml"));
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(xml);

    auto *p_node = xml->getNodeByPath("/clickhouse");
    EXPECT_EQ(xmlNodeAsString(p_node, ""), R"CONFIG(<clickhouse>
<operator>
<access_management>1</access_management>
<networks>
<ip>10.1.6.168</ip>
<ip>::1</ip>
<ip>127.0.0.1</ip>
</networks>
</operator>
</clickhouse>)CONFIG");

}
#endif
