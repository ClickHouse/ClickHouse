#include <Common/config.h>

#if USE_YAML_CPP
#include "gtest_helper_functions.h"
#include <Common/Config/YAMLParser.h>

#include <Common/Config/ConfigHelper.h>
#include <Poco/AutoPtr.h>

#include <gtest/gtest.h>

#include "Poco/DOM/Document.h"


using namespace DB;

TEST(Common, YamlParserProcessKeysList)
{
    ASSERT_THROW(YAMLParser::parse("some-non-existing-file.yaml"), Exception);

    Poco::AutoPtr<Poco::XML::Document> xml = YAMLParser::parse(resolvePath("src/Common/tests/gtest_yaml_test_config_keys_list.yaml"));
    auto *p_node = xml->getNodeByPath("/clickhouse");
    EXPECT_EQ(xmlNodeAsString(p_node), R"CONFIG(<clickhouse>
<operator>
<access_management>1</access_management>
<networks>
<ip>10.1.6.168</ip>
<ip>::1</ip>
<ip>127.0.0.1</ip>
</networks>
</operator>
</clickhouse>
)CONFIG");

}

TEST(Common, YamlParserProcessValuesList)
{
    ASSERT_THROW(YAMLParser::parse("some-non-existing-file.yaml"), Exception);

    Poco::AutoPtr<Poco::XML::Document> xml = YAMLParser::parse(resolvePath("src/Common/tests/gtest_yaml_test_config_values_list.yaml"));
    auto *p_node = xml->getNodeByPath("/clickhouse");
    EXPECT_EQ(xmlNodeAsString(p_node), R"CONFIG(<clickhouse>
<rules>
<apiGroups></apiGroups>
<resources>nodes</resources>
<resources>nodes/proxy</resources>
<resources>services</resources>
<resources>endpoints</resources>
<resources>pods</resources>
</rules>
</clickhouse>
)CONFIG");

}
#endif
