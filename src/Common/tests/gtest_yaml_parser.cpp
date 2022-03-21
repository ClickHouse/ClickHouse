#include <Common/config.h>

#if USE_YAML_CPP
#include "gtest_helper_functions.h"
#include <base/scope_guard.h>
#include <Common/Config/YAMLParser.h>
#include <Common/Config/ConfigHelper.h>
#include <Poco/AutoPtr.h>
#include "Poco/DOM/Document.h"

#include <gtest/gtest.h>


using namespace DB;

TEST(Common, YamlParserInvalidFile)
{
    ASSERT_THROW(YAMLParser::parse("some-non-existing-file.yaml"), Exception);
}

TEST(Common, YamlParserProcessKeysList)
{
    auto yaml_file = getFileWithContents("keys-list.yaml", R"YAML(
operator:
    access_management: "1"
    networks:
      - ip: "10.1.6.168"
      - ip: "::1"
      - ip: "127.0.0.1"
)YAML");
    SCOPE_EXIT({ yaml_file->remove(); });

    Poco::AutoPtr<Poco::XML::Document> xml = YAMLParser::parse(yaml_file->path());
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
    auto yaml_file = getFileWithContents("values-list.yaml", R"YAML(
rules:
  - apiGroups: [""]
    resources:
      - nodes
      - nodes/proxy
      - services
      - endpoints
      - pods
)YAML");
    SCOPE_EXIT({ yaml_file->remove(); });

    Poco::AutoPtr<Poco::XML::Document> xml = YAMLParser::parse(yaml_file->path());
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
