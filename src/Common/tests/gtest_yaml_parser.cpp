#include "config.h"

#if USE_YAML_CPP
#include <Common/tests/gtest_helper_functions.h>
#include <base/scope_guard.h>
#include <Common/Config/YAMLParser.h>
#include <Common/Config/ConfigHelper.h>
#include <Poco/AutoPtr.h>
#include <Poco/DOM/Document.h>

#include <gtest/gtest.h>


using namespace DB;

TEST(YamlParser, InvalidFile)
{
    ASSERT_THROW(YAMLParser::parse("some-non-existing-file.yaml"), Exception);
}

TEST(YamlParser, ProcessValuesList)
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

TEST(YamlParser, ProcessKeysList)
{
    auto yaml_file = getFileWithContents("keys-list.yaml", R"YAML(
operator:
    access_management: 1
    networks:
        ip:
          - 10.1.6.168
          - ::1
          - 127.0.0.1
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

TEST(YamlParser, ProcessListAttributes)
{
    auto yaml_file = getFileWithContents("list_attributes.yaml", R"YAML(
seq:
  - "@attr1": x
  - k1: val1
    k2: val2
    "@attr2": y
  - k3: val3
    "@attr3": z
)YAML");
    SCOPE_EXIT({ yaml_file->remove(); });

    Poco::AutoPtr<Poco::XML::Document> xml = YAMLParser::parse(yaml_file->path());
    auto *p_node = xml->getNodeByPath("/clickhouse");
    EXPECT_EQ(xmlNodeAsString(p_node), R"CONFIG(<clickhouse>
<seq attr1="x"></seq>
<seq attr2="y">
<k1>val1</k1>
<k2>val2</k2>
</seq>
<seq attr3="z">
<k3>val3</k3>
</seq>
</clickhouse>
)CONFIG");

}

TEST(YamlParser, ProcessMapAttributes)
{
    auto yaml_file = getFileWithContents("map_attributes.yaml", R"YAML(
map:
    "@attr1": x
    k1: val1
    k2: val2
    "@attr2": y
    k3: val3
    "@attr3": z
)YAML");
    SCOPE_EXIT({ yaml_file->remove(); });

    Poco::AutoPtr<Poco::XML::Document> xml = YAMLParser::parse(yaml_file->path());
    auto *p_node = xml->getNodeByPath("/clickhouse");
    EXPECT_EQ(xmlNodeAsString(p_node), R"CONFIG(<clickhouse>
<map attr1="x" attr2="y" attr3="z">
<k1>val1</k1>
<k2>val2</k2>
<k3>val3</k3>
</map>
</clickhouse>
)CONFIG");

}

TEST(YamlParser, ClusterDef)
{
    auto yaml_file = getFileWithContents("cluster_def.yaml", R"YAML(
test_cluster:
    shard:
        - internal_replication: false
          replica:
              - host: 127.0.0.1
                port: 9000
              - host: 127.0.0.2
                port: 9000
        - internal_replication: true
          replica:
              - host: 127.0.0.3
                port: 9000
              - host: 127.0.0.4
                port: 9000
)YAML");
    SCOPE_EXIT({ yaml_file->remove(); });

    Poco::AutoPtr<Poco::XML::Document> xml = YAMLParser::parse(yaml_file->path());
    auto *p_node = xml->getNodeByPath("/clickhouse");
    EXPECT_EQ(xmlNodeAsString(p_node), R"CONFIG(<clickhouse>
<test_cluster>
<shard>
<internal_replication>false</internal_replication>
<replica>
<host>127.0.0.1</host>
<port>9000</port>
</replica>
<replica>
<host>127.0.0.2</host>
<port>9000</port>
</replica>
</shard>
<shard>
<internal_replication>true</internal_replication>
<replica>
<host>127.0.0.3</host>
<port>9000</port>
</replica>
<replica>
<host>127.0.0.4</host>
<port>9000</port>
</replica>
</shard>
</test_cluster>
</clickhouse>
)CONFIG");

}

#endif
