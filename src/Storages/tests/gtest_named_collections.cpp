#include <Common/tests/gtest_global_context.h>
#include <Storages/NamedCollections.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/DOM/DOMParser.h>
#include <gtest/gtest.h>

using namespace DB;

TEST(NamedCollections, SimpleConfig)
{
    std::string xml(R"CONFIG(<clickhouse>
    <named_collections>
        <collection1>
            <key1>value1</key1>
            <key2>2</key2>
            <key3>3.3</key3>
            <key4>-4</key4>
        </collection1>
        <collection2>
            <key4>value4</key4>
            <key5>5</key5>
            <key6>6.6</key6>
        </collection2>
    </named_collections>
</clickhouse>)CONFIG");

    Poco::XML::DOMParser dom_parser;
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);

    NamedCollectionFactory::instance().initialize(*config);

    ASSERT_TRUE(NamedCollectionFactory::instance().exists("collection1"));
    ASSERT_TRUE(NamedCollectionFactory::instance().exists("collection2"));
    ASSERT_TRUE(NamedCollectionFactory::instance().tryGet("collection3") == nullptr);

    auto collections = NamedCollectionFactory::instance().getAll();
    ASSERT_EQ(collections.size(), 2);
    ASSERT_TRUE(collections.contains("collection1"));
    ASSERT_TRUE(collections.contains("collection2"));

    ASSERT_EQ(collections["collection1"]->dumpStructure(),
              R"CONFIG(key1:	value1
key2:	2
key3:	3.3
key4:	-4
)CONFIG");

    auto collection1 = NamedCollectionFactory::instance().get("collection1");
    ASSERT_TRUE(collection1 != nullptr);

    ASSERT_TRUE(collection1->get<String>("key1") == "value1");
    ASSERT_TRUE(collection1->get<UInt64>("key2") == 2);
    ASSERT_TRUE(collection1->get<Float64>("key3") == 3.3);
    ASSERT_TRUE(collection1->get<Int64>("key4") == -4);

    ASSERT_EQ(collections["collection2"]->dumpStructure(),
              R"CONFIG(key4:	value4
key5:	5
key6:	6.6
)CONFIG");

    auto collection2 = NamedCollectionFactory::instance().get("collection2");
    ASSERT_TRUE(collection2 != nullptr);

    ASSERT_TRUE(collection2->get<String>("key4") == "value4");
    ASSERT_TRUE(collection2->get<UInt64>("key5") == 5);
    ASSERT_TRUE(collection2->get<Float64>("key6") == 6.6);

    auto collection2_copy = collections["collection2"]->duplicate();
    NamedCollectionFactory::instance().add("collection2_copy", collection2_copy);
    ASSERT_TRUE(NamedCollectionFactory::instance().exists("collection2_copy"));
    ASSERT_EQ(NamedCollectionFactory::instance().get("collection2_copy")->dumpStructure(),
              R"CONFIG(key4:	value4
key5:	5
key6:	6.6
)CONFIG");

    collection2_copy->set<String>("key4", "value44", true);
    ASSERT_TRUE(collection2_copy->get<String>("key4") == "value44");
    ASSERT_TRUE(collection2->get<String>("key4") == "value4");

    collection2_copy->remove("key4");
    ASSERT_TRUE(collection2_copy->getOrDefault<String>("key4", "N") == "N");
    ASSERT_TRUE(collection2->getOrDefault<String>("key4", "N") == "value4");

    collection2_copy->set<String>("key4", "value45");
    ASSERT_TRUE(collection2_copy->getOrDefault<String>("key4", "N") == "value45");

    NamedCollectionFactory::instance().remove("collection2_copy");
    ASSERT_FALSE(NamedCollectionFactory::instance().exists("collection2_copy"));

    config.reset();
}

TEST(NamedCollections, NestedConfig)
{
    std::string xml(R"CONFIG(<clickhouse>
    <named_collections>
        <collection1>
            <key1>
                <key1_1>value1</key1_1>
            </key1>
            <key2>
                <key2_1>value2_1</key2_1>
                <key2_2>
                    <key2_3>
                        <key2_4>4</key2_4>
                        <key2_5>5</key2_5>
                    </key2_3>
                </key2_2>
            </key2>
        </collection1>
    </named_collections>
</clickhouse>)CONFIG");

    Poco::XML::DOMParser dom_parser;
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    NamedCollectionFactory::instance().reload(*config);

    ASSERT_TRUE(NamedCollectionFactory::instance().exists("collection1"));

    auto collection1 = NamedCollectionFactory::instance().get("collection1");
    ASSERT_TRUE(collection1 != nullptr);

    ASSERT_EQ(collection1->dumpStructure(),
              R"CONFIG(key1:
	key1_1:	value1
key2:
	key2_1:	value2_1
	key2_2:
		key2_3:
			key2_4:	4
			key2_5:	5
)CONFIG");

    ASSERT_EQ(collection1->get<String>("key1.key1_1"), "value1");
    ASSERT_EQ(collection1->get<String>("key2.key2_1"), "value2_1");
    ASSERT_EQ(collection1->get<Int64>("key2.key2_2.key2_3.key2_4"), 4);
    ASSERT_EQ(collection1->get<Int64>("key2.key2_2.key2_3.key2_5"), 5);

}
