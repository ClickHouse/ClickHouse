#include <Common/tests/gtest_global_context.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/DOM/DOMParser.h>
#include <gtest/gtest.h>

using namespace DB;

/// A class which allows to test private methods of NamedCollectionFactory.
class NamedCollectionFactoryFriend : public NamedCollectionFactory
{
public:
    static NamedCollectionFactoryFriend & instance()
    {
        static NamedCollectionFactoryFriend instance;
        return instance;
    }

    void loadFromConfig(const Poco::Util::AbstractConfiguration & config)
    {
        std::lock_guard lock(mutex);
        NamedCollectionFactory::loadFromConfig(config, lock);
    }

    void add(const std::string & collection_name, MutableNamedCollectionPtr collection)
    {
        std::lock_guard lock(mutex);
        NamedCollectionFactory::add(collection_name, collection, lock);
    }

    void remove(const std::string & collection_name)
    {
        std::lock_guard lock(mutex);
        NamedCollectionFactory::remove(collection_name, lock);
    }
};

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

    NamedCollectionFactoryFriend::instance().loadFromConfig(*config);

    ASSERT_TRUE(NamedCollectionFactoryFriend::instance().exists("collection1"));
    ASSERT_TRUE(NamedCollectionFactoryFriend::instance().exists("collection2"));
    ASSERT_TRUE(NamedCollectionFactoryFriend::instance().tryGet("collection3") == nullptr);

    auto collections = NamedCollectionFactoryFriend::instance().getAll();
    ASSERT_EQ(collections.size(), 2);
    ASSERT_TRUE(collections.contains("collection1"));
    ASSERT_TRUE(collections.contains("collection2"));

    ASSERT_EQ(collections["collection1"]->dumpStructure(),
              R"CONFIG(key1:	value1
key2:	2
key3:	3.3
key4:	-4
)CONFIG");

    auto collection1 = NamedCollectionFactoryFriend::instance().get("collection1");
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

    auto collection2 = NamedCollectionFactoryFriend::instance().get("collection2");
    ASSERT_TRUE(collection2 != nullptr);

    ASSERT_TRUE(collection2->get<String>("key4") == "value4");
    ASSERT_TRUE(collection2->get<UInt64>("key5") == 5);
    ASSERT_TRUE(collection2->get<Float64>("key6") == 6.6);

    auto collection2_copy = collections["collection2"]->duplicate();
    NamedCollectionFactoryFriend::instance().add("collection2_copy", collection2_copy);
    ASSERT_TRUE(NamedCollectionFactoryFriend::instance().exists("collection2_copy"));
    ASSERT_EQ(NamedCollectionFactoryFriend::instance().get("collection2_copy")->dumpStructure(),
              R"CONFIG(key4:	value4
key5:	5
key6:	6.6
)CONFIG");

    collection2_copy->setOrUpdate<String>("key4", "value44", {});
    ASSERT_EQ(collection2_copy->get<String>("key4"), "value44");
    ASSERT_EQ(collection2->get<String>("key4"), "value4");

    collection2_copy->remove("key4");
    ASSERT_EQ(collection2_copy->getOrDefault<String>("key4", "N"), "N");
    ASSERT_EQ(collection2->getOrDefault<String>("key4", "N"), "value4");

    collection2_copy->setOrUpdate<String>("key4", "value45", {});
    ASSERT_EQ(collection2_copy->getOrDefault<String>("key4", "N"), "value45");

    NamedCollectionFactoryFriend::instance().remove("collection2_copy");
    ASSERT_FALSE(NamedCollectionFactoryFriend::instance().exists("collection2_copy"));

    config.reset();
}

TEST(NamedCollections, NestedConfig)
{
    std::string xml(R"CONFIG(<clickhouse>
    <named_collections>
        <collection3>
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
        </collection3>
    </named_collections>
</clickhouse>)CONFIG");

    Poco::XML::DOMParser dom_parser;
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);

    NamedCollectionFactoryFriend::instance().loadFromConfig(*config);

    ASSERT_TRUE(NamedCollectionFactoryFriend::instance().exists("collection3"));

    auto collection = NamedCollectionFactoryFriend::instance().get("collection3");
    ASSERT_TRUE(collection != nullptr);

    ASSERT_EQ(collection->dumpStructure(),
              R"CONFIG(key1:
	key1_1:	value1
key2:
	key2_1:	value2_1
	key2_2:
		key2_3:
			key2_4:	4
			key2_5:	5
)CONFIG");

    ASSERT_EQ(collection->get<String>("key1.key1_1"), "value1");
    ASSERT_EQ(collection->get<String>("key2.key2_1"), "value2_1");
    ASSERT_EQ(collection->get<Int64>("key2.key2_2.key2_3.key2_4"), 4);
    ASSERT_EQ(collection->get<Int64>("key2.key2_2.key2_3.key2_5"), 5);

}

TEST(NamedCollections, NestedConfigDuplicateKeys)
{
    std::string xml(R"CONFIG(<clickhouse>
    <named_collections>
        <collection>
            <headers>
                <header>
                    <name>key1</name>
                    <value>value1</value>
                </header>
                <header>
                    <name>key2</name>
                    <value>value2</value>
                </header>
                <header>
                    <name>key3</name>
                    <value>value3</value>
                </header>
            </headers>
        </collection>
    </named_collections>
</clickhouse>)CONFIG");

    Poco::XML::DOMParser dom_parser;
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);

    NamedCollectionFactoryFriend::instance().loadFromConfig(*config);
    auto collection = NamedCollectionFactoryFriend::instance().get("collection");

    auto keys = collection->getKeys();
    ASSERT_EQ(keys.size(), 6);

    ASSERT_TRUE(keys.contains("headers.header.name"));
    ASSERT_TRUE(keys.contains("headers.header[1].name"));
    ASSERT_TRUE(keys.contains("headers.header[2].name"));

    ASSERT_TRUE(keys.contains("headers.header.value"));
    ASSERT_TRUE(keys.contains("headers.header[1].value"));
    ASSERT_TRUE(keys.contains("headers.header[2].value"));

    ASSERT_EQ(collection->get<String>("headers.header.name"), "key1");
    ASSERT_EQ(collection->get<String>("headers.header[1].name"), "key2");
    ASSERT_EQ(collection->get<String>("headers.header[2].name"), "key3");

    ASSERT_EQ(collection->get<String>("headers.header.value"), "value1");
    ASSERT_EQ(collection->get<String>("headers.header[1].value"), "value2");
    ASSERT_EQ(collection->get<String>("headers.header[2].value"), "value3");

    keys = collection->getKeys(0);
    ASSERT_EQ(keys.size(), 1);
    ASSERT_TRUE(keys.contains("headers"));

    keys = collection->getKeys(0, "headers");
    ASSERT_EQ(keys.size(), 3);

    ASSERT_TRUE(keys.contains("headers.header"));
    ASSERT_TRUE(keys.contains("headers.header[1]"));
    ASSERT_TRUE(keys.contains("headers.header[2]"));

    keys = collection->getKeys(1);
    ASSERT_EQ(keys.size(), 3);

    ASSERT_TRUE(keys.contains("headers.header"));
    ASSERT_TRUE(keys.contains("headers.header[1]"));
    ASSERT_TRUE(keys.contains("headers.header[2]"));

    keys = collection->getKeys(2);
    ASSERT_EQ(keys.size(), 6);

    ASSERT_TRUE(keys.contains("headers.header.name"));
    ASSERT_TRUE(keys.contains("headers.header[1].name"));
    ASSERT_TRUE(keys.contains("headers.header[2].name"));

    ASSERT_TRUE(keys.contains("headers.header.value"));
    ASSERT_TRUE(keys.contains("headers.header[1].value"));
    ASSERT_TRUE(keys.contains("headers.header[2].value"));
}
