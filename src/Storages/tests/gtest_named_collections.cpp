#include <Common/tests/gtest_global_context.h>
#include <Storages/NamedCollections.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/DOM/DOMParser.h>
#include <gtest/gtest.h>

using namespace DB;

TEST(NamedCollections, Simple)
{
    std::string xml(R"CONFIG(<clickhouse>
    <named_collections>
        <collection1>
            <key1>value1</key1>
            <key2>2</key2>
            <key3>-3</key3>
            <key4>4.4</key4>
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
    ASSERT_TRUE(NamedCollectionFactory::instance().tryGet("collection3", {}) == nullptr);

    using ValueInfo = NamedCollectionValueInfo;
    ValueInfo string_def{Field::Types::Which::String, std::nullopt, true};
    ValueInfo uint_def{Field::Types::Which::UInt64, std::nullopt, true};
    ValueInfo int_def{Field::Types::Which::Int64, std::nullopt, true};
    ValueInfo double_def{Field::Types::Which::Float64, std::nullopt, true};

    NamedCollectionInfo collection1_info;
    collection1_info.emplace("key1", string_def);
    collection1_info.emplace("key2", uint_def);
    collection1_info.emplace("key3", int_def);
    collection1_info.emplace("key4", double_def);

    auto collection1 = NamedCollectionFactory::instance().get("collection1", collection1_info);
    ASSERT_TRUE(collection1 != nullptr);

    ASSERT_TRUE(collection1->get("key1").safeGet<String>() == "value1");
    ASSERT_TRUE(collection1->get("key2").safeGet<UInt64>() == 2);
    ASSERT_TRUE(collection1->get("key3").safeGet<Int64>() == -3);
    ASSERT_TRUE(collection1->get("key4").safeGet<Float64>() == 4.4);

    NamedCollectionInfo collection2_info;
    collection2_info.emplace("key4", string_def);
    collection2_info.emplace("key5", uint_def);
    collection2_info.emplace("key6", double_def);

    auto collection2 = NamedCollectionFactory::instance().get("collection2", collection2_info);
    ASSERT_TRUE(collection2 != nullptr);

    ASSERT_TRUE(collection2->get("key4").safeGet<String>() == "value4");
    ASSERT_TRUE(collection2->get("key5").safeGet<UInt64>() == 5);
    ASSERT_TRUE(collection2->get("key6").safeGet<Int64>() == 6.6);

    auto mutable_collection2 = collection2->copy();
    mutable_collection2->replace("key4", UInt64(4));
    mutable_collection2->replace("key7", "value7");

    ASSERT_TRUE(mutable_collection2->get("key4").safeGet<UInt64>() == 4);
    ASSERT_TRUE(mutable_collection2->get("key7").safeGet<String>() == "value7");
}
