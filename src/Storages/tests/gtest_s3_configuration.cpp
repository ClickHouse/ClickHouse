#include <gtest/gtest.h>
#include <config.h>

#if USE_AWS_S3

#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/tests/gtest_global_context.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/S3/Configuration.h>

#include <Poco/DOM/DOMParser.h>
#include <Poco/Util/XMLConfiguration.h>

using namespace DB;

namespace
{

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
};

S3StorageParsedArguments parseCollection(const String & name, const String & collection_keys)
{
    const String xml = "<clickhouse><named_collections><" + name + ">"
        "<url>http://localhost:11111/test/data.csv</url>" + collection_keys
        + "</" + name + "></named_collections></clickhouse>";

    Poco::XML::DOMParser dom_parser;
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    NamedCollectionFactoryFriend::instance().loadFromConfig(*config);

    auto collection = NamedCollectionFactoryFriend::instance().get(name);
    S3StorageParsedArguments parsed;
    parsed.fromNamedCollection(*collection, getContext().context);
    return parsed;
}

}

/// The storage refreshes its settings from the endpoint config, which would replace the checksum algorithms
/// of the collection, so they have to be recorded as explicit request settings.
TEST(S3Configuration, NamedCollectionChecksumAlgorithmsAreExplicit)
{
    auto parsed = parseCollection(
        "ChecksumAlgorithmsAreExplicit",
        "<upload_checksum_algorithm>SHA256</upload_checksum_algorithm>"
        "<delete_checksum_algorithm>CRC32</delete_checksum_algorithm>");

    EXPECT_EQ(parsed.explicit_request_settings["upload_checksum_algorithm"], "SHA256");
    EXPECT_EQ(parsed.explicit_request_settings["delete_checksum_algorithm"], "CRC32");
}

TEST(S3Configuration, NamedCollectionWithoutChecksumAlgorithmsHasNoExplicitSettings)
{
    auto parsed = parseCollection("NoChecksumAlgorithms", "");

    EXPECT_TRUE(parsed.explicit_request_settings.empty());
}

#endif
