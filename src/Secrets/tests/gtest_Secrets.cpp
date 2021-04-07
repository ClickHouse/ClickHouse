#include <Secrets/SecretsManager.h>

#include <IO/WriteBufferFromOStream.h>
#include <Common/hex.h>

#include <Poco/Util/XMLConfiguration.h>

#include <string_view>
#include <gtest/gtest.h>

namespace
{
using namespace DB;
using namespace std::literals;
}

TEST(SecretsManager, ConfigSecretFlat)
{
    std::istringstream      // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        xml_isteam(R"END(<?xml version="1.0"?>
<root>
    <secrets>
        <super_secret>foo value</super_secret>
    </secrets>
</root>)END");

    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(xml_isteam);

    std::shared_ptr secret_manager = std::make_shared<SecretsManager>();
    secret_manager->loadScrets(*config);

    EXPECT_EQ("foo value", secret_manager->getSecretValue("super_secret").str());
}

TEST(SecretsManager, ConfigSecretFlatEncoded)
{
    std::istringstream      // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        xml_isteam(R"END(<?xml version="1.0"?>
<root>
    <secrets>
        <super_secret encoding="hex">010203666F6F200076616C7565040506</super_secret>
    </secrets>
</root>)END");

    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(xml_isteam);

    std::shared_ptr secret_manager = std::make_shared<SecretsManager>();
    secret_manager->loadScrets(*config);

    EXPECT_EQ("\x01\x02\x03""foo \0value\x04\x05\x06"sv, secret_manager->getSecretValue("super_secret").str());
}

TEST(SecretsManager, ConfigSecretFlatEncodedInvalidData)
{
    std::istringstream      // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        xml_isteam(R"END(<?xml version="1.0"?>
<root>
    <secrets>
        <super_secret encoding="hex">Z10203666F6F200076616C7565040506</super_secret>
    </secrets>
</root>)END");

    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(xml_isteam);

    std::shared_ptr secret_manager = std::make_shared<SecretsManager>();
    EXPECT_THROW(secret_manager->loadScrets(*config), Exception);
}

TEST(SecretsManager, ConfigSecretNested)
{
    std::istringstream      // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        xml_isteam(R"END(<?xml version="1.0"?>
<root>
    <secrets>
        <super_secret>
            foobar
            <a>a</a>
            <b>b</b>
            <c><d><e>e</e></d></c>
        </super_secret>
    </secrets>
</root>)END");

    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(xml_isteam);

    auto secret_manager = std::make_shared<SecretsManager>();
    secret_manager->loadScrets(*config);

    EXPECT_EQ("a", secret_manager->getSecretValue("super_secret.a").str());
    EXPECT_EQ("b", secret_manager->getSecretValue("super_secret.b").str());
    EXPECT_EQ("e", secret_manager->getSecretValue("super_secret.c.d.e").str());
}

TEST(SecretsManager, ConfigSecretErrorLoadingSecretValueIfItHasAnyChildren)
{
    std::istringstream      // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        xml_isteam(R"END(<?xml version="1.0"?>
<root>
    <secrets>
        <super_secret>
            foobar
            <a>a</a>
        </super_secret>
    </secrets>
</root>)END");

    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(xml_isteam);

    auto secret_manager = std::make_shared<SecretsManager>();
    secret_manager->loadScrets(*config);

    // No way to extract "foobar" from config once the parent key has any subkeys.
    EXPECT_THROW(secret_manager->getSecretValue("super_secret"), DB::Exception);
}


TEST(SecretsManager, redactOutAllSecrets)
{
    std::istringstream      // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        xml_isteam(R"END(<?xml version="1.0"?>
<root>
    <secrets>
        <super_secret>
            <a>super secret nested value a</a>
            <b>super secret nested value b</b>
        </super_secret>
        <flat_secret>flat secret value</flat_secret>
    </secrets>
</root>)END");

    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(xml_isteam);

    auto secret_manager = std::make_shared<SecretsManager>();
    secret_manager->loadScrets(*config);

    EXPECT_EQ("Here goes some text with  and  and other secret but which is not in the secrets.",
            secret_manager->redactOutAllSecrets("Here goes some text with super secret nested value a and flat secret value and other secret but which is not in the secrets.",
                    [](const auto&) {return "";}
    ));

    EXPECT_EQ("Here goes some text with [a very long substitute using secret name 'super_secret.a'] "
            "and [a very long substitute using secret name 'flat_secret'] "
            "and other secret but which is not in the secrets.",
            secret_manager->redactOutAllSecrets("Here goes some text with super secret nested value a and flat secret value and other secret but which is not in the secrets.",
                    [](const auto& name) {return fmt::format("[a very long substitute using secret name '{}']", name);}
    ));
}
