#include <Access/AccessControl.h>
#include <Access/UsersConfigAccessStorage.h>
#include <Access/User.h>
#include <Common/Exception.h>
#include <Poco/Util/XMLConfiguration.h>
#include <gtest/gtest.h>
#include <sstream>

using namespace DB;

namespace
{
    Poco::AutoPtr<Poco::Util::XMLConfiguration> createConfigFromXML(const std::string & xml_content)
    {
        std::istringstream xml_stream(xml_content);
        Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(xml_stream);
        return config;
    }
}

class UsersConfigMultipleAuthTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        access_control = std::make_unique<AccessControl>();
        // Set unlimited authentication methods for most tests (specific tests will override)
        access_control->setMaxAuthenticationMethodsAllowed(0);
        storage = std::make_unique<UsersConfigAccessStorage>("users_config_test", *access_control, false);
    }

    std::unique_ptr<AccessControl> access_control;
    std::unique_ptr<UsersConfigAccessStorage> storage;
};

TEST_F(UsersConfigMultipleAuthTest, SinglePlaintextPassword)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <password>plaintext_pass</password>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    storage->setConfig(*config);
    
    auto user = storage->tryRead<User>("test_user");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->authentication_methods.size(), 1);
    EXPECT_EQ(user->authentication_methods[0].getType(), AuthenticationType::PLAINTEXT_PASSWORD);
}

TEST_F(UsersConfigMultipleAuthTest, MultiplePlaintextPasswords)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <password>
                        <pass1>plaintext_pass1</pass1>
                        <pass2>plaintext_pass2</pass2>
                        <pass3>plaintext_pass3</pass3>
                    </password>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    storage->setConfig(*config);
    
    auto user = storage->tryRead<User>("test_user");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->authentication_methods.size(), 3);
    
    for (const auto & auth_method : user->authentication_methods)
    {
        EXPECT_EQ(auth_method.getType(), AuthenticationType::PLAINTEXT_PASSWORD);
    }
}

TEST_F(UsersConfigMultipleAuthTest, MultipleSHA256Passwords)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <password_sha256_hex>
                        <hash1>e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855</hash1>
                        <hash2>d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2</hash2>
                    </password_sha256_hex>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    storage->setConfig(*config);
    
    auto user = storage->tryRead<User>("test_user");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->authentication_methods.size(), 2);
    
    for (const auto & auth_method : user->authentication_methods)
    {
        EXPECT_EQ(auth_method.getType(), AuthenticationType::SHA256_PASSWORD);
    }
}

TEST_F(UsersConfigMultipleAuthTest, MultipleDoubleSHA1Passwords)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <password_double_sha1_hex>
                        <hash1>eafbd9c4b3c8b40d509308e767b41671ee5bac68</hash1>
                        <hash2>7e4ca4bb0df85c1106f39516a0753013b79b32ff</hash2>
                    </password_double_sha1_hex>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    storage->setConfig(*config);
    
    auto user = storage->tryRead<User>("test_user");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->authentication_methods.size(), 2);
    
    for (const auto & auth_method : user->authentication_methods)
    {
        EXPECT_EQ(auth_method.getType(), AuthenticationType::DOUBLE_SHA1_PASSWORD);
    }
}

TEST_F(UsersConfigMultipleAuthTest, MultipleLDAPServers)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <ldap>
                        <server>
                            <server1>ldap_server_1</server1>
                            <server2>ldap_server_2</server2>
                        </server>
                    </ldap>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    storage->setConfig(*config);
    
    auto user = storage->tryRead<User>("test_user");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->authentication_methods.size(), 2);
    
    for (const auto & auth_method : user->authentication_methods)
    {
        EXPECT_EQ(auth_method.getType(), AuthenticationType::LDAP);
    }
}

TEST_F(UsersConfigMultipleAuthTest, MultipleKerberosRealms)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <kerberos>
                        <realm>
                            <realm1>EXAMPLE.COM</realm1>
                            <realm2>TEST.ORG</realm2>
                        </realm>
                    </kerberos>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    storage->setConfig(*config);
    
    auto user = storage->tryRead<User>("test_user");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->authentication_methods.size(), 2);
    
    for (const auto & auth_method : user->authentication_methods)
    {
        EXPECT_EQ(auth_method.getType(), AuthenticationType::KERBEROS);
    }
}

TEST_F(UsersConfigMultipleAuthTest, SingleHTTPAuthentication)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <http_authentication>
                        <server>http_auth_server</server>
                        <scheme>basic</scheme>
                    </http_authentication>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    storage->setConfig(*config);
    
    auto user = storage->tryRead<User>("test_user");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->authentication_methods.size(), 1);
    EXPECT_EQ(user->authentication_methods[0].getType(), AuthenticationType::HTTP);
}

TEST_F(UsersConfigMultipleAuthTest, MultipleHTTPAuthenticationMethods)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <http_authentication>
                        <method1>
                            <server>http_auth_server_1</server>
                            <scheme>basic</scheme>
                        </method1>
                        <method2>
                            <server>http_auth_server_2</server>
                            <scheme>basic</scheme>
                        </method2>
                    </http_authentication>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    storage->setConfig(*config);
    
    auto user = storage->tryRead<User>("test_user");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->authentication_methods.size(), 2);
    
    for (const auto & auth_method : user->authentication_methods)
    {
        EXPECT_EQ(auth_method.getType(), AuthenticationType::HTTP);
    }
}

TEST_F(UsersConfigMultipleAuthTest, SingleHTTPAuthenticationWithWrapper)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <http_authentication>
                        <method1>
                            <server>http_auth_server</server>
                            <scheme>basic</scheme>
                        </method1>
                    </http_authentication>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    storage->setConfig(*config);
    
    auto user = storage->tryRead<User>("test_user");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->authentication_methods.size(), 1);
    EXPECT_EQ(user->authentication_methods[0].getType(), AuthenticationType::HTTP);
    EXPECT_EQ(user->authentication_methods[0].getHTTPAuthenticationServerName(), "http_auth_server");
    EXPECT_EQ(user->authentication_methods[0].getHTTPAuthenticationScheme(), HTTPAuthenticationScheme::BASIC);
}

TEST_F(UsersConfigMultipleAuthTest, HTTPAuthenticationMissingSchemeError)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <http_authentication>
                        <server>http_auth_server</server>
                    </http_authentication>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    EXPECT_THROW(storage->setConfig(*config), Exception);
}

TEST_F(UsersConfigMultipleAuthTest, HTTPAuthenticationMissingServerError)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <http_authentication>
                        <scheme>basic</scheme>
                    </http_authentication>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    EXPECT_THROW(storage->setConfig(*config), Exception);
}

TEST_F(UsersConfigMultipleAuthTest, MultipleHTTPAuthenticationMissingSchemeError)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <http_authentication>
                        <method1>
                            <server>http_auth_server_1</server>
                            <scheme>basic</scheme>
                        </method1>
                        <method2>
                            <server>http_auth_server_2</server>
                        </method2>
                    </http_authentication>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    EXPECT_THROW(storage->setConfig(*config), Exception);
}

TEST_F(UsersConfigMultipleAuthTest, HTTPAuthenticationMixedSyntax)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <http_authentication>
                        <server>primary_server</server>
                        <scheme>basic</scheme>
                        <method1>
                            <server>other_server</server>
                            <scheme>basic</scheme>
                        </method1>
                    </http_authentication>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    storage->setConfig(*config);
    
    auto user = storage->tryRead<User>("test_user");
    ASSERT_TRUE(user);
    // When un-nested server/scheme are present other http authn methods are ignored
    ASSERT_EQ(user->authentication_methods.size(), 1);
    EXPECT_EQ(user->authentication_methods[0].getHTTPAuthenticationServerName(), "primary_server");
    EXPECT_EQ(user->authentication_methods[0].getHTTPAuthenticationScheme(), HTTPAuthenticationScheme::BASIC);
    EXPECT_EQ(user->authentication_methods[0].getType(), AuthenticationType::HTTP);
}

TEST_F(UsersConfigMultipleAuthTest, MixedAuthenticationMethods)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <password>
                        <pass1>plaintext_pass1</pass1>
                        <pass2>plaintext_pass2</pass2>
                    </password>
                    <password_sha256_hex>
                        <hash1>e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855</hash1>
                    </password_sha256_hex>
                    <ldap>
                        <server>
                            <server1>ldap_server_1</server1>
                        </server>
                    </ldap>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    storage->setConfig(*config);
    
    auto user = storage->tryRead<User>("test_user");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->authentication_methods.size(), 4);
    
    // Count authentication method types
    int plaintext_count = 0;
    int sha256_count = 0;
    int ldap_count = 0;
    
    for (const auto & auth_method : user->authentication_methods)
    {
        switch (auth_method.getType())
        {
            case AuthenticationType::PLAINTEXT_PASSWORD:
                plaintext_count++;
                break;
            case AuthenticationType::SHA256_PASSWORD:
                sha256_count++;
                break;
            case AuthenticationType::LDAP:
                ldap_count++;
                break;
            default:
                FAIL() << "Unexpected authentication type";
        }
    }
    
    EXPECT_EQ(plaintext_count, 2);
    EXPECT_EQ(sha256_count, 1);
    EXPECT_EQ(ldap_count, 1);
}

TEST_F(UsersConfigMultipleAuthTest, MaxAuthenticationMethodsPerUserLimitEnforced)
{
    access_control->setMaxAuthenticationMethodsAllowed(2);

    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <password>
                        <pass1>plaintext_pass1</pass1>
                        <pass2>plaintext_pass2</pass2>
                        <pass3>plaintext_pass3</pass3>
                    </password>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    EXPECT_THROW(storage->setConfig(*config), Exception);
}

TEST_F(UsersConfigMultipleAuthTest, MaxAuthenticationMethodsPerUserLimitExactMatch)
{
    access_control->setMaxAuthenticationMethodsAllowed(3);

    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <password>
                        <pass1>plaintext_pass1</pass1>
                        <pass2>plaintext_pass2</pass2>
                        <pass3>plaintext_pass3</pass3>
                    </password>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    storage->setConfig(*config);

    auto user = storage->tryRead<User>("test_user");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->authentication_methods.size(), 3);
}

TEST_F(UsersConfigMultipleAuthTest, MaxAuthenticationMethodsPerUserUnlimited)
{
    // Set limit to 0 (unlimited)
    access_control->setMaxAuthenticationMethodsAllowed(0);

    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <password>
                        <pass1>plaintext_pass1</pass1>
                        <pass2>plaintext_pass2</pass2>
                        <pass3>plaintext_pass3</pass3>
                        <pass4>plaintext_pass4</pass4>
                        <pass5>plaintext_pass5</pass5>
                    </password>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    storage->setConfig(*config);

    auto user = storage->tryRead<User>("test_user");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->authentication_methods.size(), 5);
}

TEST_F(UsersConfigMultipleAuthTest, MaxAuthenticationMethodsPerUserMultipleHTTPMethods)
{
    access_control->setMaxAuthenticationMethodsAllowed(2);

    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <http_authentication>
                        <method1>
                            <server>http_auth_server_1</server>
                            <scheme>basic</scheme>
                        </method1>
                        <method2>
                            <server>http_auth_server_2</server>
                            <scheme>basic</scheme>
                        </method2>
                        <method3>
                            <server>http_auth_server_3</server>
                            <scheme>basic</scheme>
                        </method3>
                    </http_authentication>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    EXPECT_THROW(storage->setConfig(*config), Exception);
}

TEST_F(UsersConfigMultipleAuthTest, MaxAuthenticationMethodsPerUserMixedTypes)
{
    access_control->setMaxAuthenticationMethodsAllowed(3);

    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <password>plaintext_pass1</password>
                    <ldap>
                        <server>ldap_server_1</server>
                    </ldap>
                    <http_authentication>
                        <method1>
                            <server>http_auth_server_1</server>
                            <scheme>basic</scheme>
                        </method1>
                        <method2>
                            <server>http_auth_server_2</server>
                            <scheme>basic</scheme>
                        </method2>
                    </http_authentication>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    EXPECT_THROW(storage->setConfig(*config), Exception);
}
