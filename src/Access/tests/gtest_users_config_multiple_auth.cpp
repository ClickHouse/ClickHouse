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
                        <hash1>e9d94f22c21e57a5f44c5b4cc3b4a5b6c5b4c5b4c5b4c5b4c5b4c5b4c5b4c5b4</hash1>
                        <hash2>f1e2d3c4b5a69788776655443322110099887766554433221100998877665544</hash2>
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

TEST_F(UsersConfigMultipleAuthTest, MultipleHTTPAuthServers)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <http_authentication>
                        <server>
                            <server1>http_auth_server_1</server1>
                            <server2>http_auth_server_2</server2>
                        </server>
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
    ASSERT_EQ(user->authentication_methods.size(), 2);
    
    for (const auto & auth_method : user->authentication_methods)
    {
        EXPECT_EQ(auth_method.getType(), AuthenticationType::HTTP);
    }
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

TEST_F(UsersConfigMultipleAuthTest, EmptyLDAPServerThrowsException)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <ldap>
                        <server>
                            <server1></server1>
                        </server>
                    </ldap>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    EXPECT_THROW(storage->setConfig(*config), Exception);
}

TEST_F(UsersConfigMultipleAuthTest, EmptyKerberosRealmThrowsException)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <kerberos>
                        <realm>
                            <realm1></realm1>
                        </realm>
                    </kerberos>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    EXPECT_THROW(storage->setConfig(*config), Exception);
}

TEST_F(UsersConfigMultipleAuthTest, EmptyHTTPServerThrowsException)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <http_authentication>
                        <server>
                            <server1></server1>
                        </server>
                        <scheme>basic</scheme>
                    </http_authentication>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    EXPECT_THROW(storage->setConfig(*config), Exception);
}

TEST_F(UsersConfigMultipleAuthTest, NoAuthMethodsCreatesDefault)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <networks>
                        <ip>127.0.0.1</ip>
                    </networks>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    storage->setConfig(*config);
    
    auto user = storage->tryRead<User>("test_user");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->authentication_methods.size(), 1);
    EXPECT_EQ(user->authentication_methods[0].getType(), AuthenticationType::NO_PASSWORD);
}

TEST_F(UsersConfigMultipleAuthTest, BackwardCompatibilitySingleValues)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <password>single_password</password>
                    <password_sha256_hex>e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855</password_sha256_hex>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    storage->setConfig(*config);
    
    auto user = storage->tryRead<User>("test_user");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->authentication_methods.size(), 2);
    
    bool has_plaintext = false;
    bool has_sha256 = false;
    
    for (const auto & auth_method : user->authentication_methods)
    {
        if (auth_method.getType() == AuthenticationType::PLAINTEXT_PASSWORD)
            has_plaintext = true;
        else if (auth_method.getType() == AuthenticationType::SHA256_PASSWORD)
            has_sha256 = true;
    }
    
    EXPECT_TRUE(has_plaintext);
    EXPECT_TRUE(has_sha256);
}
