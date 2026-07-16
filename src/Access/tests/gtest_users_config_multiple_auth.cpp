#include <Access/AccessControl.h>
#include <Access/UsersConfigAccessStorage.h>
#include <Access/UsersConfigParser.h>
#include <Access/User.h>
#include <Access/Quota.h>
#include <Access/RowPolicy.h>
#include <Common/Exception.h>
#include <Poco/Util/XMLConfiguration.h>
#include <gtest/gtest.h>
#include <sstream>
#include <unordered_set>

#include "config.h"

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

TEST_F(UsersConfigMultipleAuthTest, FlatNoPassword)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <no_password/>
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

TEST_F(UsersConfigMultipleAuthTest, MultiplePlaintextPasswords)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <auth_methods>
                        <a1><password>plaintext_pass1</password></a1>
                        <a2><password>plaintext_pass2</password></a2>
                        <a3><password>plaintext_pass3</password></a3>
                    </auth_methods>
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
        EXPECT_EQ(auth_method.getType(), AuthenticationType::PLAINTEXT_PASSWORD);
}

TEST_F(UsersConfigMultipleAuthTest, MultipleSHA256Passwords)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <auth_methods>
                        <a1><password_sha256_hex>e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855</password_sha256_hex></a1>
                        <a2><password_sha256_hex>d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2</password_sha256_hex></a2>
                    </auth_methods>
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
        EXPECT_EQ(auth_method.getType(), AuthenticationType::SHA256_PASSWORD);
}

TEST_F(UsersConfigMultipleAuthTest, MultipleDoubleSHA1Passwords)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <auth_methods>
                        <a1><password_double_sha1_hex>eafbd9c4b3c8b40d509308e767b41671ee5bac68</password_double_sha1_hex></a1>
                        <a2><password_double_sha1_hex>7e4ca4bb0df85c1106f39516a0753013b79b32ff</password_double_sha1_hex></a2>
                    </auth_methods>
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
        EXPECT_EQ(auth_method.getType(), AuthenticationType::DOUBLE_SHA1_PASSWORD);
}

TEST_F(UsersConfigMultipleAuthTest, MultipleLDAPServers)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <auth_methods>
                        <a1><ldap><server>ldap_server_1</server></ldap></a1>
                        <a2><ldap><server>ldap_server_2</server></ldap></a2>
                    </auth_methods>
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
        EXPECT_EQ(auth_method.getType(), AuthenticationType::LDAP);
}

TEST_F(UsersConfigMultipleAuthTest, MultipleKerberosRealms)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <auth_methods>
                        <a1><kerberos><realm>EXAMPLE.COM</realm></kerberos></a1>
                        <a2><kerberos><realm>TEST.ORG</realm></kerberos></a2>
                    </auth_methods>
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
        EXPECT_EQ(auth_method.getType(), AuthenticationType::KERBEROS);
}

TEST_F(UsersConfigMultipleAuthTest, KerberosWithoutRealm)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <kerberos/>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    storage->setConfig(*config);

    auto user = storage->tryRead<User>("test_user");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->authentication_methods.size(), 1);
    EXPECT_EQ(user->authentication_methods[0].getType(), AuthenticationType::KERBEROS);
    EXPECT_EQ(user->authentication_methods[0].getKerberosRealm(), "");
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
                    <auth_methods>
                        <a1><http_authentication><server>http_auth_server_1</server><scheme>basic</scheme></http_authentication></a1>
                        <a2><http_authentication><server>http_auth_server_2</server><scheme>basic</scheme></http_authentication></a2>
                    </auth_methods>
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
        EXPECT_EQ(auth_method.getType(), AuthenticationType::HTTP);
}

TEST_F(UsersConfigMultipleAuthTest, SingleHTTPAuthenticationWithWrapper)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <auth_methods>
                        <method1>
                            <http_authentication>
                                <server>http_auth_server</server>
                                <scheme>basic</scheme>
                            </http_authentication>
                        </method1>
                    </auth_methods>
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

TEST_F(UsersConfigMultipleAuthTest, HTTPAuthenticationEmptyError)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <http_authentication/>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    EXPECT_THROW(storage->setConfig(*config), Exception);
}

TEST_F(UsersConfigMultipleAuthTest, HTTPAuthenticationNestedMethodsWithoutServerSchemeError)
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
    EXPECT_THROW(storage->setConfig(*config), Exception);
}

TEST_F(UsersConfigMultipleAuthTest, MultipleHTTPAuthenticationMissingSchemeError)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <auth_methods>
                        <a1>
                            <http_authentication>
                                <server>http_auth_server_1</server>
                                <scheme>basic</scheme>
                            </http_authentication>
                        </a1>
                        <a2>
                            <http_authentication>
                                <server>http_auth_server_2</server>
                            </http_authentication>
                        </a2>
                    </auth_methods>
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
                    <auth_methods>
                        <a1><password>plaintext_pass1</password></a1>
                        <a2><password>plaintext_pass2</password></a2>
                        <a3><password_sha256_hex>e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855</password_sha256_hex></a3>
                        <a4><ldap><server>ldap_server_1</server></ldap></a4>
                    </auth_methods>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    storage->setConfig(*config);

    auto user = storage->tryRead<User>("test_user");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->authentication_methods.size(), 4);

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

// ---------------------------------------------------------------------------
// Nested auth_methods format tests
// ---------------------------------------------------------------------------

TEST_F(UsersConfigMultipleAuthTest, NestedNoPasswordInAuthMethods)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <auth_methods>
                        <auth1>
                            <no_password/>
                        </auth1>
                    </auth_methods>
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

TEST_F(UsersConfigMultipleAuthTest, NestedNoPasswordInMultipleAuthMethodsIsDisallowed)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <auth_methods>
                        <auth1>
                            <no_password/>
                        </auth1>
                        <auth2>
                            <password>plaintext_pass1</password>
                        </auth2>
                    </auth_methods>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    EXPECT_THROW(storage->setConfig(*config), Exception);
}

TEST_F(UsersConfigMultipleAuthTest, EmptyAuthMethodsIsDisallowed)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <auth_methods/>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    EXPECT_THROW(storage->setConfig(*config), Exception);
}

TEST_F(UsersConfigMultipleAuthTest, OTPInsideAuthMethodIsDisallowed)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <auth_methods>
                        <a1>
                            <time_based_one_time_password>
                                <secret>JBSWY3DPEHPK3PXP</secret>
                            </time_based_one_time_password>
                            <password>plaintext_pass1</password>
                        </a1>
                    </auth_methods>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    EXPECT_THROW(storage->setConfig(*config), Exception);
}

TEST_F(UsersConfigMultipleAuthTest, NestedSinglePlaintextPassword)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <auth_methods>
                        <auth1>
                            <password>plaintext_pass</password>
                        </auth1>
                    </auth_methods>
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

TEST_F(UsersConfigMultipleAuthTest, NestedMultiplePlaintextPasswords)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <auth_methods>
                        <auth1>
                            <password>pass1</password>
                        </auth1>
                        <auth2>
                            <password>pass2</password>
                        </auth2>
                        <auth3>
                            <password>pass3</password>
                        </auth3>
                    </auth_methods>
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
        EXPECT_EQ(auth_method.getType(), AuthenticationType::PLAINTEXT_PASSWORD);
}

TEST_F(UsersConfigMultipleAuthTest, NestedMixedAuthenticationTypes)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <auth_methods>
                        <plain>
                            <password>plaintext_pass</password>
                        </plain>
                        <sha256>
                            <password_sha256_hex>e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855</password_sha256_hex>
                        </sha256>
                        <double_sha1>
                            <password_double_sha1_hex>eafbd9c4b3c8b40d509308e767b41671ee5bac68</password_double_sha1_hex>
                        </double_sha1>
                    </auth_methods>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    storage->setConfig(*config);

    auto user = storage->tryRead<User>("test_user");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->authentication_methods.size(), 3);

    int plaintext_count = 0;
    int sha256_count = 0;
    int double_sha1_count = 0;
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
            case AuthenticationType::DOUBLE_SHA1_PASSWORD: 
                double_sha1_count++; 
                break;
            default: 
                FAIL() << "Unexpected authentication type";
        }
    }
    EXPECT_EQ(plaintext_count, 1);
    EXPECT_EQ(sha256_count, 1);
    EXPECT_EQ(double_sha1_count, 1);
}

TEST_F(UsersConfigMultipleAuthTest, NestedLDAPAuthentication)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <auth_methods>
                        <ldap_method>
                            <ldap>
                                <server>my_ldap_server</server>
                            </ldap>
                        </ldap_method>
                    </auth_methods>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    storage->setConfig(*config);

    auto user = storage->tryRead<User>("test_user");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->authentication_methods.size(), 1);
    EXPECT_EQ(user->authentication_methods[0].getType(), AuthenticationType::LDAP);
    EXPECT_EQ(user->authentication_methods[0].getLDAPServerName(), "my_ldap_server");
}

TEST_F(UsersConfigMultipleAuthTest, NestedAndFlatFormatsCannotBeMixed)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <password>some_password</password>
                    <auth_methods>
                        <auth1>
                            <password>another_password</password>
                        </auth1>
                    </auth_methods>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    EXPECT_THROW(storage->setConfig(*config), Exception);
}

TEST_F(UsersConfigMultipleAuthTest, NestedMultipleTypesPerMethodIsError)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <auth_methods>
                        <auth1>
                            <password>pass</password>
                            <password_sha256_hex>e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855</password_sha256_hex>
                        </auth1>
                    </auth_methods>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    EXPECT_THROW(storage->setConfig(*config), Exception);
}

TEST_F(UsersConfigMultipleAuthTest, OTPWithNonPasswordAuthIsError)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <auth_methods>
                        <a1>
                            <ldap><server>ldap_server_1</server></ldap>
                        </a1>
                    </auth_methods>
                    <time_based_one_time_password>
                        <secret>JBSWY3DPEHPK3PXP</secret>
                    </time_based_one_time_password>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    EXPECT_THROW(storage->setConfig(*config), Exception);
}

TEST_F(UsersConfigMultipleAuthTest, OTPWithMixedAuthIncludingPassword)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <auth_methods>
                        <a1>
                            <ldap><server>ldap_server_1</server></ldap>
                        </a1>
                        <a2>
                            <password>plaintext_pass</password>
                        </a2>
                    </auth_methods>
                    <time_based_one_time_password>
                        <secret>JBSWY3DPEHPK3PXP</secret>
                    </time_based_one_time_password>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    storage->setConfig(*config);

    auto user = storage->tryRead<User>("test_user");
    ASSERT_TRUE(user);
    ASSERT_EQ(user->authentication_methods.size(), 2);
    EXPECT_EQ(user->authentication_methods[0].getType(), AuthenticationType::LDAP);
    EXPECT_EQ(user->authentication_methods[1].getType(), AuthenticationType::PLAINTEXT_PASSWORD);
}

#if USE_SSH
/// An explicitly empty <ssh_keys/> block (no <ssh_key> entry at all) is a real config mistake and
/// must fail with BAD_ARGUMENTS, not be silently skipped. This is distinct from the FIPS-filtered
/// case (entries present but all filtered out), which is dropped; that path only triggers on a real
/// FIPS build (isFIPSEnabled()) and cannot be reached from a non-FIPS unit test.
TEST_F(UsersConfigMultipleAuthTest, EmptySSHKeysBlockIsRejected)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <ssh_only_user>
                    <ssh_keys></ssh_keys>
                </ssh_only_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    /// An explicitly empty ssh_keys section is a misconfiguration and must surface as an error.
    EXPECT_THROW(storage->setConfig(*config), DB::Exception);
}

/// The same rejection applies inside <auth_methods>: an explicitly empty <ssh_keys/> method is a
/// config mistake and must fail rather than be silently dropped, even when a sibling valid method
/// exists.
TEST_F(UsersConfigMultipleAuthTest, EmptySSHKeysMethodIsRejected)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <auth_methods>
                        <a1><ssh_keys></ssh_keys></a1>
                        <a2><password>plaintext_pass</password></a2>
                    </auth_methods>
                </test_user>
            </users>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    EXPECT_THROW(storage->setConfig(*config), DB::Exception);
}
#endif

/// Quotas and row policies must be wired to a normally-parsed user. This guards the surviving-user
/// set that parseUsers feeds into parseQuotas/parseRowPolicies: a user that parseUsers keeps must
/// still be added as a quota grantee and get its row policy. (The complementary FIPS-skip case, where
/// a skipped user must NOT get orphaned quota/row-policy entities, only triggers on a real FIPS build
/// and cannot be reached from a non-FIPS unit test.)
TEST_F(UsersConfigMultipleAuthTest, QuotaAndRowPolicyWiredToParsedUser)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <test_user>
                    <password>plaintext_pass</password>
                    <quota>test_quota</quota>
                    <databases>
                        <test_db>
                            <test_table>
                                <filter>a = 1</filter>
                            </test_table>
                        </test_db>
                    </databases>
                </test_user>
            </users>
            <quotas>
                <test_quota></test_quota>
            </quotas>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);
    storage->setConfig(*config);

    auto user = storage->tryRead<User>("test_user");
    ASSERT_TRUE(user);
    const auto user_id_opt = storage->find<User>(String{"test_user"});
    ASSERT_TRUE(user_id_opt.has_value());
    const UUID user_id = *user_id_opt;

    auto quota = storage->tryRead<Quota>("test_quota");
    ASSERT_TRUE(quota);
    EXPECT_TRUE(quota->to_roles.ids.contains(user_id));

    /// Look up the synthesized row policy by scanning (its short name is generated internally).
    auto policy_ids = storage->findAll<RowPolicy>();
    bool found_policy_for_user = false;
    for (const auto & id : policy_ids)
    {
        auto rp = storage->tryRead<RowPolicy>(id);
        if (rp && rp->getDatabase() == "test_db" && rp->getTableName() == "test_table"
            && rp->to_roles.ids.contains(user_id))
            found_policy_for_user = true;
    }
    EXPECT_TRUE(found_policy_for_user);
}

/// Directly exercises the surviving-user set that parseUsers feeds to parseQuotas/parseRowPolicies:
/// a user absent from the surviving set (as happens when parseUser skips a FIPS all-Ed25519 user)
/// must not get a quota grantee or a row-policy entity, so no orphaned entity referencing a missing
/// user is created. FIPS-independent because it drives the surviving set explicitly.
TEST_F(UsersConfigMultipleAuthTest, SkippedUserGetsNoQuotaOrRowPolicy)
{
    const std::string xml_config = R"(
        <clickhouse>
            <users>
                <kept_user>
                    <password>p1</password>
                    <quota>shared_quota</quota>
                    <databases>
                        <db1><t1><filter>a = 1</filter></t1></db1>
                    </databases>
                </kept_user>
                <skipped_user>
                    <password>p2</password>
                    <quota>shared_quota</quota>
                    <databases>
                        <db1><t1><filter>a = 2</filter></t1></db1>
                    </databases>
                </skipped_user>
            </users>
            <quotas>
                <shared_quota></shared_quota>
            </quotas>
        </clickhouse>
    )";

    auto config = createConfigFromXML(xml_config);

    UsersConfigParser parser{*access_control};
    /// skipped_user is deliberately not in the surviving set (mimics parseUser having skipped it).
    std::unordered_set<String> surviving_user_names{"kept_user"};

    const UUID kept_id = UsersConfigParser::generateID(AccessEntityType::USER, "kept_user");
    const UUID skipped_id = UsersConfigParser::generateID(AccessEntityType::USER, "skipped_user");

    auto quotas = parser.parseQuotas(*config, surviving_user_names);
    ASSERT_EQ(quotas.size(), 1u);
    const auto & quota = static_cast<const Quota &>(*quotas[0]);
    EXPECT_TRUE(quota.to_roles.ids.contains(kept_id));
    EXPECT_FALSE(quota.to_roles.ids.contains(skipped_id));

    auto policies = parser.parseRowPolicies(*config, surviving_user_names);
    for (const auto & entity : policies)
    {
        const auto & policy = static_cast<const RowPolicy &>(*entity);
        EXPECT_FALSE(policy.to_roles.ids.contains(skipped_id));
    }
    bool found_kept_policy = false;
    for (const auto & entity : policies)
    {
        const auto & policy = static_cast<const RowPolicy &>(*entity);
        if (policy.to_roles.ids.contains(kept_id))
            found_kept_policy = true;
    }
    EXPECT_TRUE(found_kept_policy);
}
