#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <Access/AccessControl.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/LDAPAccessStorage.h>
#include <Access/Role.h>
#include <Poco/Net/IPAddress.h>

using namespace DB;
using namespace testing;
using namespace std::chrono_literals;

using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

std::mutex exit_mutex;

class TerminateOnDeadlock final
{
public:
    using Clock = std::chrono::system_clock;
    explicit TerminateOnDeadlock(Clock::duration timeout_)
        : timeout{timeout_}
    {
        thread = std::thread(&TerminateOnDeadlock::waitForCompletion, this);
        std::unique_lock lock{ mutex };
        started_var.wait(lock);
    }

    ~TerminateOnDeadlock()
    {
        completed_var.notify_one();
        thread.join();
    }

private:
    void waitForCompletion()
    {
        std::unique_lock lock{mutex};
        started_var.notify_one();
        if (completed_var.wait_for(lock, timeout) == std::cv_status::timeout) {
            std::unique_lock exit_lock{exit_mutex};
            std::exit(1);
        }
    }

    const Clock::duration timeout;
    std::mutex mutex;
    std::condition_variable completed_var;
    std::condition_variable started_var;
    std::thread thread;
};

class LDAPAccessStorageMock : public LDAPAccessStorage
{
public:
    explicit LDAPAccessStorageMock(const String & storage_name_, AccessControl & access_control_, const Poco::Util::AbstractConfiguration & config, const String & prefix)
        : LDAPAccessStorage(storage_name_, access_control_, config, prefix)
    {
    }

    MOCK_CONST_METHOD4(areLDAPCredentialsValidNoLock,
        bool(const User&, const Credentials&, const ExternalAuthenticators&, LDAPClient::SearchResultsList&));
};

void UpdateAuthenticatedUserTestImpl()
{
    TerminateOnDeadlock guardDeadLock{5s};

    auto address = Poco::Net::IPAddress{"127.0.0.1"};
    auto credentials = BasicCredentials{"user1", "pwd"};

    // most ldap server parameters in this config have no sense
    // we just should have correct structure to parse
    std::stringstream xml_conf(std::string(R"CONFIG(<clickhouse>
        <ldap_servers>
            <ldap_server1>
                <host>localhost</host>
                <port>389</port>
                <enable_tls>no</enable_tls>
                <tls_require_cert>never</tls_require_cert>
                <bind_dn>localhost\\user1</bind_dn>
                <user_dn_detection>
                    <base_dn>OU=CHILD,DC=somename,DC=dcch,DC=local</base_dn>
                    <search_filter>(&amp;(objectClass=user)(sAMAccountName=user1))</search_filter>
                </user_dn_detection>
            </ldap_server1>
        </ldap_servers>
        <role_mapping>
                <base_dn>OU=CHILD,DC=somename,DC=dcch,DC=local</base_dn>
                <attribute>CN</attribute>
                <scope>subtree</scope>
                <search_filter>(&amp;(objectClass=group)(member={user_dn}))</search_filter>
        </role_mapping>
    </clickhouse>)CONFIG"));

    Poco::XML::InputSource input_source{xml_conf};
    auto config = Poco::AutoPtr(new Poco::Util::XMLConfiguration(&input_source));
    config->setString("server", "ldap_server1");
    config->setString("roles", "role1");

    AccessControl access_control;
    access_control.setExternalAuthenticatorsConfig(*config);

    auto ldap_storage = std::make_shared<LDAPAccessStorageMock>("ldap", access_control, *config, "");
    Mock::AllowLeak(&*ldap_storage);    // in some cases when this death test fails (due to deadlock) mock leakage is a normal behavior
                                        // so we suppress warning message here
    access_control.addStorage(ldap_storage);

    access_control.addMemoryStorage("memory", false); // we use this storage just to avoid 'Unable to grant role' warning
    auto mem_storage = access_control.getStorageByName("memory");
    auto role = std::make_shared<Role>();
    role->setName("role1");
    mem_storage->insert(role);

    LDAPClient::SearchResultsList role_search_results{LDAPClient::SearchResults{"role1"}};

    EXPECT_CALL(*ldap_storage, areLDAPCredentialsValidNoLock(_,_,_,_)).WillOnce(DoAll(
        SetArgReferee<3>(role_search_results),
        Return(true)));
    auto res = ldap_storage->authenticate(credentials, address, access_control.getExternalAuthenticators(), true, true);
    ASSERT_NE(res, UUID{});

    LDAPClient::SearchResultsList role_search_results2{LDAPClient::SearchResults{"role2"}};

    EXPECT_CALL(*ldap_storage, areLDAPCredentialsValidNoLock(_,_,_,_)).WillOnce(DoAll(
        SetArgReferee<3>(role_search_results2),
        Return(true)));
    res = ldap_storage->authenticate(credentials, address, access_control.getExternalAuthenticators(), true, true); // deadlock occurs here before #53708 fix
    ASSERT_NE(res, UUID{});

    std::unique_lock lock{exit_mutex};
    std::exit(Test::HasFailure() ? 2 : 0);
}

// this test verifies fix for #53708 issue
TEST(LDAPAccessStorage, UpdateAuthenticatedUser)
{
    EXPECT_EXIT(UpdateAuthenticatedUserTestImpl(), ExitedWithCode(0), "");
}
