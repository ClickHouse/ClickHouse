#include <Access/AccessControl.h>
#include <Access/UsersConfigAccessStorage.h>
#include <Access/Role.h>
#include <Access/User.h>
#include <Poco/Util/XMLConfiguration.h>
#include <gtest/gtest.h>
#include <sstream>

using namespace DB;

namespace
{
template <typename EntityT>
std::shared_ptr<const EntityT> loadWithGrants(const String & grants)
{
    const String section = std::is_same_v<EntityT, User> ? "users" : "roles";
    const String auth = std::is_same_v<EntityT, User> ? "<no_password/>" : "";
    std::istringstream xml(
        "<clickhouse><" + section + "><e>" + auth + "<grants>" + grants + "</grants></e></" + section + "></clickhouse>");
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(xml);
    AccessControl access_control;
    UsersConfigAccessStorage storage("users_config_test", access_control, false);
    storage.setConfig(*config);
    return storage.tryRead<EntityT>("e");
}
}

TEST(UsersConfigGrants, RevokeFilterOfSourceGrantedWithoutFilter)
{
    auto user = loadWithGrants<User>("<query>GRANT ALL ON *.* WITH GRANT OPTION</query>"
                                     "<query>REVOKE READ ON URL('https://foo/.*')</query>");
    ASSERT_TRUE(user);
    AccessRights expected;
    expected.grantWithGrantOption(AccessType::ALL);
    expected.revoke(AccessType::READ, "URL");
    ASSERT_EQ(user->access, expected);
}

TEST(UsersConfigGrants, RevokeGrantOptionOfFilterOfSourceGrantedWithoutFilter)
{
    auto user = loadWithGrants<User>("<query>GRANT ALL ON *.* WITH GRANT OPTION</query>"
                                     "<query>REVOKE GRANT OPTION FOR READ ON URL('https://foo/.*')</query>");
    ASSERT_TRUE(user);
    ASSERT_TRUE(user->access.isGranted(AccessType::READ, "URL"));
    ASSERT_FALSE(user->access.hasGrantOption(AccessType::READ, "URL"));
}

TEST(UsersConfigGrants, RevokeFilterOfRoleSourceGrantedWithoutFilter)
{
    auto role = loadWithGrants<Role>("<query>GRANT READ ON URL</query>"
                                     "<query>REVOKE READ ON URL('https://foo/.*')</query>");
    ASSERT_TRUE(role);
    ASSERT_TRUE(role->access.isEmpty());
}

TEST(UsersConfigGrants, RevokeOneOfFilters)
{
    auto user = loadWithGrants<User>("<query>GRANT WRITE ON S3</query>"
                                     "<query>GRANT READ ON S3('foo')</query>"
                                     "<query>GRANT READ ON S3('bar')</query>"
                                     "<query>REVOKE READ ON S3('foo')</query>");
    ASSERT_TRUE(user);
    auto filters = user->access.getFilters("S3");
    ASSERT_EQ(filters.size(), 1);
    ASSERT_EQ(filters[0].path, "bar");
    ASSERT_TRUE(user->access.isGranted(AccessType::WRITE, "S3"));
}
