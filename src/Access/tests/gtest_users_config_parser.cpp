#include <gtest/gtest.h>

#include <Access/AccessControl.h>
#include <Access/User.h>
#include <Common/Config/ConfigProcessor.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Poco/Util/XMLConfiguration.h>
#include <base/scope_guard.h>

#include <algorithm>
#include <filesystem>

using namespace DB;

TEST(UsersConfigParser, SkipsRemovedUsersFromYamlMainConfig)
{
    namespace fs = std::filesystem;

    auto path = fs::temp_directory_path() / "test_users_config_parser_remove_default";
    auto config_path = path / "config.yaml";

    fs::create_directories(path);
    SCOPE_EXIT({ fs::remove_all(path); });

    {
        WriteBufferFromFile out(config_path.string());
        std::string data = R"YAML(
users:
    default:
        "@remove": remove
    anotheruser:
        password: ''
)YAML";
        writeString(data, out);
        out.finalize();
    }

    ConfigProcessor processor(config_path.string(), /* throw_on_bad_incl = */ false, /* log_to_console = */ false);
    bool has_zk_includes = false;
    XMLDocumentPtr config_xml = processor.processConfig(&has_zk_includes);
    ConfigurationPtr configuration(new Poco::Util::XMLConfiguration(config_xml));

    Poco::Util::AbstractConfiguration::Keys user_names;
    configuration->keys("users", user_names);
    std::sort(user_names.begin(), user_names.end());
    ASSERT_EQ(user_names.size(), 2);
    EXPECT_EQ(user_names[0], "anotheruser");
    EXPECT_EQ(user_names[1], "default");

    AccessControl access_control;
    ASSERT_NO_THROW(access_control.setUsersConfig(*configuration));
    EXPECT_FALSE(access_control.find<User>("default").has_value());
    EXPECT_TRUE(access_control.find<User>("anotheruser").has_value());
    access_control.shutdown();
}
