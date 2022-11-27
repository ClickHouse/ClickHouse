#include <Common/Config/ConfigProcessor.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Poco/Util/XMLConfiguration.h>
#include <base/scope_guard.h>
#include <gtest/gtest.h>
#include <filesystem>


TEST(Common, ConfigProcessorManyElements)
{
    namespace fs = std::filesystem;
 
    auto path = fs::path("/tmp/test_config_processor/");

    fs::create_directories(path);
    fs::create_directories(path / "config.d");
    SCOPE_EXIT({ fs::remove_all(path); });

    auto config_file = std::make_unique<Poco::File>(path / "config.xml");

    constexpr size_t element_count = 1000000;

    {
        DB::WriteBufferFromFile out(config_file->path());
        writeString("<clickhouse>\n", out);
        for (size_t i = 0; i < element_count; ++i)
            writeString("<x><name>" + std::to_string(i) + "</name></x>\n", out);
        writeString("</clickhouse>\n", out);
    }

    DB::ConfigProcessor processor(config_file->path(), /* throw_on_bad_incl = */ false, /* log_to_console = */ false);
    bool has_zk_includes;
    DB::XMLDocumentPtr config_xml = processor.processConfig(&has_zk_includes);
    DB::ConfigurationPtr configuration(new Poco::Util::XMLConfiguration(config_xml));

    ASSERT_EQ("0", configuration->getString("x.name"));
    ASSERT_EQ("1", configuration->getString("x[1].name"));
    constexpr size_t last = element_count - 1;
    ASSERT_EQ(std::to_string(last), configuration->getString("x[" + std::to_string(last) + "].name"));

    Poco::Util::AbstractConfiguration::Keys keys;
    configuration->keys("", keys);
    ASSERT_EQ(element_count, keys.size());
    ASSERT_EQ("x", keys[0]);
    ASSERT_EQ("x[1]", keys[1]);
}
