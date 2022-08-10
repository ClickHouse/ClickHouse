#include <gtest/gtest.h>

#include <filesystem>

#include <base/scope_guard.h>
#include <Common/filesystemHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Common/Config/ConfigProcessor.h>
#include <Poco/Util/XMLConfiguration.h>


TEST(Config, MergeConfigsOneSided)
{
    using namespace DB;
    namespace fs = std::filesystem;
    using File = Poco::File;

    auto path = fs::path("/tmp/test_merge_configs/");

    fs::create_directories(path);
    fs::create_directories(path / "config.d");
    SCOPE_EXIT({ fs::remove_all(path); });

    auto config_file = std::make_unique<File>(path / "config.yaml");

    {
        WriteBufferFromFile out(config_file->path());
        std::string data = R"YAML(
clickhouse:
    field1: "1"
    field2: "2"
)YAML";
        writeString(data, out);
        out.finalize();
    }

    auto system_tables_file = std::make_unique<File>(path / "config.d/system_tables.yaml");

    {
        WriteBufferFromFile out(system_tables_file->path());
        std::string data = R"YAML(
clickhouse:
    text_log:
        database: system
        table: text_log
        partition_by:
            "@remove": "1"
        engine:
            - "@replace" : "1"
            - "ENGINE MergeTree"
        flush_interval_milliseconds: 7500
        level: debug
)YAML";
        writeString(data, out);
        out.finalize();
    }


    DB::ConfigProcessor processor(config_file->path(), /* throw_on_bad_incl = */ false, /* log_to_console = */ false);
    bool has_zk_includes;
    DB::XMLDocumentPtr config_xml = processor.processConfig(&has_zk_includes);
    DB::ConfigurationPtr configuration(new Poco::Util::XMLConfiguration(config_xml));


    ASSERT_EQ("1", configuration->getString("clickhouse.field1"));
    ASSERT_EQ("2", configuration->getString("clickhouse.field2"));
    ASSERT_EQ("system", configuration->getString("clickhouse.text_log.database"));
    ASSERT_EQ("text_log", configuration->getString("clickhouse.text_log.table"));
    ASSERT_EQ("ENGINE MergeTree", configuration->getString("clickhouse.text_log.engine"));
    ASSERT_FALSE(configuration->has("clickhouse.text_log.partition_by"));
}


TEST(Config, MergeConfigsTwoSided)
{
    using namespace DB;
    namespace fs = std::filesystem;
    using File = Poco::File;

    auto path = fs::path("/tmp/test_merge_configs/");

    fs::create_directories(path);
    fs::create_directories(path / "config.d");
    SCOPE_EXIT({ fs::remove_all(path); });

    auto config_file = std::make_unique<File>(path / "config.yaml");

    {
        WriteBufferFromFile out(config_file->path());
        std::string data = R"YAML(
clickhouse:
    field1 : "1"
    field2 : "2"
    text_log :
        database: system
        table: text_log
        partition_by: "toYYYYMMDD(Something)"
        engine: "ENGINE Memory"
        flush_interval_milliseconds: 7500
        level: debug
)YAML";
        writeString(data, out);
        out.finalize();
    }

    auto system_tables_file = std::make_unique<File>(path / "config.d/system_tables.yaml");

    {
        WriteBufferFromFile out(system_tables_file->path());
        std::string data = R"YAML(
clickhouse:
    field3 : "3"
    field4 : "4"
    text_log :
        database: system
        table: text_log
        partition_by:
            "@remove": "1"
        engine:
            - "@replace" : "1"
            - "ENGINE MergeTree"
        flush_interval_milliseconds: 7500
        level: debug
)YAML";
        writeString(data, out);
        out.finalize();
    }


    DB::ConfigProcessor processor(config_file->path(), /* throw_on_bad_incl = */ false, /* log_to_console = */ false);
    bool has_zk_includes;
    DB::XMLDocumentPtr config_xml = processor.processConfig(&has_zk_includes);
    DB::ConfigurationPtr configuration(new Poco::Util::XMLConfiguration(config_xml));


    ASSERT_EQ("1", configuration->getString("clickhouse.field1"));
    ASSERT_EQ("2", configuration->getString("clickhouse.field2"));
    ASSERT_EQ("3", configuration->getString("clickhouse.field3"));
    ASSERT_EQ("4", configuration->getString("clickhouse.field4"));
    ASSERT_EQ("system", configuration->getString("clickhouse.text_log.database"));
    ASSERT_EQ("text_log", configuration->getString("clickhouse.text_log.table"));
    ASSERT_EQ("ENGINE MergeTree", configuration->getString("clickhouse.text_log.engine"));
    ASSERT_FALSE(configuration->has("clickhouse.text_log.partition_by"));
}
