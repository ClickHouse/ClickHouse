#include <Common/Config/ConfigProcessor.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Poco/Timestamp.h>
#include <Common/XMLUtils.h>
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

    Poco::Timestamp load_start;

    DB::ConfigProcessor processor(config_file->path(), /* throw_on_bad_incl = */ false, /* log_to_console = */ false);
    bool has_zk_includes;
    DB::XMLDocumentPtr config_xml = processor.processConfig(&has_zk_includes);
    DB::ConfigurationPtr configuration(new Poco::Util::XMLConfiguration(config_xml));

    float load_elapsed_ms = (Poco::Timestamp() - load_start) / 1000.0f;
    std::cerr << "Config loading took " << load_elapsed_ms << " ms" << std::endl;

    ASSERT_EQ("0", configuration->getString("x.name"));
    ASSERT_EQ("1", configuration->getString("x[1].name"));
    constexpr size_t last = element_count - 1;
    ASSERT_EQ(std::to_string(last), configuration->getString("x[" + std::to_string(last) + "].name"));

    /// More that 5 min is way too slow
    ASSERT_LE(load_elapsed_ms, 300*1000);

    Poco::Timestamp enumerate_start;

    Poco::Util::AbstractConfiguration::Keys keys;
    configuration->keys("", keys);

    float enumerate_elapsed_ms = (Poco::Timestamp() - enumerate_start) / 1000.0f;
    std::cerr << "Key enumeration took " << enumerate_elapsed_ms << " ms" << std::endl;

    ASSERT_EQ(element_count, keys.size());
    ASSERT_EQ("x", keys[0]);
    ASSERT_EQ("x[1]", keys[1]);

    /// More that 5 min is way too slow
    ASSERT_LE(enumerate_elapsed_ms, 300*1000);
}

TEST(Common, ConfigProcessorMerge)
{
    namespace fs = std::filesystem;

    auto path = fs::path("/tmp/test_config_processor/");

    fs::remove_all(path);
    fs::create_directories(path);
    fs::create_directories(path / "config.d");
    //SCOPE_EXIT({ fs::remove_all(path); });

    auto config_file = std::make_unique<Poco::File>(path / "config.xml");
    {
        DB::WriteBufferFromFile out(config_file->path());
        writeString("<clickhouse>\n", out);
        writeString("<merge_tree><min_bytes_for_wide_part>1</min_bytes_for_wide_part></merge_tree>\n", out);
        writeString("</clickhouse>\n", out);
    }
    DB::XMLDocumentPtr config_xml;
    DB::ConfigProcessor processor(config_file->path(), /* throw_on_bad_incl = */ false, /* log_to_console = */ false);
    {
        bool has_zk_includes;
        config_xml = processor.processConfig(&has_zk_includes);
    }

    auto small_part_file = std::make_unique<Poco::File>(path / "config.d" / "part.xml");
    {
        DB::WriteBufferFromFile out(small_part_file->path());
        writeString("<merge_tree><min_bytes_for_wide_part>33</min_bytes_for_wide_part></merge_tree>\n", out);
    }
    DB::XMLDocumentPtr part_xml;

    {
        DB::ConfigProcessor tiny(small_part_file->path(), /* throw_on_bad_incl = */ false, /* log_to_console = */ false);
        bool has_zk_includes;
        part_xml = tiny.processConfig(&has_zk_includes);
    }

    auto * root_node = DB::XMLUtils::getRootNode(config_xml);
    auto * part_node = DB::XMLUtils::getRootNode(part_xml);
    auto * new_node = config_xml->importNode(part_node, true);
    auto * deep_root = root_node->getNodeByPath("merge_tree");
    processor.mergeRecursive(config_xml, deep_root, new_node);
    DB::ConfigurationPtr configuration(new Poco::Util::XMLConfiguration(config_xml));

    ASSERT_EQ(configuration->getUInt64("merge_tree.min_bytes_for_wide_part"), 33);
}
