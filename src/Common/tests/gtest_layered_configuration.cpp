#include <gtest/gtest.h>

#include <thread>
#include <vector>
#include <atomic>

#include <Poco/AutoPtr.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Poco/Util/MapConfiguration.h>


using Poco::AutoPtr;
using Poco::Util::LayeredConfiguration;
using Poco::Util::MapConfiguration;


static AutoPtr<MapConfiguration> makeMapConfig(const std::string & key, const std::string & value)
{
    AutoPtr<MapConfiguration> cfg(new MapConfiguration);
    cfg->setString(key, value);
    return cfg;
}


TEST(LayeredConfiguration, ReplaceByLabelBasic)
{
    AutoPtr<LayeredConfiguration> lc(new LayeredConfiguration);

    auto cfg1 = makeMapConfig("key", "value1");
    lc->add(cfg1, "test_label", 0, true);

    EXPECT_EQ("value1", lc->getString("key"));

    auto cfg2 = makeMapConfig("key", "value2");
    lc->replace("test_label", cfg2, 0, true);

    EXPECT_EQ("value2", lc->getString("key"));
}


TEST(LayeredConfiguration, ReplaceByLabelAddsWhenNotFound)
{
    AutoPtr<LayeredConfiguration> lc(new LayeredConfiguration);

    auto cfg = makeMapConfig("newkey", "newvalue");
    lc->replace("nonexistent", cfg, 0, true);

    EXPECT_EQ("newvalue", lc->getString("newkey"));

    // Verify it was actually added with the label by replacing it again.
    auto cfg2 = makeMapConfig("newkey", "replaced");
    lc->replace("nonexistent", cfg2, 0, true);

    EXPECT_EQ("replaced", lc->getString("newkey"));
}


TEST(LayeredConfiguration, ReplaceByLabelPreservesOtherConfigs)
{
    AutoPtr<LayeredConfiguration> lc(new LayeredConfiguration);

    auto cfg_a = makeMapConfig("a", "1");
    auto cfg_b = makeMapConfig("b", "2");
    lc->add(cfg_a, "label_a", 0, true);
    lc->add(cfg_b, "label_b", 1, true);

    EXPECT_EQ("1", lc->getString("a"));
    EXPECT_EQ("2", lc->getString("b"));

    // Replace only label_a.
    auto cfg_a2 = makeMapConfig("a", "replaced");
    lc->replace("label_a", cfg_a2, 0, true);

    EXPECT_EQ("replaced", lc->getString("a"));
    EXPECT_EQ("2", lc->getString("b"));
}


TEST(LayeredConfiguration, ReplaceByLabelConcurrentReads)
{
    AutoPtr<LayeredConfiguration> lc(new LayeredConfiguration);

    auto cfg = makeMapConfig("key", "initial");
    lc->add(cfg, "default", 0, true);

    std::atomic<bool> stop{false};
    std::atomic<size_t> read_count{0};
    std::atomic<bool> saw_bad_value{false};

    // Spawn reader threads that continuously read from the config.
    constexpr size_t num_readers = 4;
    std::vector<std::thread> readers;
    readers.reserve(num_readers);
    for (size_t i = 0; i < num_readers; ++i)
    {
        readers.emplace_back([&]
        {
            while (!stop.load(std::memory_order_relaxed))
            {
                std::string val = lc->getString("key", "missing");
                // The value should always be either "initial", one of the
                // replacement values, or "missing" if we hit a transient
                // between configs. But it should never be corrupted garbage.
                if (val != "initial" && val != "missing" && !val.starts_with("round_"))
                    saw_bad_value.store(true, std::memory_order_relaxed);
                read_count.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    // Writer thread: rapidly replace the config many times.
    constexpr size_t num_replacements = 1000;
    for (size_t i = 0; i < num_replacements; ++i)
    {
        auto new_cfg = makeMapConfig("key", "round_" + std::to_string(i));
        lc->replace("default", new_cfg, 0, true);
    }

    stop.store(true, std::memory_order_relaxed);
    for (auto & t : readers)
        t.join();

    EXPECT_FALSE(saw_bad_value.load());
    EXPECT_GT(read_count.load(), 0u);

    // After all replacements, the last value should be visible.
    EXPECT_EQ("round_999", lc->getString("key"));
}
