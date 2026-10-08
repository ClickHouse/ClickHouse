#include <gtest/gtest.h>

#include <chrono>
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
    std::atomic<size_t> readers_saw_replacement{0};
    std::atomic<size_t> readers_saw_initial{0};

    // Spawn reader threads that continuously read from the config.
    constexpr size_t num_readers = 4;
    std::vector<std::thread> readers;
    readers.reserve(num_readers);
    for (size_t i = 0; i < num_readers; ++i)
    {
        readers.emplace_back([&]
        {
            bool counted_initial = false;
            bool counted_replacement = false;
            do
            {
                std::string val = lc->getString("key", "missing");
                // replace() swaps the layer under the same mutex every getString() takes, so a reader always
                // observes a complete config: "initial" before the first replacement, "round_N" after. Anything
                // else, including the "missing" default, means the key was not visible and is a defect.
                if (val != "initial" && !val.starts_with("round_"))
                    saw_bad_value.store(true, std::memory_order_relaxed);
                if (!counted_initial && val == "initial")
                {
                    counted_initial = true;
                    readers_saw_initial.fetch_add(1, std::memory_order_relaxed);
                }
                if (!counted_replacement && val.starts_with("round_"))
                {
                    counted_replacement = true;
                    readers_saw_replacement.fetch_add(1, std::memory_order_relaxed);
                }
                read_count.fetch_add(1, std::memory_order_relaxed);
            } while (!stop.load(std::memory_order_relaxed));
        });
    }

    // Bounded, so a config that stops making a value visible fails the assertions after the join
    // instead of hanging the sequential unit-test binary. 60 s dwarfs the ~100 ms these waits take.
    // Returns the count reached, for the caller whose counter can still be completed after the loop.
    auto wait_for_all_readers = [](const std::atomic<size_t> & counter, size_t target)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (counter.load(std::memory_order_relaxed) < target && std::chrono::steady_clock::now() < deadline)
            std::this_thread::yield();
        return counter.load(std::memory_order_relaxed);
    };

    // Every reader observes the pre-replacement value before the first replacement below: the value
    // cannot change while this waits, so each reader reaches the count on its first read. No snapshot
    // is needed: counting "initial" is only possible before the first replacement lands.
    wait_for_all_readers(readers_saw_initial, num_readers);

    // Writer thread: rapidly replace the config many times.
    constexpr size_t num_replacements = 1000;
    size_t saw_replacement_in_loop = 0;
    for (size_t i = 0; i < num_replacements; ++i)
    {
        auto new_cfg = makeMapConfig("key", "round_" + std::to_string(i));
        lc->replace("default", new_cfg, 0, true);
        // Hold here until every reader has observed a replaced value, so the reads interleave with the
        // replacements instead of all landing after the last one. Assert the count taken here: after the
        // loop every reader can still reach round_999, so the live count cannot show when it got there.
        if (i == 0)
            saw_replacement_in_loop = wait_for_all_readers(readers_saw_replacement, num_readers);
    }

    stop.store(true, std::memory_order_relaxed);
    for (auto & t : readers)
        t.join();

    EXPECT_FALSE(saw_bad_value.load());
    EXPECT_GE(read_count.load(), num_readers);
    EXPECT_EQ(num_readers, readers_saw_initial.load());
    EXPECT_EQ(num_readers, saw_replacement_in_loop);

    // After all replacements, the last value should be visible.
    EXPECT_EQ("round_999", lc->getString("key"));
}
