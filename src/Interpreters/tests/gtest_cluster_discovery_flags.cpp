#include <gtest/gtest.h>

#include <condition_variable>
#include <mutex>
#include <string>
#include <unordered_map>

namespace
{

/// Mirrors ClusterDiscovery::Flags set / setIfPresent / remove contract used by watch callbacks.
template <typename T>
class UpdateFlags
{
public:
    void set(const T & key, bool value = true)
    {
        std::unique_lock<std::mutex> lk(mu);
        flags[key] = value;
        any_need_update |= value;
    }

    void setIfPresent(const T & key, bool value = true)
    {
        std::unique_lock<std::mutex> lk(mu);
        auto it = flags.find(key);
        if (it == flags.end())
            return;
        it->second = value;
        any_need_update |= value;
    }

    void remove(const T & key)
    {
        std::unique_lock<std::mutex> lk(mu);
        flags.erase(key);
    }

    bool contains(const T & key) const
    {
        std::unique_lock<std::mutex> lk(mu);
        return flags.contains(key);
    }

private:
    mutable std::mutex mu;
    std::unordered_map<T, bool> flags;
    bool any_need_update = true;
};

}

TEST(ClusterDiscoveryFlags, SetIfPresentDoesNotResurrectRemovedKey)
{
    UpdateFlags<std::string> flags;
    flags.set("gone");
    ASSERT_TRUE(flags.contains("gone"));

    flags.remove("gone");
    ASSERT_FALSE(flags.contains("gone"));

    /// Late Keeper callback after removal.
    flags.setIfPresent("gone");
    EXPECT_FALSE(flags.contains("gone"));

    /// Intentional re-registration may insert again.
    flags.set("gone");
    EXPECT_TRUE(flags.contains("gone"));
}

TEST(ClusterDiscoveryFlags, SetIfPresentUpdatesExistingKey)
{
    UpdateFlags<std::string> flags;
    flags.set("alive", false);
    flags.setIfPresent("alive", true);
    EXPECT_TRUE(flags.contains("alive"));
}
