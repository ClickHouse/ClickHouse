#pragma once

#include <mutex>
#include <string>
#include <unordered_map>

namespace DB
{

/// Tracks how many logical files reference each relative blob object key.
class PlainRewritableBlobRefcounts
{
public:
    uint32_t get(const std::string & relative_object_key) const
    {
        std::lock_guard guard(mutex);
        if (auto it = counts.find(relative_object_key); it != counts.end())
            return it->second;
        return 0;
    }

    void increment(const std::string & relative_object_key)
    {
        std::lock_guard guard(mutex);
        ++counts[relative_object_key];
    }

    /// Returns the remaining refcount after decrement (0 if the key was removed).
    uint32_t decrement(const std::string & relative_object_key)
    {
        std::lock_guard guard(mutex);
        auto it = counts.find(relative_object_key);
        if (it == counts.end())
            return 0;
        if (it->second <= 1)
        {
            counts.erase(it);
            return 0;
        }
        return --it->second;
    }

    void replaceAll(std::unordered_map<std::string, uint32_t> new_counts)
    {
        std::lock_guard guard(mutex);
        counts = std::move(new_counts);
    }

    std::unordered_map<std::string, uint32_t> snapshot() const
    {
        std::lock_guard guard(mutex);
        return counts;
    }

private:
    mutable std::mutex mutex;
    std::unordered_map<std::string, uint32_t> counts;
};

}
