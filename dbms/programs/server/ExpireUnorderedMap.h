#pragma once

#include "IServer.h"
#include "WebTerminalSessionQuery.h"

#include <Common/HTMLForm.h>
#include <DataStreams/BlockIO.h>
#include <Interpreters/Context.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Common/setThreadName.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_EXPIRE_KEY;
}

template <typename Key, typename Value>
class ExpireUnorderedMap
{
private:
    struct ExpireEntityInfo
    {
        UInt64 close_cycle = 0;
        std::chrono::steady_clock::duration timeout;
    };

    UInt64 close_cycle = 0;
    std::chrono::steady_clock::time_point close_cycle_time = std::chrono::steady_clock::now();

    std::unordered_map<UInt64, std::vector<Key>> expire_keys_map;
    std::unordered_map<Key, std::pair<std::shared_ptr<Value>, ExpireEntityInfo>> data;

    static constexpr std::chrono::steady_clock::duration close_interval = std::chrono::seconds(1);

public:
    ~ExpireUnorderedMap()
    {
        try
        {
            {
                std::lock_guard lock{mutex};
                quit = true;
            }

            cond.notify_one();
            thread.join();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    ExpireUnorderedMap() {}

    void erase(const Key & key)
    {
        std::unique_lock lock(mutex);
        data.erase(key);
    }

    std::shared_ptr<Value> & get(const Key & key, const String & not_found_msg = "")
    {
        std::unique_lock lock(mutex);

        if (data.find(key) == data.end())
            throw Exception(not_found_msg.empty() ? key + " not found in ExpireUnorderedMap" : not_found_msg,
                ErrorCodes::UNKNOWN_EXPIRE_KEY);

        return getImpl(key);
    }

    template <typename... Args>
    std::shared_ptr<Value> emplace(const Key & key, const std::chrono::steady_clock::duration & expire_timeout, Args &&... args)
    {
        std::unique_lock lock(mutex);

        if (data.find(key) == data.end())
        {
            const auto entry_close_cycle = close_cycle + (expire_timeout / close_interval + 1);
            auto emplace_data_value = std::make_shared<Value>(std::forward<Args>(args)...);
            ExpireEntityInfo emplace_data_info {.close_cycle = entry_close_cycle, .timeout = expire_timeout};

            expire_keys_map[entry_close_cycle].emplace_back(key);
            return data.insert(std::make_pair(key, std::make_pair(emplace_data_value, emplace_data_info))).first->second.first;
        }

        return getImpl(key);
    }

private:
    std::shared_ptr<Value> & getImpl(const Key & key)
    {
        const auto new_close_cycle = close_cycle + (data[key].second.timeout / close_interval + 1);
        data[key].second.close_cycle = new_close_cycle;
        expire_keys_map[new_close_cycle].emplace_back(key);
        return data[key].first;
    }

    void expiredCleanerMain()
    {
        setThreadName("ExpiredCleaner");

        std::unique_lock lock{mutex};

        while (true)
        {
            const auto now = std::chrono::steady_clock::now();

            if (now < close_cycle_time)
            {
                if (cond.wait_for(lock, close_cycle_time - now, [this]() -> bool { return quit; }))
                    break;
            }

            ++close_cycle;
            close_cycle_time = now + close_interval;

            if (expire_keys_map.find(close_cycle) != expire_keys_map.end())
            {
                for (const auto & maybe_expired_key : expire_keys_map[close_cycle])
                {
                    const auto iterator = data.find(maybe_expired_key);
                    if (iterator != data.end() && iterator->second.second.close_cycle <= close_cycle)
                    {
                        if (iterator->second.first.unique())
                            data.erase(iterator);
                        else
                        {
                            iterator->second.second.close_cycle += 1;
                            expire_keys_map[iterator->second.second.close_cycle].emplace_back(maybe_expired_key);
                        }
                    }
                }

                expire_keys_map.erase(close_cycle);
            }

            if (cond.wait_for(lock, close_interval, [this]() -> bool { return quit; }))
                break;
        }
    }

    std::mutex mutex;
    std::condition_variable cond;
    std::atomic<bool> quit{false};
    ThreadFromGlobalPool thread{&ExpireUnorderedMap::expiredCleanerMain, this};
};

}
