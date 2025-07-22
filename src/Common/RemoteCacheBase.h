#pragma once
#include <Common/Exception.h>
#include <Common/ICachePolicy.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <base/UUID.h>
#include <base/defines.h>
#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <Poco/Types.h>
#include <Common/RedisCommon.h>


namespace DB
{

namespace ErrorCodes
{
extern const int EXTERNAL_QUERY_RESULT_CACHE_ERROR;
extern const int UNKNOWN_REDIS_SCRIPT;
extern const int DUPLICATED_REDIS_SCRIPT;
}

/// This is a thread-safe cache backed by an external distributed storage system.
/// It caches query results to avoid redundant executions across multiple nodes.
/// In contrast to local caching, external cache systems handle storage capacity and data expiration.
/// When writing data, a TTL (Time-To-Live) value is included,
/// and the caching system will automatically remove expired cache entries based on this TTL.
template <typename TKey, typename TMapped, typename HashFunction = std::hash<TKey>, typename WeightFunction = EqualWeightFunction<TMapped>>
class RemoteCacheBase
{
    static auto constexpr EVALSHA = "EVALSHA";
        /// Save key-value to external redis with expiration.
    static auto constexpr SET_SCRIPT = R"(
        if redis.call("setnx", KEYS[1], ARGV[1]) == 1 then
            redis.call("pexpire", KEYS[1], ARGV[2])
            return 1
        else
            return 0
        end
    )";

    ///  Batch read entries via SCAN+MGET commands.
    static auto constexpr DUMP_SCRIPT = R"(
        local cursor = ARGV[1]
        local pattern = ARGV[2]
        local count = tonumber(ARGV[3]) or 100
        local all_keys = {}
        local max_keys = tonumber(ARGV[4]) or 1024
        repeat
            local reply = redis.call("SCAN", cursor, "MATCH", pattern, "COUNT", count)
            cursor = reply[1]
            local keys = reply[2]
            for _, key in ipairs(keys) do
            if #all_keys < max_keys then
                table.insert(all_keys, key)
            end
        end
        if #all_keys >= max_keys then
            break
        end
        until cursor == "0"
        if #all_keys == 0 then
            return {}
        end
        return redis.call("MGET", unpack(all_keys))
    )";

    /// Batch clear key-values via SCAN + DEL commands.
    static auto constexpr CLEAR_SCRIPT = R"(
        local keys = redis.call('SCAN', 0, 'MATCH', ARGV[1], 'COUNT', ARGV[2])
        if #keys > 0 then
            return redis.call('DEL', unpack(keys))
        else
            return 0
        end
    )";
public:
    using Key = TKey;
    using Mapped = TMapped;
    using MappedPtr = std::shared_ptr<Mapped>;
    struct KeyMapped
    {
        Key key;
        MappedPtr mapped;
    };

    explicit RemoteCacheBase(std::shared_ptr<RedisConfiguration> config_)
        : config(config_)
        , pool(std::make_shared<RedisPool>(config->pool_size))
    {
        registerScript("SET", SET_SCRIPT);
        registerScript("DUMP", DUMP_SCRIPT);
        registerScript("CLEAR", CLEAR_SCRIPT);
    }
    virtual ~RemoteCacheBase() = default;

    void updateConfiguration(std::shared_ptr<RedisConfiguration> config_)
    {
        std::atomic_store(&config, config_);
    }

    std::optional<KeyMapped> getWithKey(const Key & key) noexcept
    {
        static auto constexpr GET = "GET";
        auto store_key = key.encodeTo();
        try
        {
            auto conn = getConnection();
            RedisCommand get_command(GET);
            get_command.add(store_key.c_str());
            auto ret = conn->client->template execute<Poco::Redis::BulkString>(get_command);
            if (ret.isNull())
            {
                misses++;
                return {};
            }
            ReadBufferFromString value_buffer(std::move(ret.value()));
            auto entry = std::make_shared<TMapped>();
            Key key_ = key;
            entry->deserializeWithKey(key_, value_buffer);
            hits++;
            return std::optional<KeyMapped>{}.emplace(key_, entry);
        }
        catch (const Exception & e)
        {
            /// If an exception occurs while reading from the external cache,
            /// log the error but do not throw the exception to prevent query interruption.
            LOG_ERROR(logger, "Failed to get key {}, caused by {}", store_key, e.displayText());
        }
        catch (...)
        {
            auto message = getCurrentExceptionMessage(false);
            LOG_ERROR(logger, "Failed to get key {}, caused by {}", store_key, message);
        }
        return {};
    }

    void set(const Key & key, const MappedPtr & val) noexcept
    {
        auto store_key = key.encodeTo();
        WriteBufferFromOwnString val_buffer;
        val->serializeWithKey(key, val_buffer);
        auto ttl_ms = std::chrono::duration_cast<std::chrono::milliseconds>(key.expires_at - std::chrono::system_clock::now()).count();
        try
        {
            auto conn = getConnection();
            RedisCommand eval(EVALSHA);
            eval << getLuaScriptHash("SET") << "1" << store_key << val_buffer.str() << std::to_string(ttl_ms);
            conn->client->template execute<Poco::Int64>(eval);
        }
        catch (const Exception & e)
        {
            /// If an exception occurs while writing key-value to the external cache,
            /// log the error but do not throw the exception to prevent query interruption.
            LOG_ERROR(logger, "Failed to call set key {}, caused by {}", store_key, e.displayText());
        }
        catch (...)
        {
            auto message = getCurrentExceptionMessage(false);
            LOG_ERROR(logger, "Failed to call set key {}, caused by {}", store_key, message);
        }
    }

    void remove(const Key & key) noexcept
    {
        static auto constexpr DEL = "DEL";
        auto store_key = key.encodeTo();
        try
        {
            auto conn = getConnection();
            RedisCommand del(DEL);
            del.add(store_key.c_str());
            conn->client->template execute<Poco::Int64>(del);
        }
        catch (const Exception & e)
        {
            LOG_ERROR(logger, "Failed to remove key {}, caused by {}", store_key, e.displayText());
            throw Exception(ErrorCodes::EXTERNAL_QUERY_RESULT_CACHE_ERROR, "Failed to remove key {} caused by {}", store_key, e.displayText());
        }
        catch (...)
        {
            auto message = getCurrentExceptionMessage(false);
            LOG_ERROR(logger, "Failed to remove key {}, caused by {}", store_key, message);
            throw Exception(ErrorCodes::EXTERNAL_QUERY_RESULT_CACHE_ERROR, "Failed to remove key {} caused by {}", store_key, message);
        }
    }

    void getStats(size_t & out_hits, size_t & out_misses) const
    {
        out_hits = hits;
        out_misses = misses;
    }

    /// Fetching query results from external cache is extremely costly, therefore this functionality is not supported.
    std::vector<KeyMapped> dump() const
    {
        std::vector<KeyMapped> ret;
        try
        {
            auto conn = getConnection();
            RedisCommand eval(EVALSHA);
            eval << getLuaScriptHash("DUMP") << "0" << "0" << "*" << std::to_string(default_operate_entry_batch) << std::to_string(default_dump_entry_count);
            auto values = conn->client->template execute<Poco::Redis::Array>(eval);
            if (values.isNull())
                return ret;
            for (size_t i = 0; i < values.size(); i++)
            {
                auto nullable_string = values.template get<Poco::Redis::BulkString>(i);
                if (nullable_string.isNull())
                    continue;
                ReadBufferFromString value_buffer(nullable_string.value());
                auto entry = std::make_shared<TMapped>();
                Key key_;
                entry->deserializeWithKey(key_, value_buffer);
                ret.emplace_back(key_, entry);
            }
        }
        catch (const Exception & e)
        {
            LOG_ERROR(logger, "Failed to dump, caused by {}", e.displayText());
            throw Exception(ErrorCodes::EXTERNAL_QUERY_RESULT_CACHE_ERROR, "Failed to dump, caused by {}", e.displayText());
        }
        catch (...)
        {
            auto message = getCurrentExceptionMessage(false);
            throw Exception(ErrorCodes::EXTERNAL_QUERY_RESULT_CACHE_ERROR, "Failed to dump, caused by {}", message);
        }
        return ret;
    }

    void clearByTag(const std::string & tag)
    {
        auto batch_remove = [&] (const std::string & tag_) -> int
        {
            auto conn = getConnection();
            RedisCommand eval(EVALSHA);
            eval << getLuaScriptHash("CLEAR") << "0" << tag_ << ":*" << std::to_string(default_operate_entry_batch);
            return conn->client->template execute<Poco::Int64>(eval);
        };
        try
        {
            while (batch_remove(tag) > 0)
            {
            }
        }
        catch (const Exception & e)
        {
            LOG_ERROR(logger, "Failed to call clear({}), caused by {}", tag, e.displayText());
            throw Exception(ErrorCodes::EXTERNAL_QUERY_RESULT_CACHE_ERROR, "Failed to clear cache({}), caused by {}", tag, e.displayText());
        }
        catch (...)
        {
            auto message = getCurrentExceptionMessage(false);
            LOG_ERROR(logger, "Failed to call clear({}), caused by {}", tag, message);
            throw Exception(ErrorCodes::EXTERNAL_QUERY_RESULT_CACHE_ERROR, "Failed to clear cache({}), caused by {}", tag, message);
        }
    }

    void clear()
    {
        static auto constexpr FLUSH_ALL = "FLUSHALL";
        try
        {
            auto conn = getConnection();
            RedisCommand flush_all(FLUSH_ALL);
            conn->client->template execute<String>(flush_all);
            hits = 0;
            misses = 0;
        }
        catch (const Exception & e)
        {
            LOG_ERROR(logger, "Failed to clear, caused by {}", e.displayText());
            throw Exception(ErrorCodes::EXTERNAL_QUERY_RESULT_CACHE_ERROR, "Failed to clear cache, caused by {}", e.displayText());
        }
        catch (...)
        {
            auto message = getCurrentExceptionMessage(false);
            LOG_ERROR(logger, "Failed to clear, caused by {}", message);
            throw Exception(ErrorCodes::EXTERNAL_QUERY_RESULT_CACHE_ERROR, "Failed to clear cache, caused by {}", message);
        }
    }

    size_t sizeInBytes() const
    {
        return 0;
    }

    size_t count() const
    {
        static auto constexpr DBSIZE = "DBSIZE";
        try
        {
            auto conn = getConnection();
            RedisCommand dbsize(DBSIZE);
            return conn->client->template execute<Poco::Int64>(dbsize);
        }
        catch (const Exception & e)
        {
            LOG_ERROR(logger, "Failed to execute dbsize, caused by {}", e.displayText());
            throw Exception(ErrorCodes::EXTERNAL_QUERY_RESULT_CACHE_ERROR, "Failed to execute DBSIZE, caused by {}", e.displayText());
        }
        catch (...)
        {
            auto message = getCurrentExceptionMessage(false);
            throw Exception(ErrorCodes::EXTERNAL_QUERY_RESULT_CACHE_ERROR, "Failed to execute DBSIZE, caused by {}", message);
        }
        std::unreachable();
    }

    size_t maxSizeInBytes() const
    {
        return 0;
    }

    void setMaxCount(size_t)
    {
    }

    void setMaxSizeInBytes(size_t)
    {
    }

    void setQuotaForUser(const UUID &, size_t, size_t)
    {
    }

private:
    std::shared_ptr<RedisConfiguration> config { nullptr };
    RedisPoolPtr pool { nullptr };
    std::atomic<size_t> hits{0};
    std::atomic<size_t> misses{0};
    size_t default_dump_entry_count = 1024;
    size_t default_operate_entry_batch = 1024;

    LoggerPtr logger = getLogger("RemoteCache");

    /// This variable `script_hashes is initialized in the constructor and remains read-only in all other scenarios.
    /// No lock protection is required.
    std::unordered_map<std::string, std::string> script_hashes;
    std::string getLuaScriptHash(const std::string & name) const
    {
        const auto it = script_hashes.find(name);
        if (it == script_hashes.end())
            throw Exception(ErrorCodes::UNKNOWN_REDIS_SCRIPT, "Unknown script name {}", name);
        return it->second;
    }
    void registerScript(const std::string & name, const std::string & script)
    {
        if (script_hashes.contains(name))
            throw Exception(ErrorCodes::DUPLICATED_REDIS_SCRIPT, "Duplicate script name {}", name);
        try
        {
            RedisArray command;
            command.add("SCRIPT").add("LOAD").add(script);
            auto conn = getConnection();
            auto sha = conn->client->template execute<Poco::Redis::BulkString>(command);
            assert(sha.isNull() == false);
            script_hashes.emplace(name, sha.value());
        }
        catch (const Exception & e)
        {
            throw Exception(ErrorCodes::EXTERNAL_QUERY_RESULT_CACHE_ERROR, "External query result cache error {}", e.displayText());
        }
        catch (...)
        {
            auto message = getCurrentExceptionMessage(false);
            throw Exception(ErrorCodes::EXTERNAL_QUERY_RESULT_CACHE_ERROR, "External query result cache error {}", message);
        }
    }

    /// Override this method if you want to track how much weight was lost in removeOverflow method.
    virtual void onRemoveOverflowWeightLoss(size_t /*weight_loss*/) {}
    virtual void onRemove(const MappedPtr & /*mapped*/) {}
    RedisConnectionPtr getConnection() const
    {
        auto config_ = std::atomic_load(&config);
        return getRedisConnection(pool, *config);
    }
};

}

