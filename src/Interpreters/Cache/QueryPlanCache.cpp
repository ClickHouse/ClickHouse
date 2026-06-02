#include <Interpreters/Cache/QueryPlanCache.h>

#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/FieldVisitorHash.h>
#include <Common/SipHash.h>
#include <Core/Settings.h>
#include <Storages/ColumnDefault.h>


namespace ProfileEvents
{
    extern const Event QueryPlanCacheHits;
    extern const Event QueryPlanCacheMisses;
}

namespace CurrentMetrics
{
    extern const Metric QueryPlanCacheBytes;
    extern const Metric QueryPlanCacheEntries;
}

namespace DB
{

bool isSettingIgnoredInQueryPlanCache(std::string_view setting_name)
{
    /// Cache control: settings that select or sidestep the cache itself.
    return setting_name == "allow_experimental_query_plan_cache"
        || setting_name == "enable_query_plan_cache"
        || setting_name == "query_plan_cache_size_in_bytes_quota"
        || setting_name.starts_with("query_cache_")
        || setting_name.ends_with("_query_cache")
        /// Output formatting: applied after the plan and never baked into a plan step.
        || setting_name.starts_with("output_format_")
        || setting_name == "send_progress_in_http_headers"
        || setting_name == "http_response_headers"
        /// Generic resource limits: enforced by execution-time guards, not stored on any plan step.
        || setting_name == "max_execution_time"
        || setting_name == "max_memory_usage"
        || setting_name == "max_rows_to_read"
        || setting_name == "max_bytes_to_read"
        || setting_name == "max_result_rows"
        || setting_name == "max_result_bytes"
        || setting_name == "max_rows_in_set"
        || setting_name == "max_bytes_in_set"
        /// Thread fan-out is decided when the pipeline is built from the plan, not stored
        /// on the plan itself, so different thread counts can safely share a cached plan.
        || setting_name == "max_threads"
        || setting_name == "max_insert_threads"
        /// Logging and profiling: orthogonal to plan structure.
        || setting_name == "log_comment"
        || setting_name.starts_with("log_queries")
        || setting_name == "log_profile_events";
    /// Sort-related limits (`max_rows_to_sort`, `max_bytes_to_sort`) and `extremes` are
    /// intentionally NOT ignored: they are baked into `SortingStep` / `ExtremesStep`,
    /// so two queries with different values of these settings must not share a plan.
}
bool QueryPlanCacheKey::operator==(const QueryPlanCacheKey & other) const
{
    return ast_hash == other.ast_hash
        && semantic_settings_hash == other.semantic_settings_hash
        && table_metadata_versions == other.table_metadata_versions
        && storage_id.uuid == other.storage_id.uuid
        && row_policy_hash == other.row_policy_hash
        && user_id == other.user_id
        && current_user_roles == other.current_user_roles;
}

size_t QueryPlanCacheKeyHasher::operator()(const QueryPlanCacheKey & key) const
{
    SipHash hash;

    /// Hash the 128-bit AST hash as two 64-bit words.
    hash.update(key.ast_hash.low64);
    hash.update(key.ast_hash.high64);

    hash.update(key.semantic_settings_hash);

    /// Hash the per-table metadata versions map.
    for (const auto & [table, version] : key.table_metadata_versions)
    {
        hash.update(table);
        hash.update(version);
    }

    /// Hash storage UUID so that DROP+CREATE of the same name in an Atomic database
    /// does not collide with cached entries from the previous table.
    hash.update(key.storage_id.uuid);

    /// Hash row policy expression as two 64-bit words.
    hash.update(key.row_policy_hash.low64);
    hash.update(key.row_policy_hash.high64);

    /// Hash user identity.
    if (key.user_id.has_value())
        hash.update(*key.user_id);

    for (const auto & role_id : key.current_user_roles)
        hash.update(role_id);

    return hash.get64();
}

QueryPlanCache::QueryPlanCache(size_t max_size_in_bytes, size_t max_entries)
    : Base(CurrentMetrics::QueryPlanCacheBytes, CurrentMetrics::QueryPlanCacheEntries, max_size_in_bytes, max_entries)
{
}

void QueryPlanCache::updateConfiguration(size_t max_size_in_bytes, size_t max_entries)
{
    setMaxSizeInBytes(max_size_in_bytes);
    setMaxCount(max_entries);
    /// max_count=0 means "unlimited" in LRUCachePolicy, but for query plan cache
    /// we treat 0 as "disabled" -- clear all existing entries.
    if (max_size_in_bytes == 0 || max_entries == 0)
        clear();
}

QueryPlanCache::MappedPtr QueryPlanCache::get(const QueryPlanCacheKey & key)
{
    auto result = Base::get(key);
    if (result)
    {
        /// Reject entries with incompatible format version.
        if (result->format_version != QUERY_PLAN_CACHE_FORMAT_VERSION)
        {
            Base::remove(key);
            ProfileEvents::increment(ProfileEvents::QueryPlanCacheMisses);
            return nullptr;
        }
        ProfileEvents::increment(ProfileEvents::QueryPlanCacheHits);
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::QueryPlanCacheMisses);
    }
    return result;
}

void QueryPlanCache::set(const QueryPlanCacheKey & key, QueryPlanCacheEntry entry, size_t max_size_in_bytes_for_user)
{
    /// CacheBase treats max_count=0 as "unlimited", but for query plan cache
    /// we treat 0 as "disabled". Guard against inserting into a disabled cache.
    if (maxSizeInBytes() == 0 || maxCount() == 0)
        return;

    if (!canStoreForUser(key, entry, max_size_in_bytes_for_user))
        return;

    /// Stamp the inserter and the key so that `onEntryRemoval` can decrement the
    /// right user counter and erase the matching `entry_weights` record when this
    /// entry is later evicted.
    entry.inserter_user_id = key.user_id;
    entry.cache_key = key;
    const size_t entry_weight = QueryPlanCacheEntryWeight{}(entry);

    /// Skip entries that cannot fit globally: `Base::set` would admit them and
    /// `removeOverflow` would evict them synchronously. The eviction callback
    /// runs before we add the new weight to `per_user_bytes`, so a ghost charge
    /// would be left behind that future inserts could not amortize.
    if (entry_weight > maxSizeInBytes())
        return;

    /// Same-key replacement is silent in `LRU/SLRUCachePolicy::set`: it overwrites
    /// the existing cell without invoking `onEntryRemoval`. Account the prior
    /// entry's weight ourselves so `per_user_bytes` and `entry_weights` stay in sync.
    {
        std::lock_guard lock(per_user_mutex);
        if (auto it = entry_weights.find(key); it != entry_weights.end())
        {
            const auto & [old_user, old_weight] = it->second;
            if (old_user.has_value())
            {
                auto user_it = per_user_bytes.find(*old_user);
                if (user_it != per_user_bytes.end())
                {
                    if (user_it->second <= old_weight)
                        per_user_bytes.erase(user_it);
                    else
                        user_it->second -= old_weight;
                }
            }
            entry_weights.erase(it);
        }
    }

    Base::set(key, std::make_shared<QueryPlanCacheEntry>(std::move(entry)));

    {
        std::lock_guard lock(per_user_mutex);
        if (key.user_id.has_value())
            per_user_bytes[*key.user_id] += entry_weight;
        entry_weights[key] = {key.user_id, entry_weight};
    }
}

void QueryPlanCache::clear()
{
    Base::clear();
    std::lock_guard lock(per_user_mutex);
    per_user_bytes.clear();
    entry_weights.clear();
}

size_t QueryPlanCache::sizeInBytes() const
{
    return Base::sizeInBytes();
}

size_t QueryPlanCache::count() const
{
    return Base::count();
}

std::vector<QueryPlanCache::KeyMapped> QueryPlanCache::dump() const
{
    return Base::dump();
}

bool QueryPlanCache::canStoreForUser(const QueryPlanCacheKey & key, const QueryPlanCacheEntry & entry, size_t max_size_in_bytes_for_user) const
{
    if (!key.user_id.has_value() || max_size_in_bytes_for_user == 0)
        return true;

    const size_t new_entry_size = QueryPlanCacheEntryWeight{}(entry);

    /// Early rejection: a single entry larger than the quota can never fit.
    if (new_entry_size > max_size_in_bytes_for_user)
        return false;

    /// O(1) admission: read the cached per-user byte count maintained by `set` and
    /// `onEntryRemoval`. The check is best-effort — concurrent inserts may race past
    /// it, just like in `QueryResultCache`.
    std::lock_guard lock(per_user_mutex);
    auto it = per_user_bytes.find(*key.user_id);
    const size_t current_size_for_user = it == per_user_bytes.end() ? 0 : it->second;
    return current_size_for_user + new_entry_size <= max_size_in_bytes_for_user;
}

void QueryPlanCache::onEntryRemoval(size_t weight_loss, const MappedPtr & mapped_ptr)
{
    if (!mapped_ptr)
        return;

    std::lock_guard lock(per_user_mutex);

    if (mapped_ptr->inserter_user_id.has_value())
    {
        auto it = per_user_bytes.find(*mapped_ptr->inserter_user_id);
        if (it != per_user_bytes.end())
        {
            if (it->second <= weight_loss)
                per_user_bytes.erase(it);
            else
                it->second -= weight_loss;
        }
    }

    /// LRU/SLRU eviction is the only path that reaches `onEntryRemoval`: same-key
    /// replacement bypasses it (handled in `set` directly). Erase the tracking
    /// record so a future `set` for this key does not see a stale weight and
    /// double-decrement.
    if (mapped_ptr->cache_key.has_value())
        entry_weights.erase(*mapped_ptr->cache_key);
}

UInt64 SemanticSettings::computeHash(const Settings & settings)
{
    SipHash hash;

    for (const auto setting_name : settings.getAllRegisteredNames())
    {
        if (isSettingIgnoredInQueryPlanCache(setting_name))
            continue;

        hash.update(setting_name);
        applyVisitor(FieldVisitorHash(hash), settings.get(setting_name));
    }

    return hash.get64();
}

Int64 computeSchemaHash(const StorageInMemoryMetadata & metadata)
{
    SipHash hash;

    /// Hash all columns: name + type + default kind + default expression.
    /// The default kind (Default/Materialized/Alias/Ephemeral) and its expression
    /// must be part of the hash so that ALTER TABLE MODIFY COLUMN ... ALIAS/DEFAULT
    /// invalidates cached plans. ALIAS columns are evaluated at SELECT time and
    /// directly affect query results; DEFAULT/MATERIALIZED changes can also alter
    /// implicit column references in cached plans.
    for (const auto & column : metadata.columns)
    {
        hash.update(column.name);
        hash.update(column.type->getName());
        hash.update(static_cast<UInt8>(column.default_desc.kind));
        if (column.default_desc.expression)
            hash.update(column.default_desc.expression->formatForLogging());
    }

    /// Hash sorting key expression
    if (metadata.sorting_key.expression_list_ast)
        hash.update(metadata.sorting_key.expression_list_ast->formatForLogging());

    /// Hash partition key expression
    if (metadata.partition_key.expression_list_ast)
        hash.update(metadata.partition_key.expression_list_ast->formatForLogging());

    /// Hash primary key expression (may differ from sorting key)
    if (metadata.primary_key.expression_list_ast)
        hash.update(metadata.primary_key.expression_list_ast->formatForLogging());

    /// Mask to positive Int64 range to distinguish from metadata_version (small positive integers)
    return static_cast<Int64>(hash.get64() & 0x7FFFFFFFFFFFFFFF);
}

}
