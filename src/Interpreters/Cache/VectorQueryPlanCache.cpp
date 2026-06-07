#include <algorithm>
#include <functional>
#include <memory>
#include <utility>
#include <Interpreters/Cache/VectorQueryPlanCache.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/IParser.h>
#include <Parsers/TokenIterator.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Common/Logger.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/TTLCachePolicy.h>
#include <Common/CurrentMetrics.h>
#include <Common/FieldVisitorToString.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <base/defines.h>

namespace ProfileEvents
{
    extern const Event VectorASTCacheHits;
    extern const Event VectorASTCacheMisses;
    extern const Event VectorQueryPlanCacheHits;
    extern const Event VectorQueryPlanCacheMisses;
}

namespace CurrentMetrics
{
    extern const Metric VectorQueryPlanCacheBytes;
    extern const Metric VectorQueryPlanCacheEntries;
}

namespace DB
{

namespace Setting
{   
    extern const SettingsString vector_query_plan_cache_tag;
}

namespace
{

bool isVectorQueryPlanCacheRelatedSetting(const String & setting_name)
{
    // Any setting that controls the plan cache itself should not affect the cache key.
    return (setting_name.find("vector_query_plan_cache") != String::npos) && setting_name != "vector_query_plan_cache_tag";
}

std::vector<std::pair<String, String>> getRelevantSettings(const Settings & settings)
{
    // Build a stable, deterministic list of settings that can influence planning decisions.
    SettingsChanges changed_settings = settings.changes();
    std::vector<std::pair<String, String>> changed_settings_sorted; /// (name, value)
    changed_settings_sorted.reserve(16);
    for (const auto & setting : changed_settings)
    {
        const String & name = setting.name;
        const String & value = applyVisitor(FieldVisitorToString(), setting.value);
        if (!isVectorQueryPlanCacheRelatedSetting(name) && name != "allow_experimental_analyzer") /// this setting doesn't affect plan cache key
            changed_settings_sorted.push_back({name, value});
    }
    std::sort(changed_settings_sorted.begin(), changed_settings_sorted.end(), [](auto & lhs, auto & rhs) { return lhs.first < rhs.first; });
    return changed_settings_sorted;
}

String getCanonicalTableName(const StorageID & table_id)
{
    // The reverse invalidation index stores unquoted `db.table` names so entries
    // created by different call sites still compare by a single canonical form.
    return table_id.getFullNameNotQuoted();
}

using NameSetForInvalidation = std::unordered_set<String>;

void addNameIfNotEmpty(std::vector<String> & names, NameSetForInvalidation & seen, const String & name)
{
    if (name.empty())
        return;

    if (seen.emplace(name).second)
        names.push_back(name);
}

void addResolvedTableForInvalidation(
    std::vector<String> & table_names,
    NameSetForInvalidation & seen_tables,
    ContextPtr context,
    const StorageID & table_id)
{
    // Mutating queries may omit the database name or refer to temporary names.
    // Always resolve through Context before touching the reverse invalidation index.
    try
    {
        StorageID resolved_table_id = context->resolveStorageID(table_id, Context::ResolveOrdinary);
        addNameIfNotEmpty(table_names, seen_tables, getCanonicalTableName(resolved_table_id));
    }
    catch (...)
    {
        LOG_DEBUG(getLogger("VectorQueryPlanCache"), "addResolvedTableForInvalidation error");
    }
}

void addResolvedTableForInvalidation(
    std::vector<String> & table_names,
    NameSetForInvalidation & seen_tables,
    ContextPtr context,
    const ASTQueryWithTableAndOutput & query)
{
    const String table_name = query.getTable();
    if (table_name.empty())
        return;

    addResolvedTableForInvalidation(
        table_names,
        seen_tables,
        context,
        StorageID(query.getDatabase(), table_name, query.uuid));
}

void collectMutationTargets(
    const ASTPtr & ast,
    ContextPtr context,
    std::vector<String> & table_names,
    std::vector<String> & database_names)
{
    // VectorQueryPlanCache is only populated for SELECTs, but writes to tables or table
    // metadata may invalidate those cached SELECTs. This helper recognizes the main
    // mutating AST families and returns the precise table/database scopes to clear.
    NameSetForInvalidation seen_tables;
    NameSetForInvalidation seen_databases;

    if (const auto * insert_query = ast->as<ASTInsertQuery>())
    {
        if (insert_query->table_id)
            addNameIfNotEmpty(table_names, seen_tables, getCanonicalTableName(insert_query->table_id));
        else if (!insert_query->getTable().empty())
            addResolvedTableForInvalidation(table_names, seen_tables, context, StorageID(insert_query->getDatabase(), insert_query->getTable()));
        return;
    }

    if (const auto * delete_query = ast->as<ASTDeleteQuery>())
    {
        addResolvedTableForInvalidation(table_names, seen_tables, context, *delete_query);
        return;
    }

    if (const auto * alter_query = ast->as<ASTAlterQuery>())
    {
        if (alter_query->alter_object == ASTAlterQuery::AlterObjectType::DATABASE)
            addNameIfNotEmpty(database_names, seen_databases, alter_query->getDatabase());
        else
            addResolvedTableForInvalidation(table_names, seen_tables, context, *alter_query);
        return;
    }

    if (const auto * drop_query = ast->as<ASTDropQuery>())
    {
        if (drop_query->database_and_tables)
        {
            // Multi-table DROP/TRUNCATE is normalized into per-table ASTs so the rest
            // of the invalidation logic can stay uniform.
            auto rewritten_drop = drop_query->clone();
            for (const auto & rewritten_ast : rewritten_drop->as<ASTDropQuery &>().getRewrittenASTsOfSingleTable(rewritten_drop))
            {
                if (const auto * single_drop = rewritten_ast->as<ASTDropQuery>())
                {
                    if (!single_drop->getTable().empty())
                        addResolvedTableForInvalidation(table_names, seen_tables, context, *single_drop);
                    else
                        addNameIfNotEmpty(database_names, seen_databases, single_drop->getDatabase());
                }
            }
            return;
        }

        if (drop_query->has_all || (drop_query->getTable().empty() && !drop_query->getDatabase().empty()))
        {
            addNameIfNotEmpty(database_names, seen_databases, drop_query->getDatabase());
            return;
        }

        addResolvedTableForInvalidation(table_names, seen_tables, context, *drop_query);
        return;
    }

    if (const auto * rename_query = ast->as<ASTRenameQuery>())
    {
        for (const auto & element : rename_query->getElements())
        {
            if (rename_query->database)
            {
                addNameIfNotEmpty(database_names, seen_databases, element.from.getDatabase());
                addNameIfNotEmpty(database_names, seen_databases, element.to.getDatabase());
                continue;
            }

            addResolvedTableForInvalidation(
                table_names,
                seen_tables,
                context,
                StorageID(!element.from.database ? context->getCurrentDatabase() : element.from.getDatabase(), element.from.getTable()));
            addResolvedTableForInvalidation(
                table_names,
                seen_tables,
                context,
                StorageID(!element.to.database ? context->getCurrentDatabase() : element.to.getDatabase(), element.to.getTable()));
        }
        return;
    }

    if (const auto * create_query = ast->as<ASTCreateQuery>())
    {
        if (!create_query->getTable().empty())
            addResolvedTableForInvalidation(table_names, seen_tables, context, *create_query);
    }
}

}

VectorQueryPlanCache::Key::Key(
    String query_string_,
    const String & current_database_,
    const Settings & settings,
    Block header_,
    std::optional<UUID> user_id_,
    const std::vector<UUID> & current_user_roles_,
    bool is_shared_,
    std::chrono::time_point<std::chrono::system_clock> expires_at_,
    bool is_compressed_)
    : query_string(std::move(query_string_))
    , current_database(current_database_)
    , relevant_settings(getRelevantSettings(settings))
    , header(std::move(header_))
    , header_structure(this->header.dumpStructure())
    , user_id(user_id_)
    , current_user_roles(current_user_roles_)
    , is_shared(is_shared_)
    , expires_at(expires_at_)
    , tag(settings[Setting::vector_query_plan_cache_tag])
    , is_compressed(is_compressed_)
{
    // Sort roles to ensure deterministic key regardless of order from source
    std::sort(current_user_roles.begin(), current_user_roles.end());
}

VectorQueryPlanCache::Key::Key(
    String query_string_,
    const String & current_database_,
    const Settings & settings,
    std::optional<UUID> user_id_,
    const std::vector<UUID> & current_user_roles_)
    : query_string(std::move(query_string_))
    , current_database(current_database_)
    , relevant_settings(getRelevantSettings(settings))
    , header({})
    , header_structure(this->header.dumpStructure())
    , user_id(user_id_)
    , current_user_roles(current_user_roles_)
    , is_shared(false)
    , expires_at(std::chrono::system_clock::from_time_t(1))
    , tag(settings[Setting::vector_query_plan_cache_tag])
    , is_compressed(false)
{
    // Sort roles to ensure deterministic key regardless of order from source
    std::sort(current_user_roles.begin(), current_user_roles.end());
}

bool VectorQueryPlanCache::Key::operator==(const Key & other) const
{
    return query_string == other.query_string &&
           current_database == other.current_database &&
           relevant_settings == other.relevant_settings &&
           tag == other.tag &&
           user_id == other.user_id &&
           current_user_roles == other.current_user_roles &&
           is_shared == other.is_shared &&
           header_structure == other.header_structure;
}

size_t VectorQueryPlanCache::KeyHasher::operator()(const Key & key) const
{
    SipHash hash;
    hash.update(key.query_string);
    hash.update(key.current_database);
    for (const auto & setting : key.relevant_settings)
    {
        hash.update(setting.first);
        hash.update(setting.second);
    }
    hash.update(key.tag);
    if (key.user_id)
        hash.update(key.user_id->toUnderType());
    else
        hash.update(static_cast<UInt128>(0));
    for (const auto & role_id : key.current_user_roles)
        hash.update(role_id.toUnderType());
    hash.update(key.is_shared);
    // Hash the cached structure string used by operator==. This keeps hashing
    // consistent with equality and avoids walking the Block again on every lookup.
    hash.update(key.header_structure);
    return hash.get64();
}

size_t VectorQueryPlanCache::EntryWeight::operator()(const Entry & entry) const
{
    return entry.plan_size + entry.ast_size;
}

bool VectorQueryPlanCache::IsStale::operator()(const Key & key) const
{
    return key.expires_at < std::chrono::system_clock::now();
}

VectorQueryPlanCache::Writer::Writer(
    VectorQueryPlanCache & owner_,
    Cache & cache_,
    const VectorQueryPlanCache::Key & key_,
    size_t max_entry_size_in_bytes_,
    size_t max_entry_size_in_rows_,
    std::chrono::milliseconds min_plan_build_time_)
    : owner(owner_)
    , cache(cache_)
    , key(key_)
    , max_entry_size_in_bytes(max_entry_size_in_bytes_)
    , max_entry_size_in_rows(max_entry_size_in_rows_)
    , query_start_time(std::chrono::system_clock::now())
    , min_plan_build_time(min_plan_build_time_)
{
    // If a non-stale entry already exists, avoid overwriting it with a new writer.
    if (auto entry = cache.getWithKey(key); entry.has_value() && !IsStale()(entry->key))
    {
        LOG_TRACE(logger, "Skipped insert because the cache contains a non-stale query plan for query {}", doubleQuoteString(key.query_string)); 
        skip_insert = true;
    }
}

VectorQueryPlanCache::Writer::Writer(const Writer & other)
    : owner(other.owner)
    , cache(other.cache)
    , key(other.key)
    , max_entry_size_in_bytes(other.max_entry_size_in_bytes)
    , max_entry_size_in_rows(other.max_entry_size_in_rows)
    , query_start_time(other.query_start_time)
    , min_plan_build_time(other.min_plan_build_time)
    , query_plan(other.query_plan)  // Copy the shared_ptr directly, which is thread-safe
    , skip_insert(other.skip_insert.load())
    , was_finalized(other.was_finalized.load())
    , logger(other.logger)
{
    // query_plan is a shared_ptr, so direct copy is thread-safe
    // No need to lock other.mutex as shared_ptr copy is atomic
}

void VectorQueryPlanCache::Writer::setPlan(QueryPlan & plan)
{
    std::lock_guard lock(mutex);
    if (skip_insert)
        return;

    /// Store a cloned plan to avoid mutation during pipeline build.
    try
    {
        query_plan->plan_cache = std::make_shared<QueryPlan>(plan.clone());
    }
    catch (const Exception & e)
    {
        LOG_TRACE(logger, "Skipped plan clone for query {}: {}", doubleQuoteString(key.query_string), e.message()); 
    }
}

void VectorQueryPlanCache::Writer::setAst(const ASTPtr & ast)
{
    std::lock_guard lock(mutex);
    if (skip_insert)
        return;

    if (!ast)
        return;

    try
    {
        // Store a cloned AST snapshot to avoid sharing mutable query trees.
        query_plan->ast = ast->clone();
        query_plan->ast_size = query_plan->ast->formatForErrorMessage().size();
    }
    catch (const Exception & e)
    {
        LOG_TRACE(logger, "Skipped AST clone for query {}: {}", doubleQuoteString(key.query_string), e.message()); 
    }
}

void VectorQueryPlanCache::Writer::setAstLiteralPositions(std::vector<ASTLiteralPosition> ast_literal_positions)
{
    std::lock_guard lock(mutex);
    if (skip_insert)
        return;

    query_plan->ast_literal_positions = std::move(ast_literal_positions);
}

void VectorQueryPlanCache::Writer::setPlanConstantBindings(std::vector<PlanConstantBinding> plan_constant_bindings)
{
    std::lock_guard lock(mutex);
    if (skip_insert)
        return;

    query_plan->plan_constant_bindings = std::move(plan_constant_bindings);
}

void VectorQueryPlanCache::Writer::setTableNames(std::vector<String> table_names)
{
    std::lock_guard lock(mutex);
    if (skip_insert)
        return;

    // The writer stores resolved source tables together with the entry so publication
    // can rebuild the reverse invalidation map in one place.
    query_plan->table_names = std::move(table_names);
}

void VectorQueryPlanCache::Writer::finalizeWrite()
{
    Cache::MappedPtr query_plan_snapshot;
    {
        std::lock_guard lock(mutex);
        if (skip_insert)
            return;

        // Protect against null entry due to unexpected allocation failure or race.
        if (!query_plan)
            return;

        query_plan_snapshot = query_plan;
    }

    // Pre-compute AST size outside the critical section to reduce lock contention.
    // serializeAST() can be expensive for large ASTs.
    if (query_plan_snapshot->ast && query_plan_snapshot->ast_size == 0)
        query_plan_snapshot->ast_size = query_plan_snapshot->ast->formatForErrorMessage().size();

    bool needs_rebuild_mappings = false;
    {
        std::lock_guard lock(mutex);
        if (skip_insert)
            return;

        if (was_finalized.load())
            return;

        // Skip caching if the query is too fast; we only cache expensive plan builds.
        if (auto query_runtime = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - query_start_time); query_runtime < min_plan_build_time)
        {
            LOG_TRACE(logger, "Skipped insert because the query is not expensive enough, query runtime: {} msec (minimum query runtime: {} msec), query: {}",
                query_runtime.count(), min_plan_build_time.count(), doubleQuoteString(key.query_string));
            return;
        }    

        auto existing_entry = cache.getWithKey(key);
        if (existing_entry.has_value())
        {
            const auto & existing = existing_entry->mapped;
            const bool existing_is_stale = IsStale()(existing_entry->key);

            if (!existing_is_stale)
            {
                const bool has_new_component
                    = (query_plan_snapshot->plan_cache && !existing->plan_cache)
                    || (query_plan_snapshot->ast && !existing->ast)
                    || (!query_plan_snapshot->ast_literal_positions.empty() && existing->ast_literal_positions.empty())
                    || (!query_plan_snapshot->plan_constant_bindings.empty() && existing->plan_constant_bindings.empty());

                if (!has_new_component)
                    return;
            }

            // Preserve missing components by merging with the previous entry if it exists.
            if (!query_plan_snapshot->plan_cache && existing->plan_cache)
            {
                query_plan_snapshot->plan_cache = existing->plan_cache;
                query_plan_snapshot->plan_size = existing->plan_size;
            }
            if (!query_plan_snapshot->ast && existing->ast)
            {
                query_plan_snapshot->ast = existing->ast;
                query_plan_snapshot->ast_size = existing->ast_size;
            }
            if (query_plan_snapshot->ast_literal_positions.empty() && !existing->ast_literal_positions.empty())
                query_plan_snapshot->ast_literal_positions = existing->ast_literal_positions;
            if (query_plan_snapshot->plan_constant_bindings.empty() && !existing->plan_constant_bindings.empty())
                query_plan_snapshot->plan_constant_bindings = existing->plan_constant_bindings;
            if (query_plan_snapshot->table_names.empty() && !existing->table_names.empty())
                query_plan_snapshot->table_names = existing->table_names;
        }

        // Charge plan+AST size and enforce per-entry cap.
        // AST size was pre-computed before acquiring the lock.
        size_t new_entry_size_in_bytes = query_plan_snapshot->plan_size + query_plan_snapshot->ast_size;

        if (new_entry_size_in_bytes > max_entry_size_in_bytes)
        {
            LOG_TRACE(logger, "Skipped insert because the query plan is too big, query plan size: {} (maximum size: {}), query: {}",
                formatReadableSizeWithBinarySuffix(new_entry_size_in_bytes, 0), formatReadableSizeWithBinarySuffix(max_entry_size_in_bytes, 0), doubleQuoteString(key.query_string));
            return;
        }

        // Capture the flag locally to know if we need to rebuild mappings
        needs_rebuild_mappings = !was_finalized.load();

        cache.set(key, query_plan_snapshot);
        was_finalized.store(true);
        LOG_TRACE(logger, "Stored query result of query {}", doubleQuoteString(key.query_string));
    } // lock goes out of scope here
    
    if (needs_rebuild_mappings)
    {
        std::lock_guard cache_lock(owner.mutex);
        owner.rebuildTableMappings();
    }
}

VectorQueryPlanCache::Reader::Reader(Cache & cache_, const Cache::Key & key, const std::lock_guard<std::mutex> &)
{
    // Snapshot the cache entry under a mutex to keep it consistent for this reader.
    auto entry = cache_.getWithKey(key);
    if (!entry.has_value())
    {
        LOG_TRACE(logger, "No cache entry found for query {}", doubleQuoteString(key.query_string));
        return;
    }

    const auto & entry_key = entry->key;
    const auto & entry_mapped_ref = entry->mapped;

    const bool is_same_user = ((!entry_key.user_id.has_value() && !key.user_id.has_value())
        || (entry_key.user_id.has_value() && key.user_id.has_value() && *entry_key.user_id == *key.user_id));
    const bool is_same_roles = (entry_key.current_user_roles == key.current_user_roles);
    if (!entry_key.is_shared && (!is_same_user || !is_same_roles))
    {
        // Private entries are only visible to the same user+roles.
        LOG_TRACE(logger, "Inaccessible cache entry for query {}", doubleQuoteString(key.query_string));
        return;
    }

    if (IsStale()(entry_key))
    {
        LOG_TRACE(logger, "Stale cache entry for query {}", doubleQuoteString(key.query_string));
        return;
    }
    // Copy shared_ptr snapshots so the reader does not hold the cache lock.
    this->entry_mapped = entry_mapped_ref;
    plan = entry_mapped_ref->plan_cache;
    ast = entry_mapped_ref->ast;
    ast_literal_positions = entry_mapped_ref->ast_literal_positions;
    plan_constant_bindings = entry_mapped_ref->plan_constant_bindings;
    
    // Store sizes for deferred statistics counting
    plan_size = entry_mapped_ref->plan_size;
    ast_size = entry_mapped_ref->ast_size;
}

bool VectorQueryPlanCache::Reader::hasCacheEntryForKey(bool update_profile_events) const
{
    bool has_entry = static_cast<bool>(plan);
    if (update_profile_events)
    {
        if (has_entry)
        {
            ProfileEvents::increment(ProfileEvents::VectorQueryPlanCacheHits);
        }
        else
            ProfileEvents::increment(ProfileEvents::VectorQueryPlanCacheMisses);
    }
    return has_entry;
}

bool VectorQueryPlanCache::Reader::hasAstCacheEntryForKey(bool update_profile_events) const
{
    bool has_entry = static_cast<bool>(ast);
    if (update_profile_events)
    {
        if (has_entry)
        {
            ProfileEvents::increment(ProfileEvents::VectorASTCacheHits);
        }
        else
            ProfileEvents::increment(ProfileEvents::VectorASTCacheMisses);
    }
    return has_entry;
}

QueryPlanPtr VectorQueryPlanCache::Reader::getPlan()
{
    if (!plan)
        return {};
    /// Return a fresh clone for each cache hit to keep steps reusable.
    try
    {
        // Each caller receives its own plan instance to avoid shared mutable state.
        return std::make_unique<QueryPlan>(plan->clone());
    }
    catch (const Exception & e)
    {
        LOG_TRACE(logger, "Failed to clone cached plan: {}", e.message()); 
        return {};
    }
}

ASTPtr VectorQueryPlanCache::Reader::getAst()
{
    if (!ast)
        return {};
    try
    {
        return ast->clone();
    }
    catch (const Exception & e)
    {
        LOG_TRACE(logger, "Failed to clone cached AST: {}", e.message()); 
        return {};
    }
}

const std::vector<VectorQueryPlanCache::ASTLiteralPosition> & VectorQueryPlanCache::Reader::getAstLiteralPositions() const
{
    return ast_literal_positions;
}

const std::vector<VectorQueryPlanCache::PlanConstantBinding> & VectorQueryPlanCache::Reader::getPlanConstantBindings() const
{
    return plan_constant_bindings;
}

VectorQueryPlanCache::VectorQueryPlanCache(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_)
    : cache(std::make_unique<Cache>(std::make_unique<TTLCachePolicy<Key, Entry, KeyHasher, EntryWeight, IsStale>>(
        CurrentMetrics::VectorQueryPlanCacheBytes,
        CurrentMetrics::VectorQueryPlanCacheEntries,
        std::make_unique<PerUserTTLCachePolicyUserQuota>())))
{
    updateConfiguration(max_size_in_bytes, max_entries, max_entry_size_in_bytes_, max_entry_size_in_rows_);
}

void VectorQueryPlanCache::updateConfiguration(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_)
{
    std::lock_guard lock(mutex);
    cache->setMaxSizeInBytes(max_size_in_bytes);
    cache->setMaxCount(max_entries);
    max_entry_size_in_bytes = max_entry_size_in_bytes_;
    max_entry_size_in_rows = max_entry_size_in_rows_;
}

VectorQueryPlanCache::Reader VectorQueryPlanCache::createReader(const Key & key)
{
    std::lock_guard lock(mutex);
    return Reader(*cache, key, lock);
}

VectorQueryPlanCache::Writer VectorQueryPlanCache::createWriter(
    const Key & key,
    std::chrono::milliseconds min_plan_build_time,
    size_t max_query_plan_cache_size_in_bytes_quota,
    size_t max_query_cache_entries_quota)
{
    if (key.user_id.has_value())
        cache->setQuotaForUser(*key.user_id, max_query_plan_cache_size_in_bytes_quota, max_query_cache_entries_quota);

    std::lock_guard lock(mutex);
    return Writer(*this, *cache, key, max_entry_size_in_bytes, max_entry_size_in_rows, min_plan_build_time);
}

void VectorQueryPlanCache::clear(const std::optional<String> & tag)
{
    if (tag)
    {
        auto predicate = [tag](const Key & key, const Cache::MappedPtr &) { return key.tag == tag.value(); };
        cache->remove(predicate);
    }
    else
    {
        cache->clear();
    }

    std::lock_guard lock(mutex);
    table_to_keys.clear();
    if (tag)
        rebuildTableMappings();
}

void VectorQueryPlanCache::clearByTables(const std::vector<String> & table_names)
{
    if (table_names.empty())
        return;

    std::unordered_set<Key, KeyHasher> keys_to_remove;
    keys_to_remove.reserve(table_names.size());
    {
        std::lock_guard lock(mutex);
        // Reverse lookup lets us invalidate only plans that reference the changed
        // table instead of dropping the entire plan cache.
        for (const auto & table_name : table_names)
        {
            auto it = table_to_keys.find(table_name);
            if (it == table_to_keys.end())
                continue;

            keys_to_remove.insert(it->second.begin(), it->second.end());
        }
    }

    if (keys_to_remove.empty())
        return;

    auto predicate = [&keys_to_remove](const Key & key, const Cache::MappedPtr &)
    {
        return keys_to_remove.find(key) != keys_to_remove.end();
    };
    cache->remove(predicate);

    std::lock_guard lock(mutex);
    rebuildTableMappings();
}

void VectorQueryPlanCache::clearByDatabases(const std::vector<String> & database_names)
{
    if (database_names.empty())
        return;

    std::unordered_set<Key, KeyHasher> keys_to_remove;
    keys_to_remove.reserve(database_names.size());
    {
        std::lock_guard lock(mutex);
        for (const auto & database_name : database_names)
        {
            const String prefix = database_name + ".";

            // Iterate from lower_bound and break when prefix no longer matches.
            // This avoids using fragile upper_bound sentinel strings that may not work with UTF-8.
            for (auto it = table_to_keys.lower_bound(prefix); it != table_to_keys.end(); ++it)
            {
                const auto & [table_name, keys] = *it;
                if (table_name.rfind(prefix, 0) != 0)
                    break;
                keys_to_remove.insert(keys.begin(), keys.end());
            }
        }
    }

    if (keys_to_remove.empty())
        return;

    auto predicate = [&keys_to_remove](const Key & key, const Cache::MappedPtr &)
    {
        return keys_to_remove.find(key) != keys_to_remove.end();
    };
    cache->remove(predicate);

    std::lock_guard lock(mutex);
    rebuildTableMappings();
}

void VectorQueryPlanCache::invalidateByQuery(const ASTPtr & ast, ContextPtr context)
{
    if (!ast || !context)
        return;

    // Keep executeQuery.cpp thin: it only forwards the AST here, while VectorQueryPlanCache
    // owns the policy for deciding which cached SELECTs must be invalidated.
    std::vector<String> invalidated_tables;
    std::vector<String> invalidated_databases;
    collectMutationTargets(ast, context, invalidated_tables, invalidated_databases);

    if (!invalidated_tables.empty())
        clearByTables(invalidated_tables);
    if (!invalidated_databases.empty())
        clearByDatabases(invalidated_databases);
}

namespace
{
    void visitASTForTableNames(const ASTPtr & node, ContextPtr context, std::vector<String> & table_names, std::unordered_set<String> & seen)
    {
        if (!node)
            return;

        if (const auto * table_expression = node->as<ASTTableExpression>())
        {
            if (table_expression->database_and_table_name)
            {
                // Cache entries should remember every physical source table touched by
                // the SELECT tree, including tables reached through nested subqueries.
                try
                {
                    StorageID table_id = context->resolveStorageID(table_expression->database_and_table_name);
                    String table_name = getCanonicalTableName(table_id);
                    if (seen.emplace(table_name).second)
                        table_names.push_back(std::move(table_name));
                }
                catch (...)
                {
                    // Log the exception but continue with other tables.
                    // A table that fails to resolve will simply be missing from the invalidation set,
                    // potentially leading to stale cache entries that won't be invalidated.
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                }
            }
        }

        for (const auto & child : node->children)
            visitASTForTableNames(child, context, table_names, seen);
    }
}

std::vector<String> VectorQueryPlanCache::collectTableNames(const ASTPtr & ast, ContextPtr context)
{
    std::vector<String> table_names;
    std::unordered_set<String> seen;

    visitASTForTableNames(ast, context, table_names, seen);
    return table_names;
}

namespace
{
    void visitASTForSubquery(const ASTPtr & node, bool & has_subquery)
    {
        if (!node || has_subquery)
            return;

        if (node->as<ASTSubquery>())
        {
            has_subquery = true;
            return;
        }

        for (const auto & child : node->children)
            visitASTForSubquery(child, has_subquery);
    }
}

bool VectorQueryPlanCache::containsSubquery(const ASTPtr & ast)
{
    if (!ast)
        return false;

    // Cache restore currently rebinds constants by positional order. As soon as
    // the original query contains a real subquery node, planner optimizations may
    // legally reorder or duplicate literals across query boundaries. We therefore
    // use a conservative policy here: any ASTSubquery means "do not publish this
    // query shape into VectorQueryPlanCache".
    bool has_subquery = false;
    visitASTForSubquery(ast, has_subquery);
    return has_subquery;
}

void VectorQueryPlanCache::rebuildTableMappings()
{
    // The reverse index is derived state: rebuild it from the cache contents after
    // entry publication or removal so there is exactly one source of truth.
    table_to_keys.clear();
    for (const auto & key_mapped : cache->dump())
    {
        const auto & key = key_mapped.key;
        const auto & entry = key_mapped.mapped;
        if (!entry)
            continue;

        for (const auto & table_name : entry->table_names)
            table_to_keys[table_name].insert(key);
    }
}

}
