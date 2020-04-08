#pragma once

#include <Common/NamePrompter.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/registerStorages.h>
#include <Access/AccessType.h>
#include <unordered_map>


namespace DB
{

class Context;
class ASTCreateQuery;
class ASTStorage;
struct StorageID;


/** Allows to create a table by the name and parameters of the engine.
  * In 'columns' Nested data structures must be flattened.
  * You should subsequently call IStorage::startup method to work with table.
  */
class StorageFactory : private boost::noncopyable, public IHints<1, StorageFactory>
{
public:

    static StorageFactory & instance();

    struct Arguments
    {
        const String & engine_name;
        ASTs & engine_args;
        ASTStorage * storage_def;
        const ASTCreateQuery & query;
        /// Path to table data.
        /// Relative to <path> from server config (possibly <path> of some <disk> of some <volume> for *MergeTree)
        const String & relative_data_path;
        const StorageID & table_id;
        Context & local_context;
        Context & context;
        const ColumnsDescription & columns;
        const ConstraintsDescription & constraints;
        bool attach;
        bool has_force_restore_data_flag;
    };

    struct StorageFeatures
    {
        bool supports_settings = false;
        bool supports_skipping_indices = false;
        bool supports_sort_order = false;
        bool supports_ttl = false;
        bool supports_replication = false;
        bool supports_deduplication = false;
        AccessType source_access_type = AccessType::NONE;
    };

    using CreatorFn = std::function<StoragePtr(const Arguments & arguments)>;
    struct Creator
    {
        CreatorFn creator_fn;
        StorageFeatures features;
    };

    using Storages = std::unordered_map<std::string, Creator>;

    StoragePtr get(
        const ASTCreateQuery & query,
        const String & relative_data_path,
        Context & local_context,
        Context & context,
        const ColumnsDescription & columns,
        const ConstraintsDescription & constraints,
        bool has_force_restore_data_flag) const;

    /// Register a table engine by its name.
    /// No locking, you must register all engines before usage of get.
    void registerStorage(const std::string & name, CreatorFn creator_fn, StorageFeatures features = StorageFeatures{
        .supports_settings = false,
        .supports_skipping_indices = false,
        .supports_sort_order = false,
        .supports_ttl = false,
        .supports_replication = false,
        .supports_deduplication = false,
        .source_access_type = AccessType::NONE,
    });

    const Storages & getAllStorages() const
    {
        return storages;
    }

    std::vector<String> getAllRegisteredNames() const override
    {
        std::vector<String> result;
        auto getter = [](const auto & pair) { return pair.first; };
        std::transform(storages.begin(), storages.end(), std::back_inserter(result), getter);
        return result;
    }

    using FeatureMatcherFn = std::function<bool(StorageFeatures)>;
    std::vector<String> getAllRegisteredNamesByFeatureMatcherFn(FeatureMatcherFn feature_matcher_fn) const
    {
        std::vector<String> result;
        for (const auto& pair : storages)
            if (feature_matcher_fn(pair.second.features))
                result.push_back(pair.first);
        return result;
    }

    AccessType getSourceAccessType(const String & table_engine) const;

private:
    Storages storages;
};

}
