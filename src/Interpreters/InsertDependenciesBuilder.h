#pragma once

#include <Interpreters/StorageID.h>
#include <Interpreters/QueryViewsLog.h>
#include <Storages/StorageSnapshot.h>

#include <QueryPipeline/Chain.h>
#include <Common/Logger.h>

#include <exception>
#include <map>
#include <memory>
#include <vector>

namespace DB
{

class ThreadGroup;
using ThreadGroupPtr = std::shared_ptr<ThreadGroup>;

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class ViewErrorsRegistry;
using ViewErrorsRegistryPtr = std::shared_ptr<ViewErrorsRegistry>;

class InsertDependenciesBuilder : public std::enable_shared_from_this<InsertDependenciesBuilder>
{
private:
    struct StorageIDPrivate : public StorageID
    {
        // This class just releases some restriction from StorageID
        // StorageIDPrivate has default c-tor implemented as StorageID::createEmpty()
        using StorageID::StorageID;

        StorageIDPrivate();
        StorageIDPrivate(const StorageID & other); // NOLINT this is an implicit c-tor

        bool operator<(const StorageIDPrivate & other) const;
        bool operator==(const StorageIDPrivate & other) const;
    };

    /// We cannot use std::set, because operator< is inconsistent with operator==
    /// for StorageId and StorageIDPrivate.
    /// Take a look at the detailed comment in StorageID::operator==.
    using StorageIDPrivateSet = std::unordered_set<StorageIDPrivate, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;

    friend class ViewErrorsRegistry;

    class DependencyPath
    {
    private:
        std::vector<StorageIDPrivate> path;
        StorageIDPrivateSet visited;

    public:
        void pushBack(StorageIDPrivate id);
        void popBack();

        [[maybe_unused]] bool empty() const { return path.empty(); }
        const StorageIDPrivate & back() const { return path.back(); }
        const StorageIDPrivate & current() const { return back(); }
        StorageIDPrivate parent(size_t inheritance) const;
        String debugString() const;
    };

    using MapIdManyId = std::unordered_map<StorageIDPrivate, std::vector<StorageID>, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
    using MapIdId = std::unordered_map<StorageIDPrivate, StorageIDPrivate, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
    using MapIdStorage = std::unordered_map<StorageIDPrivate, StoragePtr, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
    using MapIdMetadata = std::unordered_map<StorageIDPrivate, StorageMetadataPtr, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;

    using MapIdAST = std::unordered_map<StorageIDPrivate, ASTPtr, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
    using MapIdLock = std::unordered_map<StorageIDPrivate, TableLockHolder, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
    using MapIdContext = std::unordered_map<StorageIDPrivate, ContextPtr, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
    using MapIdBlock = std::unordered_map<StorageIDPrivate, Block, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
    using MapIdThreadGroup = std::unordered_map<StorageIDPrivate, ThreadGroupPtr, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;
    using MapIdViewType = std::unordered_map<StorageIDPrivate, QueryViewsLogElement::ViewType, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>;

public:
    using ConstPtr = std::shared_ptr<const InsertDependenciesBuilder>;

    template <class... Args>
    static ConstPtr create(Args &&... args)
    {
        struct MakeSharedEnabler : public InsertDependenciesBuilder
        {
            explicit MakeSharedEnabler(Args &&... args) : InsertDependenciesBuilder(std::forward<Args>(args)...) {}
        };
        return std::make_shared<const MakeSharedEnabler>(std::forward<Args>(args)...);
    }

    Chain createChainWithDependencies() const;
    bool isViewsInvolved() const;

    void logQueryView(StorageID view_id, std::exception_ptr exception, bool before_start = false) const;

protected:
    InsertDependenciesBuilder(StoragePtr table, ASTPtr query, Block insert_header, bool async_insert_, bool skip_destination_table_, ContextPtr context);

private:
    bool isView(StorageIDPrivate id) const;

    std::pair<ContextPtr, ContextPtr> createSelectInsertContext(const DependencyPath & path);
    bool observePath(const DependencyPath & path);
    void collectAllDependencies();

    Chain createPreSink(StorageIDPrivate view_id) const;
    Chain createSelect(StorageIDPrivate view_id) const;
    Chain createSink(StorageIDPrivate view_id) const;
    Chain createPostSink(StorageIDPrivate view_id) const;

    static QueryViewsLogElement::ViewStatus getQueryViewStatus(std::exception_ptr exception, bool before_start);

    StorageIDPrivate init_table_id;
    StoragePtr init_storage;
    ASTPtr init_query;
    Block init_header;
    ContextPtr init_context;

    bool async_insert = false;
    bool skip_destination_table = false;

    StorageIDPrivate root_view;

    MapIdManyId dependent_views;
    MapIdId inner_tables;
    MapIdId source_tables;
    MapIdStorage storages;
    MapIdViewType view_types;
    MapIdLock storage_locks;
    MapIdMetadata metadata_snapshots;
    MapIdAST select_queries;
    MapIdContext insert_contexts;
    MapIdContext select_contexts;
    MapIdBlock input_headers;
    MapIdBlock output_headers;
    MapIdThreadGroup thread_groups;

    ViewErrorsRegistryPtr views_error_registry;

    LoggerPtr logger;

public:
    // expose settings value into public
    bool deduplicate_blocks_in_dependent_materialized_views = false;
    bool insert_null_as_default = false;
    bool materialized_views_ignore_errors = false;
    bool ignore_materialized_views_with_dropped_target_table = false;
};

}
