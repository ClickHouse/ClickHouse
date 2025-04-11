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

    friend class ViewErrorsRegistry;

    class DependencyPath
    {
    private:
        std::vector<StorageIDPrivate> path;
        std::set<StorageIDPrivate> visited;

        static StorageIDPrivate empty_id;

    public:
        void pushBack(StorageIDPrivate id);
        void popBack();

        [[maybe_unused]] bool empty() const { return path.empty(); }
        const StorageIDPrivate & back() const { return path.back(); }
        const StorageIDPrivate & current() const { return back(); }
        const StorageIDPrivate & parent() const { if (path.size() > 1) return *++path.rbegin(); return empty_id; }
        const StorageIDPrivate & prevParent() const { if (path.size() > 2) return *++++path.rbegin(); return empty_id; }
        const StorageIDPrivate & prevPrevParent() const { if (path.size() > 3) return *++++++path.rbegin(); return empty_id; }
        String debugString() const;
    };

    using MapIdManyId = std::map<StorageIDPrivate, std::vector<StorageID>>;
    using MapIdId = std::map<StorageIDPrivate, StorageIDPrivate>;
    using MapIdStorage = std::map<StorageIDPrivate, StoragePtr>;
    using MapIdMetadata = std::map<StorageIDPrivate, StorageMetadataPtr>;

    using MapIdAST = std::map<StorageIDPrivate, ASTPtr>;
    using MapIdLock = std::map<StorageIDPrivate, TableLockHolder>;
    using MapIdContext = std::map<StorageIDPrivate, ContextPtr>;
    using MapIdBlock = std::map<StorageIDPrivate, Block>;
    using MapIdThreadGroup = std::map<StorageIDPrivate, ThreadGroupPtr>;
    using MapIdViewType = std::map<StorageIDPrivate, QueryViewsLogElement::ViewType>;

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
    InsertDependenciesBuilder(StoragePtr table, ASTPtr query, Block insert_header, bool async_insert_, bool skip_destination_table_, bool allow_materialized_, ContextPtr context);

private:
    std::pair<ContextPtr, ContextPtr> createSelectInsertContext(const DependencyPath & path);
    bool collectPath(const DependencyPath & path);
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
    bool allow_materialized = false;

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
