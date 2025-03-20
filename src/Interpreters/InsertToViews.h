#pragma once

#include <Interpreters/StorageID.h>
#include <Interpreters/QueryViewsLog.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/IStorage.h>

#include <QueryPipeline/Chain.h>
#include "Common/ThreadStatus.h"
#include <Common/Logger.h>
#include "Interpreters/ExpressionAnalyzer.h"

#include <exception>
#include <map>
#include <memory>
#include <vector>

namespace DB
{

class ViewsManager : public std::enable_shared_from_this<ViewsManager>
{
private:
    friend class FinalizingViewsTransform;

    struct StorageIDPrivate : public StorageID
    {
        using StorageID::StorageID;

        StorageIDPrivate()
            : StorageIDPrivate(StorageID::createEmpty())
        {}

        StorageIDPrivate(const StorageID & other) // NOLINT this is an implicit c-tor
            : StorageID(other)
        {}

        bool operator < (const StorageID & other) const
        {
            if (hasUUID() && other.hasUUID())
                return uuid < other.uuid;
            return std::tuple(database_name, table_name) < std::tuple(other.database_name, other.table_name);
        }

        bool operator == (const StorageID & other) const
        {
            if (empty() && other.empty())
                return true;
            if (empty())
                return false;
            if (other.empty())
                return false;
            return StorageID::operator==(other);
        }
    };

    struct BundleID
    {
        StorageIDPrivate view_id;
        StorageIDPrivate inner_id;
    };

    class VisitedPath
    {
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

    using MapIdManyId = std::map<StorageIDPrivate, std::vector<StorageIDPrivate>>;
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
    using ConstPtr = std::shared_ptr<const ViewsManager>;

    template <class... Args>
    static ConstPtr create(Args &&... args)
    {
        struct MakeSharedEnabler : public ViewsManager
        {
            explicit MakeSharedEnabler(Args &&... args) : ViewsManager(std::forward<Args>(args)...) {}
        };
        return std::make_shared<const MakeSharedEnabler>(std::forward<Args>(args)...);
    }

    Chain createPreSink() const;
    Chain createSink() const;
    Chain createPostSink() const;
    Chain createRetry(VisitedPath path);

    void logQueryView(StorageID view_id, std::exception_ptr exception) const;

protected:
    ViewsManager(StoragePtr table, ASTPtr query, Block insert_header, bool async_insert_, bool skip_destination_table_, bool allow_materialized_, ContextPtr context);

private:
    void buildRelaitions();
    Chain createSelect(StorageIDPrivate view_id) const;
    Chain createPreSink(StorageIDPrivate view_id) const;
    Chain createSink(StorageIDPrivate view_id) const;
    Chain createPostSink(StorageIDPrivate view_id, size_t level) const;

    StorageID init_table_id;
    StoragePtr init_storage;
    ASTPtr init_query;
    Block init_header;
    ContextPtr init_context;
    bool async_insert;
    bool skip_destination_table;
    bool allow_materialized = false;

    BundleID root;

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

    LoggerPtr logger;

public:
    bool deduplicate_blocks_in_dependent_materialized_views = false;
    bool insert_null_as_default = false;
    bool materialized_views_ignore_errors = false;
};


}
