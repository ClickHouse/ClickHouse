#pragma once

#include <Interpreters/StorageID.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/IStorage.h>
#include <QueryPipeline/Chain.h>
#include "QueryPipeline/QueryPlanResourceHolder.h"

#include <map>
#include <memory>

namespace DB
{

class ViewsManager : public std::enable_shared_from_this<ViewsManager>
{
public:
    using Ptr = std::shared_ptr<ViewsManager>;

    template <class... Args>
    static Ptr create(Args &&... args)
    {
        struct MakeSharedEnabler : public ViewsManager
        {
            explicit MakeSharedEnabler(Args &&... args) : ViewsManager(std::forward<Args>(args)...) {}
        };
        return std::make_shared<MakeSharedEnabler>(std::forward<Args>(args)...);
    }
    static Ptr create(StorageID table_id, ASTPtr query, Block insert_header, ContextPtr context);

    Chain createPreSink(StorageID t_id);
    Chain createSink(StorageID t_id);
    Chain createPostSink(StorageID t_id, size_t level = 0);
    Chain createRetry(StorageID t_id);

protected:
    ViewsManager(StoragePtr table, ASTPtr query, Block insert_header, ContextPtr context);

private:
    void buildRelaitions();
    Chain createSelect(StorageID t_id);

    struct StorageIDPrivate : public StorageID
    {
        using StorageID::StorageID;

        StorageIDPrivate()
            : StorageIDPrivate("EMPTY", "EMPTY")
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
    };

    using MapIdManyId = std::map<StorageIDPrivate, std::vector<StorageIDPrivate>>;
    using MapIdId = std::map<StorageIDPrivate, StorageIDPrivate>;
    using MapIdStorage = std::map<StorageIDPrivate, StoragePtr>;
    using MapIdMetadata = std::map<StorageIDPrivate, StorageMetadataPtr>;

    using MapIdAST = std::map<StorageIDPrivate, ASTPtr>;
    using MapIdLock = std::map<StorageIDPrivate, TableLockHolder>;
    using MapIdContext = std::map<StorageIDPrivate, ContextPtr>;
    using MapIdBlock = std::map<StorageIDPrivate, Block>;

    StorageID init_id;
    StoragePtr init_storage;
    ASTPtr init_query;
    Block init_header;
    ContextPtr init_context;

    MapIdManyId children;
    MapIdId parents;
    MapIdId inner_storages;
    MapIdStorage storages;
    MapIdLock storage_locks;
    MapIdMetadata metadata_snapshots;
    MapIdAST select_queries;
    MapIdContext insert_contexts;
    MapIdContext select_contexts;
    MapIdBlock insert_headers;
    MapIdBlock select_headers;

    bool deduplicate_blocks_in_dependent_materialized_views = false;
    bool insert_null_as_default = false;

    LoggerPtr logger;
};


}
