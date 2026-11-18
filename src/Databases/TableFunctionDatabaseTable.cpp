#include <Databases/TableFunctionDatabaseTable.h>

#include <Interpreters/Context.h>
#include <Storages/StorageProxy.h>
#include <TableFunctions/ITableFunction.h>

#include <mutex>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
}

namespace
{

/// Resolving such a table requires the READ source grant: the table is resolved by
/// inferring its structure from the data, and the catalog then exposes that structure to the user
/// (`DESCRIBE`, `EXPLAIN`, the analyzer) without necessarily reading the data. Writing requires the
/// WRITE source grant on top of that: the delegate storage itself may be writable
/// (`INSERT INTO web.`s3://...``) and the interpreter checks only the INSERT privilege on the
/// logical table name, so the proxy intercepts every write entry point (INSERT, async insert, a
/// materialized view target, `TRUNCATE`) and re-resolves the delegate in write mode.
///
/// The proxy also carries the logical storage ID (`db`.`name`): the table function creates the
/// nested storage under the internal `_table_function` database, and privilege checks against the
/// storage ID (e.g. the SELECT check in `InterpreterSelectQuery`) must see the name the user's
/// grants refer to.
class StorageTableFunctionDatabaseTable final : public StorageProxy
{
public:
    StorageTableFunctionDatabaseTable(
        const StorageID & storage_id_, ResolveTableFunctionDelegate resolve_, StoragePtr nested_, TableFunctionPtr table_function_)
        : StorageProxy(storage_id_)
        , resolve(std::move(resolve_))
        , nested(std::move(nested_))
        , table_function(std::move(table_function_))
    {
        const auto nested_metadata = nested->getInMemoryMetadataPtr(nullptr, false);
        setInMemoryMetadata(*nested_metadata);
    }

    StoragePtr getNested() const override { return nested; }
    StoragePtr tryGetNested() const override { return nested; }
    String getName() const override { return nested->getName(); }

    /// Engine classification is used by policy checks (e.g. the `disable_insertion_and_mutation`
    /// guard in `InterpreterInsertQuery`, which exempts external engines), so report the class of
    /// the delegate instead of the `IStorage` default of a local engine. Deliberately not done in
    /// `StorageProxy`: the lazy proxies (`StorageTableProxy`, `StorageTableFunctionProxy`) would
    /// have to materialize and start up the nested storage just to answer a classification query.
    bool isDataLake() const override { return nested->isDataLake(); }
    bool isExternalDatabase() const override { return nested->isExternalDatabase(); }
    bool isObjectStorage() const override { return nested->isObjectStorage(); }
    bool isMessageQueue() const override { return nested->isMessageQueue(); }

    /// Forward the delegate's planner-visible capability contracts. `StorageProxy` forwards only a
    /// part of them (e.g. `supportsPrewhere`, `supportsSubcolumns`), so without these overrides the
    /// wrapper would silently fall back to the `IStorage` defaults and such a table
    /// would behave differently from the same data read directly through `file`/`s3` or through
    /// `ENGINE = URL(...)` (whose wrapper, `StorageURLSchemeDispatch`, carries the same overrides):
    ///
    /// * `supportedPrewhereColumns` defaults to `std::nullopt` (unrestricted) and
    ///   `canMoveConditionsToPrewhere` to `supportsPrewhere`, while the delegates keep `PREWHERE`
    ///   away from columns not materialized at `PREWHERE` time (default expressions, hive-partition
    ///   columns);
    /// * `supportsTrivialCountOptimization` defaults to `false`, while the delegates return `true`,
    ///   so `SELECT count()` would parse the external data instead of using the optimized count path.
    std::optional<NameSet> supportedPrewhereColumns() const override { return nested->supportedPrewhereColumns(); }
    bool canMoveConditionsToPrewhere() const override { return nested->canMoveConditionsToPrewhere(); }
    bool supportsTrivialCountOptimization(const StorageSnapshotPtr & storage_snapshot, ContextPtr query_context) const override
    {
        return nested->supportsTrivialCountOptimization(storage_snapshot, query_context);
    }

    /// Read through a snapshot of the nested storage (like `StorageTableFunctionProxy` does):
    /// the storage may downcast `storage_snapshot->storage` to its own type.
    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr &,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override
    {
        const auto nested_metadata = nested->getInMemoryMetadataPtr(context, false);
        auto nested_snapshot = nested->getStorageSnapshot(nested_metadata, context);
        nested->read(query_plan, column_names, nested_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
    }

    /// Checked on the initiator, before the query is executed or queued for asynchronous insertion:
    /// with `async_insert = 1` the sink below is created in a background flush, after the query has
    /// already reported success to the user (`wait_for_async_insert = 0`) and possibly with
    /// different privileges than the ones the user had when the query was issued.
    void checkInsertIsAllowed(ContextPtr context) const override
    {
        table_function->checkSourceAccess(context, /* is_insert_query */ true);
    }

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override
    {
        return getNestedForWrite(context)->write(query, metadata_snapshot, context, async_insert);
    }

    void truncate(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        TableExclusiveLockHolder & lock) override
    {
        getNestedForWrite(context)->truncate(query, metadata_snapshot, context, lock);
    }

    void shutdown(bool is_drop) override
    {
        nested->shutdown(is_drop);

        std::lock_guard lock(nested_for_write_mutex);
        if (nested_for_write)
            nested_for_write->shutdown(is_drop);
    }

private:
    /// The delegate of a write is a separate storage: the backends of the `url` table function
    /// construct different clients for reads and writes (`azureBlobStorage` creates the container
    /// only in write mode), and the resolved one is a read-mode storage. It is created on the first
    /// write and reused, because a single `INSERT` may write through several sinks.
    /// The write source grant is checked as part of the resolution (see `ResolveTableFunctionDelegate`).
    StoragePtr getNestedForWrite(ContextPtr context) const
    {
        std::lock_guard lock(nested_for_write_mutex);
        if (!nested_for_write)
            nested_for_write = resolve(context, /* is_insert_query */ true).storage;

        if (!nested_for_write)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Cannot write to the table {}: it is not writable",
                            getStorageID().getNameForLogs());

        return nested_for_write;
    }

    const ResolveTableFunctionDelegate resolve;
    StoragePtr nested;
    TableFunctionPtr table_function;

    mutable std::mutex nested_for_write_mutex;
    mutable StoragePtr nested_for_write;
};

}

StoragePtr makeTableFunctionDatabaseTable(const StorageID & storage_id, const ResolveTableFunctionDelegate & resolve, const ContextPtr & context)
{
    auto delegate = resolve(context, /* is_insert_query */ false);
    if (!delegate.storage)
        return nullptr;

    return std::make_shared<StorageTableFunctionDatabaseTable>(
        storage_id, resolve, std::move(delegate.storage), std::move(delegate.table_function));
}

}
