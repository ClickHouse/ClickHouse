#include <Storages/StorageNull.h>
#include <Storages/StorageFactory.h>
#include <Storages/AlterCommands.h>

#include <Common/quoteString.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>

#include <IO/WriteHelpers.h>

#include <Core/Streaming/Settings.h>
#include <Interpreters/castColumn.h>
#include <Processors/ISource.h>
#include <Storages/SelectQueryInfo.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// A subscriber that falls behind the inserts by more blocks than that is failed, see `NullTableSubscription`.
constexpr size_t MAX_QUEUED_BLOCKS_OF_STREAMING_QUERY = 1024;

/// Pushes the inserted blocks to the streaming queries.
class NullStreamingSink final : public SinkToStorage
{
public:
    NullStreamingSink(SharedHeader header_, std::shared_ptr<NullTableSubscribers> subscribers_)
        : SinkToStorage(header_), subscribers(std::move(subscribers_))
    {
    }

    std::string getName() const override { return "NullSinkToStorage"; }

    void consume(Chunk & chunk) override
    {
        if (!subscribers->hasSubscribers() || chunk.getNumRows() == 0)
            return;
        subscribers->push(getHeader().cloneWithColumns(chunk.getColumns()));
    }

private:
    const std::shared_ptr<NullTableSubscribers> subscribers;
};

#if defined(OS_LINUX) || defined(OS_DARWIN)

/// Returns the blocks inserted into the table while the streaming query runs. Waits for them asynchronously,
/// without holding a thread of the pipeline.
class NullStreamingSource final : public ISource
{
public:
    NullStreamingSource(SharedHeader header_, NullTableSubscriptionPtr subscription_)
        : ISource(header_), subscription(std::move(subscription_))
    {
    }

    String getName() const override { return "NullStreamingSource"; }

    Status prepare() override
    {
        if (isCancelled())
        {
            getPort().finish();
            return Status::Finished;
        }

        /// The output may be closed while the source waits for a block, e.g. when an outer `LIMIT` is satisfied by
        /// another branch of the pipeline.
        if (getPort().isFinished())
            return Status::Finished;

        if (is_async_state)
            return Status::Async;

        return ISource::prepare();
    }

    int schedule() override { return subscription->fd(); }

    /// Wake up the source waiting in the async poller when its output is closed, so that it finishes.
    void onUpdatePorts() override
    {
        if (getPort().isFinished())
            subscription->wakeUp();
    }

    void onCancel() noexcept override
    {
        try
        {
            subscription->wakeUp();
        }
        catch (...)
        {
            tryLogCurrentException("NullStreamingSource");
        }
    }

protected:
    std::optional<Chunk> tryGenerate() override
    {
        if (isCancelled())
            return {};

        Block block;
        if (!subscription->tryPop(block))
        {
            is_async_state = true;
            return Chunk();
        }
        is_async_state = false;

        const auto & header = getPort().getHeader();
        Columns columns;
        columns.reserve(header.columns());
        for (const auto & column : header)
        {
            /// The structure of the table may have changed since the start of the query.
            auto inserted = block.getColumnOrSubcolumnByName(column.name);
            columns.push_back(castColumn(inserted, column.type)->convertToFullColumnIfConst());
        }
        return Chunk(std::move(columns), block.rows());
    }

private:
    const NullTableSubscriptionPtr subscription;
    bool is_async_state = false;
};

#endif

}


void registerStorageNull(StorageFactory & factory);
void registerStorageNull(StorageFactory & factory)
{
    factory.registerStorage("Null", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Engine {} doesn't support any arguments ({} given)",
                args.engine_name, args.engine_args.size());

        return std::make_shared<StorageNull>(args.table_id, args.columns, args.constraints, args.comment);
    },
    SecretArgumentsSpec{},
    {
        .supports_parallel_insert = true,
        .supports_temporary = true,
    },
    Documentation{
        .description = R"DOCS_MD(
When writing data to a `Null` table, data is ignored.
When reading from a `Null` table, the response is empty.

The `Null` table engine is useful for data transformations where you no longer need the original data after it has been transformed.
For this purpose you can create a materialized view on a `Null` table.
The data written to the table will be consumed by the view, but the original raw data will be discarded.

## Streaming queries {#streaming-queries}

A streaming query (`SELECT ... FROM null_table STREAM`, an experimental feature enabled by `enable_streaming_queries`) receives every block inserted into the table while it runs. Any number of queries can insert into the table and subscribe to it at the same time, so the table works as an ephemeral queue with many producers and many consumers. The blocks inserted before the start of a subscription, or while there are no subscribers, are lost.

```sql
SELECT * FROM null_table STREAM WHERE level = 'Error'
```

The table does not store anything, so `CURSOR` is not supported, and `STREAM BOUNDED` returns nothing. A subscriber does not slow down the inserts: if it falls behind by more than 1024 blocks, its query fails with `TOO_SLOW`. `WATERMARK` is not supported.
)DOCS_MD",
        .syntax = "ENGINE = Null"});
}

Pipe StorageNull::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processing_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    auto header = std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(column_names));

    if (!query_info.isStream())
        return Pipe(std::make_shared<NullSource>(header));

    const auto & stream_settings = *query_info.table_expression_modifiers->getStreamSettings();
    if (stream_settings.cursor)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "A Null table does not store anything, so a streaming query from it cannot start at a CURSOR");
    if (stream_settings.watermark)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "WATERMARK is not supported for STREAM from a table with engine Null");

    /// A bounded stream reads what the table has now: nothing.
    if (!stream_settings.subscribe_for_updates)
        return Pipe(std::make_shared<NullSource>(header));

#if defined(OS_LINUX) || defined(OS_DARWIN)
    return Pipe(std::make_shared<NullStreamingSource>(header, subscribers->subscribe(MAX_QUEUED_BLOCKS_OF_STREAMING_QUERY)));
#else
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Streaming queries from a Null table are not supported on this platform");
#endif
}

SinkToStoragePtr StorageNull::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr, bool)
{
    return std::make_shared<NullStreamingSink>(std::make_shared<const Block>(metadata_snapshot->getSampleBlock()), subscribers);
}

void StorageNull::checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const
{
    std::optional<NameDependencies> name_deps{};
    for (const auto & command : commands)
    {
        if (command.type != AlterCommand::Type::ADD_COLUMN
            && command.type != AlterCommand::Type::MODIFY_COLUMN
            && command.type != AlterCommand::Type::DROP_COLUMN
            && command.type != AlterCommand::Type::COMMENT_COLUMN
            && command.type != AlterCommand::Type::COMMENT_TABLE)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}",
                command.type, getName());

        if (command.type == AlterCommand::DROP_COLUMN && !command.clear)
        {
            if (!name_deps)
                name_deps = getDependentViewsByColumn(context);
            const auto & deps_mv = name_deps.value()[command.column_name];
            if (!deps_mv.empty())
            {
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Trying to ALTER DROP column {} which is referenced by materialized view {}",
                    backQuoteIfNeed(command.column_name), toString(deps_mv)
                    );
            }
        }
    }
}


void StorageNull::alter(const AlterCommands & params, ContextPtr context, AlterLockHolder &, DDLGuardPtr &)
{
    auto table_id = getStorageID();

    auto metadata_snapshot = getInMemoryMetadataPtr(context, false);
    StorageInMemoryMetadata new_metadata = *metadata_snapshot;
    params.apply(new_metadata, context);
    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(context, table_id, new_metadata, /*validate_new_create_query=*/true);
    setInMemoryMetadata(new_metadata);
}

}
