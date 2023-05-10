#include <Storages/StorageNull.h>
#include <Storages/StorageFactory.h>
#include <Storages/AlterCommands.h>
#include <Storages/SelectQueryInfo.h>
#include <Parsers/ASTInsertQuery.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/Context.h>
#include <Databases/IDatabase.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/NullStreamSource.h>
#include <IO/WriteHelpers.h>

#include <DataTypes/DataTypesNumber.h>


namespace DB
{

class NullStreamSink : public SinkToStorage
{
public:
    NullStreamSink(StorageNull & storage_, const StorageMetadataPtr & metadata_snapshot_, ContextPtr context_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock())
        , storage(storage_)
        , storage_snapshot(storage_.getStorageSnapshot(metadata_snapshot_, context_))
    {}
    
    using SinkToStorage::SinkToStorage;

    std::string getName() const override { return "NullStreamSink"; }

    void consume(Chunk chunk) override
    {
        auto block = getHeader().cloneWithColumns(chunk.getColumns());
        storage_snapshot->metadata->check(block, true);

        new_blocks.emplace_back(block);
    }

    void onFinish() override
    {
        std::lock_guard lock(storage.mutex);

        for (auto it = storage.subscribers->begin(); it != storage.subscribers->end(); ++it) {
            (*storage.subscribers)[it->first] = std::make_shared<Blocks>();
            (*storage.subscribers)[it->first]->insert((*storage.subscribers)[it->first]->end(), new_blocks.begin(), new_blocks.end());
        }
        storage.condition.notify_all();
    }
private:
    Blocks new_blocks;
    StorageNull & storage;
    StorageSnapshotPtr storage_snapshot;
};

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
}

void StorageNull::shutdown()
{
    shutdown_called = true;
}


StorageNull::~StorageNull()
{
    shutdown();
}

void StorageNull::drop()
{
    std::lock_guard lock(mutex);
    condition.notify_all();
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
    Block block = storage_snapshot->getSampleBlockForColumns(column_names);
    UInt64 subs_cnt = getNextSubscriberId();
    auto & select = query_info.query->as<ASTSelectQuery &>();

    if (select.is_stream) {
        Blocks new_blocks;
        new_blocks.emplace_back(block);
        (*subscribers)[subs_cnt] = std::make_shared<Blocks>();
        (*subscribers)[subs_cnt]->insert((*subscribers)[subs_cnt]->end(), new_blocks.begin(), new_blocks.end());
        condition.notify_all();
        return Pipe(
                std::make_shared<NullStreamSource>(
                block,
                std::static_pointer_cast<StorageNull>(shared_from_this()), subs_cnt));
    }
    return Pipe(
            std::make_shared<NullSource>(block));
}

SinkToStoragePtr StorageNull::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    return std::make_shared<NullStreamSink>(*this, metadata_snapshot, context);
}

void registerStorageNull(StorageFactory & factory)
{
    factory.registerStorage("Null", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Engine {} doesn't support any arguments ({} given)",
                args.engine_name, args.engine_args.size());

        return std::make_shared<StorageNull>(args.table_id, args.columns, args.constraints, args.comment);
    },
    {
        .supports_parallel_insert = true,
    });
}

void StorageNull::checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const
{
    auto name_deps = getDependentViewsByColumn(context);
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
            const auto & deps_mv = name_deps[command.column_name];
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


void StorageNull::alter(const AlterCommands & params, ContextPtr context, AlterLockHolder &)
{
    auto table_id = getStorageID();

    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    params.apply(new_metadata, context);
    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(context, table_id, new_metadata);
    setInMemoryMetadata(new_metadata);
}

}
