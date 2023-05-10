#include <Storages/StorageNull.h>
#include <Storages/StorageFactory.h>
#include <Storages/AlterCommands.h>
#include <Storages/SelectQueryInfo.h>
#include <Parsers/ASTInsertQuery.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/Context.h>
#include <Databases/IDatabase.h>
#include <Processors/Sources/NullSource.h>
#include <IO/WriteHelpers.h>

#include <DataTypes/DataTypesNumber.h>

namespace DB
{

class NullStreamSink : public SinkToStorage
{
public:
    NullStreamSink (StorageNull & storage_, const StorageMetadataPtr & metadata_snapshot_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock()), 
        storage(storage_) {}
    
    using SinkToStorage::SinkToStorage;

    std::string getName() const override { return "NullStreamSink"; }

    void consume(Chunk chunk) override {
        std::lock_guard lock(storage.mutex);

        auto block = getHeader().cloneWithColumns(chunk.getColumns());
        BlocksPtr new_blocks = std::make_shared<Blocks>();
        new_blocks->push_back(block);
        std::shared_ptr<BlocksPtr>  blocks_ptr = std::make_shared<BlocksPtr>();
        *(blocks_ptr) = new_blocks;
        for (auto it = storage.subscribers->begin(); it != storage.subscribers->end(); ++it) {
            (*storage.subscribers)[it->first] = blocks_ptr;
        }
        storage.condition.notify_all();
    }
private:
    StorageNull & storage;
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
    int client_id_ = client_id.fetch_add(1, std::memory_order_relaxed);
    SelectQueryInfo modified_query_info = query_info;
    modified_query_info.query = query_info.query->clone();
    auto & modified_select = modified_query_info.query->as<ASTSelectQuery &>();

    if (modified_select.is_stream) {
        std::shared_ptr<BlocksPtr>  blocks_ptr = std::make_shared<BlocksPtr>();
        if (!(*blocks_ptr)) {
            BlocksPtr new_blocks = std::make_shared<Blocks>();
            new_blocks->push_back(block);
            (*blocks_ptr) = new_blocks;
            subscribers->insert(std::pair{client_id_, blocks_ptr});
            condition.notify_all();
        }
        return Pipe(
                std::make_shared<NullSource>(
                block,
                std::static_pointer_cast<StorageNull>(shared_from_this()), client_id_));
    }
    return Pipe(
            std::make_shared<NullSource>(block));
}

SinkToStoragePtr StorageNull::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr /*ctx*/)
{
    return std::make_shared<NullStreamSink>(*this, metadata_snapshot);
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
