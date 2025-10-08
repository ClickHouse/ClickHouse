#include <Storages/StorageAlias.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Parsers/ASTCreateQuery.h>
#include <QueryPipeline/Pipe.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Common/CurrentThread.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
}

StorageAlias::StorageAlias(const StorageID & table_id_, const StorageID & ref_table_id_, ContextPtr context_)
: IStorage(table_id_), WithMutableContext(context_->getGlobalContext()), ref_table_id(ref_table_id_)
{
    if (table_id_ == ref_table_id_)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Alias table cannot refer to itself.");
}

void registerStorageAlias(StorageFactory & factory)                                                                                                                                           
{                                                                                                                                                                                             
    factory.registerStorage("Alias", [](const StorageFactory::Arguments & args) -> StoragePtr
    {
        if (!args.columns.empty() && args.mode == LoadingStrictnessLevel::CREATE)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "No need to define the schema of Alias table");

        String current_db = args.getLocalContext()->getCurrentDatabase();
        ASTs & engine_args = args.engine_args;
        if (engine_args.size() > 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Storage Alias requires 0 or 1 arguments - Create Table t as t1 Engine = Alias or Engine = Alias(database.table)");

        const ContextPtr & local_context = args.getLocalContext();

        if (engine_args.empty())
        {
            if (args.query.as_table.empty())
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "We need to write as_table if Alias arguments are empty");
            String table_name = args.query.as_table;
            String db_name = args.query.as_database;
            if (db_name.empty())
                db_name = current_db;
            return std::make_shared<StorageAlias>(args.table_id, StorageID(db_name, table_name), args.getContext());
        }
        if (engine_args.size() == 1)
        {
            engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], local_context);
            auto arg = checkAndGetLiteralArgument<String>(engine_args[0], "database_with_table");
            auto qualified_name = QualifiedTableName::parseFromString(arg);
            /// The definition is Alias(database.table)
            if (!qualified_name.database.empty())
                return std::make_shared<StorageAlias>(args.table_id, StorageID(qualified_name), args.getContext());
            return std::make_shared<StorageAlias>(args.table_id, StorageID(current_db, qualified_name.table), args.getContext());
        }
        return nullptr;
    },
    {                                                                                                                                                                                         
        .supports_schema_inference = true,                                                                                                                                                    
    });                                                                                                                                                                                       
}

StoragePtr StorageAlias::getReferenceTable() const
{
    return DatabaseCatalog::instance().getTable(ref_table_id, getContext());
}

void StorageAlias::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    getReferenceTable()->read(query_plan, column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
}

SinkToStoragePtr StorageAlias::write(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context_,
    bool async_insert)
{
    return getReferenceTable()->write(query, metadata_snapshot, context_, async_insert);
}

std::optional<QueryPipeline> StorageAlias::distributedWrite(const ASTInsertQuery & query, ContextPtr context_)
{
    return getReferenceTable()->distributedWrite(query, context_);
}

void StorageAlias::truncate(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context_,
    TableExclusiveLockHolder & lock_holder)
{
    getReferenceTable()->truncate(query, metadata_snapshot, context_, lock_holder);
}

void StorageAlias::alter(const AlterCommands & commands, ContextPtr context_, AlterLockHolder & lock_holder)
{
    getReferenceTable()->alter(commands, context_, lock_holder);
}

void StorageAlias::updateExternalDynamicMetadataIfExists(ContextPtr context_)
{
    getReferenceTable()->updateExternalDynamicMetadataIfExists(context_);
}

void StorageAlias::checkAlterIsPossible(const AlterCommands & commands, ContextPtr context_) const
{
    getReferenceTable()->checkAlterIsPossible(commands, context_);
}

void StorageAlias::checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const
{
    getReferenceTable()->checkMutationIsPossible(commands, settings);
}

Pipe StorageAlias::alterPartition(
    const StorageMetadataPtr & metadata_snapshot,
    const PartitionCommands & commands,
    ContextPtr context_)
{
    return getReferenceTable()->alterPartition(metadata_snapshot, commands, context_);
}

void StorageAlias::checkAlterPartitionIsPossible(
    const PartitionCommands & commands,
    const StorageMetadataPtr & metadata_snapshot,
    const Settings & settings,
    ContextPtr context_) const
{
    getReferenceTable()->checkAlterPartitionIsPossible(commands, metadata_snapshot, settings, context_);
}

bool StorageAlias::optimize(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    bool cleanup,
    ContextPtr context_)
{
    return getReferenceTable()->optimize(query, metadata_snapshot, partition, final, deduplicate, deduplicate_by_columns, cleanup, context_);
}

QueryPipeline StorageAlias::updateLightweight(const MutationCommands & commands, ContextPtr context_)
{
    return getReferenceTable()->updateLightweight(commands, context_);
}

void StorageAlias::mutate(const MutationCommands & commands, ContextPtr context_)
{
    getReferenceTable()->mutate(commands, context_);
}

CancellationCode StorageAlias::killMutation(const String & mutation_id)
{
    return getReferenceTable()->killMutation(mutation_id);
}

void StorageAlias::waitForMutation(const String & mutation_id, bool wait_for_another_mutation)
{
    getReferenceTable()->waitForMutation(mutation_id, wait_for_another_mutation);
}

void StorageAlias::setMutationCSN(const String & mutation_id, UInt64 csn)
{
    getReferenceTable()->setMutationCSN(mutation_id, csn);
}

CancellationCode StorageAlias::killPartMoveToShard(const UUID & task_uuid)
{
    return getReferenceTable()->killPartMoveToShard(task_uuid);
}

StorageInMemoryMetadata StorageAlias::getInMemoryMetadata() const
{
    return getReferenceTable()->getInMemoryMetadata();
}

StorageMetadataPtr StorageAlias::getInMemoryMetadataPtr() const
{
    return getReferenceTable()->getInMemoryMetadataPtr();
}

StorageSnapshotPtr StorageAlias::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
{
    return getReferenceTable()->getStorageSnapshot(metadata_snapshot, query_context);
}

/// Creates a storage snapshot from given metadata and columns, which are used in query.
StorageSnapshotPtr StorageAlias::getStorageSnapshotForQuery(const StorageMetadataPtr & metadata_snapshot, const ASTPtr & query, ContextPtr query_context) const
{
    return getReferenceTable()->getStorageSnapshotForQuery(metadata_snapshot, query, query_context);
}

/// Creates a storage snapshot but without holding a data specific to storage.
StorageSnapshotPtr StorageAlias::getStorageSnapshotWithoutData(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
{
    return getReferenceTable()->getStorageSnapshotWithoutData(metadata_snapshot, query_context);
}


}
