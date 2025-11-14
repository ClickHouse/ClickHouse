#include <Storages/StorageAlias.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Core/Settings.h>
#include <Access/Common/AccessFlags.h>
#include <Interpreters/InterpreterSelectQuery.h>


namespace DB
{

namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsBool allow_experimental_alias_table_engine;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

StorageAlias::StorageAlias(
    const StorageID & table_id_,
    ContextPtr context_,
    const String & target_database_,
    const String & target_table_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , target_database(target_database_)
    , target_table(target_table_)
{
    StorageID target_id(target_database, target_table);
    if (table_id_ == target_id)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Alias table cannot refer to itself");
}

StoragePtr StorageAlias::getTargetTable(std::optional<TargetAccess> access_check) const
{
    if (access_check)
    {
        if (access_check->column_names.empty())
            access_check->context->checkAccess(access_check->access_type, target_database, target_table);
        else
            access_check->context->checkAccess(access_check->access_type, target_database, target_table, access_check->column_names);
    }

    return DatabaseCatalog::instance().getTable(StorageID(target_database, target_table), getContext());
}

void StorageAlias::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    auto target_storage = getTargetTable(TargetAccess{local_context, AccessType::SELECT, column_names});
    auto lock = target_storage->lockForShare(
        local_context->getCurrentQueryId(),
        local_context->getSettingsRef()[Setting::lock_acquire_timeout]);

    auto target_metadata = target_storage->getInMemoryMetadataPtr();
    auto target_snapshot = target_storage->getStorageSnapshot(target_metadata, local_context);

    target_storage->read(
        query_plan,
        column_names,
        target_snapshot,
        query_info,
        local_context,
        processed_stage,
        max_block_size,
        num_streams);

    query_plan.addStorageHolder(target_storage);
    query_plan.addTableLock(std::move(lock));
}

SinkToStoragePtr StorageAlias::write(
    const ASTPtr & query,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    ContextPtr local_context,
    bool async_insert)
{
    auto target_storage = getTargetTable(TargetAccess{local_context, AccessType::INSERT});
    auto lock = target_storage->lockForShare(
        local_context->getCurrentQueryId(),
        local_context->getSettingsRef()[Setting::lock_acquire_timeout]);

    auto target_metadata = target_storage->getInMemoryMetadataPtr();
    auto sink = target_storage->write(query, target_metadata, local_context, async_insert);

    sink->addTableLock(lock);
    return sink;
}

void StorageAlias::alter(
    const AlterCommands & params,
    ContextPtr local_context,
    AlterLockHolder & table_lock_holder)
{
    auto target_storage = getTargetTable(TargetAccess{local_context, AccessType::ALTER});
    target_storage->alter(params, local_context, table_lock_holder);
}

void StorageAlias::truncate(
    const ASTPtr & query,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    ContextPtr local_context,
    TableExclusiveLockHolder & table_lock_holder)
{
    auto target_storage = getTargetTable(TargetAccess{local_context, AccessType::TRUNCATE});
    auto target_metadata = target_storage->getInMemoryMetadataPtr();
    target_storage->truncate(query, target_metadata, local_context, table_lock_holder);
}

bool StorageAlias::optimize(
    const ASTPtr & query,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    bool cleanup,
    ContextPtr local_context)
{
    auto target_storage = getTargetTable(TargetAccess{local_context, AccessType::OPTIMIZE});
    auto target_metadata = target_storage->getInMemoryMetadataPtr();
    return target_storage->optimize(query, target_metadata, partition, final, deduplicate,
                                    deduplicate_by_columns, cleanup, local_context);
}

Pipe StorageAlias::alterPartition(
    const StorageMetadataPtr &,
    const PartitionCommands & commands,
    ContextPtr local_context)
{
    auto target_storage = getTargetTable(TargetAccess{local_context, AccessType::ALTER});
    auto target_metadata = target_storage->getInMemoryMetadataPtr();
    return target_storage->alterPartition(target_metadata, commands, local_context);
}

void StorageAlias::checkAlterPartitionIsPossible(
    const PartitionCommands & commands,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const Settings & settings,
    ContextPtr local_context) const
{
    auto target_storage = getTargetTable();
    auto target_metadata = target_storage->getInMemoryMetadataPtr();
    target_storage->checkAlterPartitionIsPossible(commands, target_metadata, settings, local_context);
}

void StorageAlias::mutate(const MutationCommands & commands, ContextPtr local_context)
{
    auto target_storage = getTargetTable(TargetAccess{local_context, AccessType::ALTER});
    target_storage->mutate(commands, local_context);
}

QueryPipeline StorageAlias::updateLightweight(const MutationCommands & commands, ContextPtr local_context)
{
    auto target_storage = getTargetTable(TargetAccess{local_context, AccessType::ALTER});
    return target_storage->updateLightweight(commands, local_context);
}

CancellationCode StorageAlias::killMutation(const String & mutation_id)
{
    return getTargetTable()->killMutation(mutation_id);
}

void StorageAlias::waitForMutation(const String & mutation_id, bool wait_for_another_mutation)
{
    getTargetTable()->waitForMutation(mutation_id, wait_for_another_mutation);
}

void StorageAlias::setMutationCSN(const String & mutation_id, UInt64 csn)
{
    getTargetTable()->setMutationCSN(mutation_id, csn);
}

CancellationCode StorageAlias::killPartMoveToShard(const UUID & task_uuid)
{
    return getTargetTable()->killPartMoveToShard(task_uuid);
}

void StorageAlias::updateExternalDynamicMetadataIfExists(ContextPtr local_context)
{
    getTargetTable()->updateExternalDynamicMetadataIfExists(local_context);
}

std::optional<QueryPipeline> StorageAlias::distributedWrite(const ASTInsertQuery & query, ContextPtr local_context)
{
    return getTargetTable(TargetAccess{local_context, AccessType::INSERT})->distributedWrite(query, local_context);
}

StorageSnapshotPtr StorageAlias::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
{
    return getTargetTable()->getStorageSnapshot(metadata_snapshot, query_context);
}

StorageSnapshotPtr StorageAlias::getStorageSnapshotWithoutData(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
{
    return getTargetTable()->getStorageSnapshotWithoutData(metadata_snapshot, query_context);
}

void StorageAlias::rename(const String & /* new_path_to_table_data */, const StorageID & new_table_id)
{
    // Only rename the alias itself, not the target table
    renameInMemory(new_table_id);
}

QueryProcessingStage::Enum StorageAlias::getQueryProcessingStage(
    ContextPtr local_context,
    QueryProcessingStage::Enum to_stage,
    const StorageSnapshotPtr & /* storage_snapshot */,
    SelectQueryInfo & query_info) const
{
    auto target_storage = getTargetTable();
    auto target_metadata = target_storage->getInMemoryMetadataPtr();
    auto target_snapshot = target_storage->getStorageSnapshot(target_metadata, local_context);
    return target_storage->getQueryProcessingStage(local_context, to_stage, target_snapshot, query_info);
}

void registerStorageAlias(StorageFactory & factory)
{
    factory.registerStorage("Alias", [](const StorageFactory::Arguments & args)
    {
        // Supported syntaxes:
        //  CREATE TABLE t2 ENGINE = Alias(t)
        //  CREATE TABLE t2 ENGINE = Alias(db, t)
        //  CREATE TABLE t2 ENGINE = Alias('t')
        //  CREATE TABLE t2 ENGINE = Alias('db', 't')

        auto local_context = args.getLocalContext();

        // Compatible with existing Alias tables
        if (args.mode == LoadingStrictnessLevel::CREATE && !local_context->getSettingsRef()[Setting::allow_experimental_alias_table_engine])
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED, "Experimental Alias table engine is not enabled (turn on setting 'allow_experimental_alias_table_engine')");

        String target_database;
        String target_table;

        if (args.engine_args.empty())
        {
            // Note: CREATE TABLE ... AS ... syntax is not supported
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Storage Alias requires target table name in ENGINE arguments. "
                "Use: ENGINE = Alias('table_name') or ENGINE = Alias('database', 'table_name').");
        }
        else if (args.engine_args.size() == 1)
        {
            // Syntax: ENGINE = Alias(table_name) or ENGINE = Alias(db.table_name)
            auto evaluated_arg = evaluateConstantExpressionOrIdentifierAsLiteral(args.engine_args[0], local_context);
            String table_arg = checkAndGetLiteralArgument<String>(evaluated_arg, "table_name");

            auto dot_pos = table_arg.find('.');
            if (dot_pos != String::npos)
            {
                target_database = table_arg.substr(0, dot_pos);
                target_table = table_arg.substr(dot_pos + 1);
            }
            else
            {
                target_table = table_arg;
                target_database = args.table_id.database_name;
            }
        }
        else if (args.engine_args.size() == 2)
        {
            // Syntax: ENGINE = Alias(database_name, table_name)
            auto evaluated_db = evaluateConstantExpressionOrIdentifierAsLiteral(args.engine_args[0], local_context);
            auto evaluated_table = evaluateConstantExpressionOrIdentifierAsLiteral(args.engine_args[1], local_context);
            target_database = checkAndGetLiteralArgument<String>(evaluated_db, "database_name");
            target_table = checkAndGetLiteralArgument<String>(evaluated_table, "table_name");
        }
        else
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Storage Alias requires at most 2 arguments: database name and table name");
        }

        // Storage Alias does not support explicit column definitions
        // Columns are always dynamically fetched from the target table
        // Only check for CREATE, not for ATTACH/RESTORE
        if (!args.columns.empty() && args.mode < LoadingStrictnessLevel::ATTACH)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Storage Alias does not support explicit column definitions");
        }

        return std::make_shared<StorageAlias>(
            args.table_id,
            local_context,
            target_database,
            target_table);
    },
    {
        .supports_schema_inference = true
    });
}

}
