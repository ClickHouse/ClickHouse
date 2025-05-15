#include "StorageLance.h"

#if USE_LANCE

#    include "LanceSink.h"
#    include "LanceSource.h"

#    include <Interpreters/evaluateConstantExpression.h>
#    include <Storages/AlterCommands.h>
#    include <Storages/StorageFactory.h>
#    include <Storages/checkAndGetLiteralArgument.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int CANNOT_CREATE_DATABASE;
extern const int NOT_IMPLEMENTED;
extern const int METADATA_MISMATCH;
extern const int LOGICAL_ERROR;
extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
}


StorageLance::StorageLance(
    const StorageID & table_id_,
    LanceDBPtr lance_db_,
    const String & table_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment_)
    : IStorage(table_id_), lance_db(std::move(lance_db_)), table_name(table_name_)
{
    StorageInMemoryMetadata storage_metadata;

    lance_table = lance_db->tryOpenTable(table_name);

    if (columns_.empty())
    {
        if (!lance_table)
        {
            throw Exception(ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED, "Trying to create Lance table with empty schema");
        }
        storage_metadata.setColumns(lance_table->getSchema());
    }
    else
    {
        if (!lance_table)
        {
            lance_table = lance_db->tryCreateTableWithSchema(table_name, columns_);
            if (!lance_table)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot open or create lance table");
            }
        }
        else if (lance_table->getSchema() != columns_)
        {
            throw Exception(ErrorCodes::METADATA_MISMATCH, "Column schema mismatch with already existing Lance table schema");
        }
        storage_metadata.setColumns(columns_);
    }

    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);
    storage_metadata.setComment(comment_);
}

Pipe StorageLance::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context_*/,
    QueryProcessingStage::Enum,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({column_data.type, column_data.name});
    }

    return Pipe(std::make_shared<LanceSource>(LanceTableReader(lance_table, max_block_size), sample_block));
}

SinkToStoragePtr
StorageLance::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr /*context*/, bool /*async_insert*/)
{
    return std::make_shared<LanceSink>(metadata_snapshot, lance_table);
}

void StorageLance::alter(const AlterCommands & commands, ContextPtr local_context, AlterLockHolder & alter_lock_holder)
{
    IStorage::alter(commands, local_context, alter_lock_holder);
    for (const auto & command : commands)
    {
        if (command.type == AlterCommand::Type::RENAME_COLUMN)
        {
            lance_table->renameColumn(command.column_name, command.rename_to);
        }
        else if (command.type == AlterCommand::Type::DROP_COLUMN)
        {
            lance_table->dropColumn(command.column_name);
        }
    }
}

void StorageLance::checkAlterIsPossible(const AlterCommands & commands, ContextPtr /*local_context*/) const
{
    for (const auto & command : commands)
    {
        if (command.type != AlterCommand::Type::RENAME_COLUMN && command.type != AlterCommand::Type::DROP_COLUMN)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}", command.type, getName());
    }
}

void StorageLance::drop()
{
    if (!lance_db->dropTable(table_name.c_str()))
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Couldn't drop lance table: {}", table_name);
    }
}


void registerStorageLance(StorageFactory & factory)
{
    factory.registerStorage(
        "Lance",
        [](const StorageFactory::Arguments & args) -> StoragePtr
        {
            ASTs & engine_args = args.engine_args;

            if (engine_args.size() != 2)
            {
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Lance database requires 2 arguments: database path, table name");
            }

            for (auto & engine_arg : engine_args)
            {
                engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());
            }

            const auto database_path = checkAndGetLiteralArgument<String>(engine_args[0], "database_path");
            const auto table_name = checkAndGetLiteralArgument<String>(engine_args[1], "table_name");

            LanceDBPtr lance_db = LanceDB::connect(database_path);
            if (!lance_db)
            {
                throw Exception(ErrorCodes::CANNOT_CREATE_DATABASE, "Cannot connect to Lance database by passed path: {}", database_path);
            }

            return std::make_shared<StorageLance>(args.table_id, lance_db, table_name, args.columns, args.constraints, args.comment);
        });
}

} // namespace DB

#endif
