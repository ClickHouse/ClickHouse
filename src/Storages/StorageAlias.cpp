#include <Storages/StorageAlias.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>

namespace DB
{

namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
}
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

StorageAlias::StorageAlias(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    const StorageID & ref_table_id_)
    : IStorage(table_id_)
    , ref_table_id(ref_table_id_)
{
    if (table_id_ == ref_table_id_)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Alias table cannot refer to itself.");

    StorageInMemoryMetadata storage_metadata;
    if (columns_.empty())
    {
        auto ref_table = DatabaseCatalog::instance().getTable(ref_table_id, context_);
        storage_metadata.setColumns(ref_table->getInMemoryMetadataPtr()->getColumns());
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}

void registerStorageAlias(StorageFactory & factory)
{
    factory.registerStorage("Alias", [](const StorageFactory::Arguments & args)
    {
        /** Arguments of engine is following: *
          * - the database name of the referenced table
          * - the table name of the referenced table
          */

        ASTs & engine_args = args.engine_args;
        if (engine_args.size() != 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Storage Alias requires 2 arguments - database name of the reference table and table name of the reference table");

        const ContextPtr & local_context = args.getLocalContext();
        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], local_context);
        engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], local_context);
        String database = checkAndGetLiteralArgument<String>(engine_args[0], "database");
        String table = checkAndGetLiteralArgument<String>(engine_args[1], "table");

        return std::make_shared<StorageAlias>(
            args.table_id,
            args.columns,
            args.constraints,
            args.comment,
            args.getContext(),
            StorageID(database, table));
    },
    {
        .supports_schema_inference = true,
    });
}

StoragePtr StorageAlias::getRefStorage(ContextPtr context)
{
    StorageMetadataPtr metadata_snapshot;
    StorageMetadataPtr ref_metadata_snapshot;
    auto lock_acquire_timeout = context->getSettingsRef()[Setting::lock_acquire_timeout];
    {
        auto lock = lockForShare(context->getCurrentQueryId(), lock_acquire_timeout);
        metadata_snapshot = getInMemoryMetadataPtr();
    }

    auto ref_storage =  DatabaseCatalog::instance().getTable(ref_table_id, context);
    {
        auto ref_lock = ref_storage->lockForShare(context->getCurrentQueryId(), lock_acquire_timeout);
        ref_metadata_snapshot = ref_storage->getInMemoryMetadataPtr();
    }

    if (metadata_snapshot->getColumns()!= ref_metadata_snapshot->getColumns())
        throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Columns of Alias table {} and its reference table {} are different", getStorageID().getFullTableName(), ref_table_id.getNameForLogs());

    return ref_storage;
}

}
