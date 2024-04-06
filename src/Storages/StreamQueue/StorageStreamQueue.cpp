#include <Formats/FormatFactory.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/StorageFactory.h>
#include <Storages/StreamQueue/StorageStreamQueue.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/checkAndGetLiteralArgument.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int BAD_ARGUMENTS;
}
namespace
{
StorageID getSourceStorage(ContextPtr context, const ASTIdentifier & arg)
{
    std::string database_name;
    std::string table_name;

    if (arg.compound())
    {
        database_name = arg.name_parts[0];
        table_name = arg.name_parts[1];
    }
    else
    {
        table_name = arg.name_parts[0];
    }

    StorageID storage_id(database_name, table_name);
    return context->resolveStorageID(storage_id);
}

ColumnsDescription getColumns(ContextPtr context, const StorageID & table_id)
{
    auto table = DatabaseCatalog::instance().getTable(table_id, context);
    auto metadata_snapshot = table->getInMemoryMetadataPtr();
    return metadata_snapshot->getColumns();
}
}


StorageStreamQueue::StorageStreamQueue(
    std::unique_ptr<StreamQueueSettings> settings_,
    const StorageID & table_id_,
    ContextPtr context_,
    const StorageID & source_table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment)
    : IStorage(table_id_)
    , WithContext(context_)
    , settings(std::move(settings_))
    , source_table_id(source_table_id_)
    , log(getLogger("StorageStreamQueue (" + table_id_.table_name + ")"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);

    setInMemoryMetadata(storage_metadata);
    setVirtuals(VirtualColumnUtils::getVirtualsForFileLikeStorage(storage_metadata.getColumns()));

    task = getContext()->getSchedulePool().createTask("StreamQueueTask", [this] { threadFunc(); });

    LOG_INFO(log, "Source table id: {}", source_table_id);
}

void StorageStreamQueue::startup()
{
    if (task)
        task->activateAndSchedule();
}

void StorageStreamQueue::shutdown(bool)
{
    LOG_TRACE(log, "Shutting down storage...");

    shutdown_called = true;
    if (task)
        task->deactivate();
    LOG_TRACE(log, "Shut down storage");
}

void StorageStreamQueue::threadFunc()
{
    if (shutdown_called)
        return;

    task->scheduleAfter(settings->streamqueue_polling_min_timeout_ms);
}

StoragePtr createStorage(const StorageFactory::Arguments & args)
{
    auto & engine_args = args.engine_args;
    if (engine_args.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "External data source must have arguments");


    if (engine_args.size() != 1)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Storage StreamQueue requires exactly 1 argument: {}", StreamQueueArgumentName);

    const auto arg = engine_args[0];

    if (!arg->as<ASTIdentifier>())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Argument {} must be table identifier, get {} (value: {})",
            StreamQueueArgumentName,
            arg ? arg->getID() : "NULL",
            arg ? arg->formatForErrorMessage() : "NULL");
    }

    const ASTIdentifier & identifier_expression = *arg->as<ASTIdentifier>();
    auto source_storage_id = getSourceStorage(args.getLocalContext(), identifier_expression);


    auto source_columns_description = getColumns(args.getLocalContext(), source_storage_id);

    if (args.columns != source_columns_description)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "New table must have same columns as source.\nNew table's columns:\n{}\nSource table's columns:\n{}",
            args.columns.toString(),
            source_columns_description.toString());
    }

    auto settings = std::make_unique<StreamQueueSettings>();
    if (args.storage_def->settings)
        settings->loadFromQuery(*args.storage_def);

    return std::make_shared<StorageStreamQueue>(
        std::move(settings), args.table_id, args.getContext(), source_storage_id, args.columns, args.constraints, args.comment);
}

void registerStorageStreamQueue(StorageFactory & factory)
{
    factory.registerStorage(
        StreamQueueStorageName,
        createStorage,
        {
            .supports_settings = true,
            .supports_schema_inference = true,
        });
}
};
