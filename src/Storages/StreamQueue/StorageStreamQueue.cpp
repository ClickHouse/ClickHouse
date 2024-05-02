#include <Formats/FormatFactory.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Storages/StorageFactory.h>
#include <Storages/StreamQueue/StorageStreamQueue.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include "Common/ZooKeeper/Types.h"
#include "Parsers/ASTLiteral.h"

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

zkutil::ZooKeeperPtr StorageStreamQueue::getZooKeeper() const
{
    return getContext()->getZooKeeper();
}

StorageStreamQueue::StorageStreamQueue(
    std::unique_ptr<StreamQueueSettings> settings_,
    const StorageID & table_id_,
    ContextPtr context_,
    StorageID source_table_id_,
    std::shared_ptr<ASTIdentifier> key_column_,
    const Names & column_names_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment)
    : IStorage(table_id_)
    , WithContext(context_)
    , settings(std::move(settings_))
    , source_table_id(source_table_id_)
    , column_names(column_names_)
    , key_column(key_column_)
    , log(getLogger("StorageStreamQueue (" + table_id_.table_name + ")"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);

    setInMemoryMetadata(storage_metadata);

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

std::string StorageStreamQueue::createZooKeeperPath() {
    return "/storage/stream/queue/test/path";
}

bool StorageStreamQueue::createZooKeeperNode() {
    auto zookeeper = getZooKeeper();
    auto path = createZooKeeperPath();
    zookeeper->createAncestors(path);
    auto code = zookeeper->tryCreate(path, "", zkutil::CreateMode::Ephemeral);
    if (code == Coordination::Error::ZNODEEXISTS) {
        return false;
    }

    downloading = true;
    return true;
}

void StorageStreamQueue::threadFunc()
{
    if (shutdown_called)
        return;

    if (!(downloading || createZooKeeperNode())) {
        return;
    }

    const size_t dependencies_count = DatabaseCatalog::instance().getDependentViews(getStorageID()).size();
    if (dependencies_count)
        move_data();
    task->scheduleAfter(settings->streamqueue_polling_min_timeout_ms);
}

void StorageStreamQueue::move_data()
{
    LOG_TRACE(log, "Source table name: {}", source_table_id);
    auto source_table = DatabaseCatalog::instance().getTable(source_table_id, getContext());
    if (!source_table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Source table {} doesn't exist.", source_table_id.getNameForLogs());

    auto table_id = getStorageID();
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (!source_table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Engine table {} doesn't exist.", table_id.getNameForLogs());

    auto queue_context = Context::createCopy(getContext());
    queue_context->makeQueryContext();

    auto select = std::make_shared<ASTSelectQuery>();
    select->replaceDatabaseAndTable(source_table_id);

    auto select_expr_list = std::make_shared<ASTExpressionList>();
    for (const auto & name : column_names)
        select_expr_list->children.push_back(std::make_shared<ASTIdentifier>(name));
    select->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expr_list));

    auto gt_function = makeASTFunction("greaterOrEquals");
    gt_function->arguments->children.push_back(key_column);
    gt_function->arguments->children.push_back(std::make_shared<ASTLiteral>(10));
    select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(gt_function));

    auto order_by = std::make_shared<ASTExpressionList>();
    auto order_by_elem = std::make_shared<ASTOrderByElement>();
    order_by_elem->children.push_back(key_column);
    order_by_elem->direction = 1;
    order_by->children.push_back(order_by_elem);
    select->setExpression(ASTSelectQuery::Expression::ORDER_BY, std::move(order_by));

    UInt64 limit_offset = settings->streamqueue_max_rows_per_iter;
    ASTPtr new_limit_offset_ast = std::make_shared<ASTLiteral>(limit_offset);
    select->setExpression(ASTSelectQuery::Expression::LIMIT_BY_OFFSET, std::move(new_limit_offset_ast));

    InterpreterSelectQuery select_interpreter(select, queue_context, SelectQueryOptions());
    auto select_block_io = select_interpreter.execute();
    PullingPipelineExecutor pulling_executor(select_block_io.pipeline);

    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = table_id;

    InterpreterInsertQuery insert_interpreter(insert, queue_context, false, true, true);
    auto insert_block_io = insert_interpreter.execute();
    PushingPipelineExecutor pushing_executor(insert_block_io.pipeline);

    Block block;
    while (pulling_executor.pull(block))
    {
        if (!block)
            continue;
        pushing_executor.push(block);
    }
    pushing_executor.finish();
}

StoragePtr createStorage(const StorageFactory::Arguments & args)
{
    auto & engine_args = args.engine_args;

    if (engine_args.size() != 2)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Storage StreamQueue requires exactly 2 arguments: {}, <key column name>", StreamQueueArgumentName);

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

    const auto key_arg = engine_args[1];
    if (!key_arg->as<ASTIdentifier>())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Argument <key column name> must be table identifier, get {} (value: {})",
            key_arg ? key_arg->getID() : "NULL",
            key_arg ? key_arg->formatForErrorMessage() : "NULL");
    }

    auto key_column = static_pointer_cast<ASTIdentifier>(key_arg);

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
    Names column_names;
    for (const auto & column : args.columns.getOrdinary())
        column_names.push_back(column.name);
    if (column_names.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table must have at least 1 column");

    auto settings = std::make_unique<StreamQueueSettings>();
    if (args.storage_def->settings)
        settings->loadFromQuery(*args.storage_def);

    return std::make_shared<StorageStreamQueue>(
        std::move(settings),
        args.table_id,
        args.getContext(),
        source_storage_id,
        key_column,
        column_names,
        args.columns,
        args.constraints,
        args.comment);
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
