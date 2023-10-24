#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCheckQuery.h>
#include <Access/Common/AccessFlags.h>
#include <Storages/IStorage.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <Interpreters/ProcessList.h>
#include <algorithm>

#include <Columns/IColumn.h>

#include <Processors/ResizeProcessor.h>
#include <Processors/IAccumulatingTransform.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

Block getSingleValueBlock(UInt8 value)
{
    return Block{{ColumnUInt8::create(1, value), std::make_shared<DataTypeUInt8>(), "result"}};
}

Block getHeaderForCheckResult()
{
    auto names_and_types = NamesAndTypes{
        {"part_path", std::make_shared<DataTypeString>()},
        {"is_passed", std::make_shared<DataTypeUInt8>()},
        {"message", std::make_shared<DataTypeString>()},
    };

    return Block({
        {names_and_types[0].type->createColumn(), names_and_types[0].type, names_and_types[0].name},
        {names_and_types[1].type->createColumn(), names_and_types[1].type, names_and_types[1].name},
        {names_and_types[2].type->createColumn(), names_and_types[2].type, names_and_types[2].name},
    });
}

Chunk getChunkFromCheckResult(const CheckResult & check_result)
{
    MutableColumns columns = getHeaderForCheckResult().cloneEmptyColumns();
    columns[0]->insert(check_result.fs_path);
    columns[1]->insert(static_cast<UInt8>(check_result.success));
    columns[2]->insert(check_result.failure_message);
    return Chunk(std::move(columns), 1);
}

class TableCheckTask
{
public:
    TableCheckTask(StorageID table_id, const std::variant<std::monostate, ASTPtr, String> & partition_or_part, ContextPtr context)
        : table(DatabaseCatalog::instance().getTable(table_id, context))
        , check_data_tasks(table->getCheckTaskList(partition_or_part, context))
    {
        context->checkAccess(AccessType::SHOW_TABLES, table_id);
    }

    TableCheckTask(StoragePtr table_, ContextPtr context)
        : table(table_)
        , check_data_tasks(table->getCheckTaskList({}, context))
    {
        context->checkAccess(AccessType::SHOW_TABLES, table_->getStorageID());
    }

    std::optional<CheckResult> checkNext()
    {
        if (!table || !check_data_tasks)
            return {};
        auto result = table->checkDataNext(check_data_tasks);
        is_finished = !result.has_value();
        return result;
    }

    bool isFinished() const { return !table || !check_data_tasks || is_finished; }
    size_t size() const { return check_data_tasks ? check_data_tasks->size() : 0; }
    String getNameForLogs() const { return table ? table->getStorageID().getNameForLogs() : ""; }

private:
    StoragePtr table;
    IStorage::DataValidationTasksPtr check_data_tasks;

    std::atomic_bool is_finished{false};
};

class TableCheckWorkerProcessor : public ISource
{
public:
    TableCheckWorkerProcessor(std::shared_ptr<TableCheckTask> table_check_task_, Poco::Logger * log_)
        : ISource(getHeaderForCheckResult())
        , table_check_task(table_check_task_)
        , log(log_)
    {
    }


    TableCheckWorkerProcessor(Strings databases_, ContextPtr context_, Poco::Logger * log_)
        : ISource(getHeaderForCheckResult())
        , databases(databases_)
        , context(context_)
        , log(log_)
    {
    }

    String getName() const override { return "TableCheckWorkerProcessor"; }

protected:

    std::optional<Chunk> tryGenerate() override
    {
        while (true)
        {
            std::shared_ptr<TableCheckTask> current_check_task = getTableToCheck();
            if (!current_check_task)
                return {};

            auto check_result = current_check_task->checkNext();
            if (!check_result)
                continue;

            /// We can omit manual `progess` call, ISource will may count it automatically by returned chunk
            /// However, we want to report only rows in progress, since bytes doesn't make sense here
            progress(1, 0);

            if (!check_result->success)
            {
                LOG_WARNING(log, "Check query for table {} failed, path {}, reason: {}",
                    current_check_task->getNameForLogs(), check_result->fs_path, check_result->failure_message);
            }

            return getChunkFromCheckResult(*check_result);
        }
    }

private:
    std::shared_ptr<TableCheckTask> getTableToCheck()
    {
        std::lock_guard lock(current_table_mutex);
        if (table_check_task && !table_check_task->isFinished())
            return table_check_task;

        const auto & database_catalog = DatabaseCatalog::instance();

        while ((current_table_it && current_table_it->isValid()) || !databases.empty())
        {
            if (!context)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Context is not set");

            while (current_table_it && current_table_it->isValid())
            {
                /// Try to get next table from current database
                current_table_it->next();
                StoragePtr table = current_table_it->table();
                if (table && table->isMergeTree())
                {
                    table_check_task = std::make_shared<TableCheckTask>(table, context);
                    addTotalRowsApprox(table_check_task->size());
                    LOG_DEBUG(log, "Checking {} parts in table {}", table_check_task->size(), table_check_task->getNameForLogs());
                    return table_check_task;
                }

                LOG_TRACE(log, "Skip checking table {} because it is not {}", current_table_it->name(), table ? "MergeTree" : "found");
            }

            /// No more tables in current database, try to get next database
            if (!databases.empty())
            {
                auto database_name = std::move(databases.back());
                databases.pop_back();
                current_database = database_catalog.tryGetDatabase(database_name);

                if (current_database)
                {
                    LOG_DEBUG(log, "Checking {} database", database_name);
                    current_table_it = current_database->getTablesIterator(context);
                }
                else
                {
                    LOG_DEBUG(log, "Skipping database {} because it was dropped", database_name);
                }
            }
            else
            {
                /// No more databases, we are done
                return {};
            }
        }
        return {};
    }

    std::mutex current_table_mutex;
    Strings databases;
    DatabasePtr current_database = nullptr;
    DatabaseTablesIteratorPtr current_table_it = nullptr;

    std::shared_ptr<TableCheckTask> table_check_task = nullptr;

    ContextPtr context;

    Poco::Logger * log;
};

class TableCheckResultEmitter : public IAccumulatingTransform
{
public:
    TableCheckResultEmitter() : IAccumulatingTransform(getHeaderForCheckResult(), getSingleValueBlock(1).cloneEmpty()) {}

    String getName() const override { return "TableCheckResultEmitter"; }

    void consume(Chunk chunk) override
    {
        if (result_value == 0)
            return;

        auto columns = chunk.getColumns();
        if (columns.size() != 3)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of columns: {}", columns.size());

        const auto * col = checkAndGetColumn<ColumnUInt8>(columns[1].get());
        for (size_t i = 0; i < col->size(); ++i)
        {
            if (col->getElement(i) == 0)
            {
                result_value = 0;
                return;
            }
        }
    }

    Chunk generate() override
    {
        if (is_value_emitted.exchange(true))
            return {};
        auto block = getSingleValueBlock(result_value);
        return Chunk(block.getColumns(), block.rows());
    }

private:
    std::atomic<UInt8> result_value{1};
    std::atomic_bool is_value_emitted{false};
};

}

InterpreterCheckQuery::InterpreterCheckQuery(const ASTPtr & query_ptr_, ContextPtr context_)
    : WithContext(context_)
    , query_ptr(query_ptr_)
{
}

static Strings getAllDatabases(const ContextPtr & context)
{
    Strings res;
    const auto & databases = DatabaseCatalog::instance().getDatabases();
    res.reserve(databases.size());
    for (const auto & [database_name, _] : databases)
    {
        if (DatabaseCatalog::isPredefinedDatabase(database_name))
            continue;
        context->checkAccess(AccessType::SHOW_DATABASES, database_name);
        res.emplace_back(database_name);
    }
    return res;
}

BlockIO InterpreterCheckQuery::execute()
{
    const auto & context = getContext();

    BlockIO res;
    {
        const auto & settings = context->getSettingsRef();
        auto processors = std::make_shared<Processors>();

        std::vector<OutputPort *> worker_ports;
        size_t num_streams = std::max<size_t>(settings.max_threads, 1);
        if (const auto * check_query = query_ptr->as<ASTCheckTableQuery>())
        {
            /// Check specific table
            auto table_id = context->resolveStorageID(*check_query, Context::ResolveOrdinary);
            auto table_check_task = std::make_shared<TableCheckTask>(table_id, check_query->getPartitionOrPartitionID(), context);
            LOG_DEBUG(log, "Checking {} parts in table {}", table_check_task->size(), table_check_task->getNameForLogs());
            for (size_t i = 0; i < num_streams; ++i)
            {
                auto worker_processor = std::make_shared<TableCheckWorkerProcessor>(table_check_task, log);
                /// Set total rows approx to first worker only not to count it multiple times
                if (i == 0)
                    worker_processor->addTotalRowsApprox(table_check_task->size());

                worker_ports.emplace_back(&worker_processor->getPort());
                processors->emplace_back(std::move(worker_processor));
            }
        }
        else if (query_ptr->as<ASTCheckAllTablesQuery>())
        {
            /// Check all tables in all databases
            auto databases = getAllDatabases(context);
            LOG_DEBUG(log, "Checking {} databases", databases.size());
            for (size_t i = 0; i < num_streams; ++i)
            {
                auto worker_processor = std::make_shared<TableCheckWorkerProcessor>(databases, context, log);
                worker_ports.emplace_back(&worker_processor->getPort());
                processors->emplace_back(std::move(worker_processor));
            }
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected query {} in InterpreterCheckQuery", query_ptr->formatForErrorMessage());
        }

        OutputPort * resize_outport;
        {
            auto resize_processor = std::make_shared<ResizeProcessor>(getHeaderForCheckResult(), worker_ports.size(), 1);

            auto & resize_inputs = resize_processor->getInputs();
            auto resize_inport_it = resize_inputs.begin();
            for (size_t i = 0; i < worker_ports.size(); ++i, ++resize_inport_it)
                connect(*worker_ports[i], *resize_inport_it);
            processors->emplace_back(resize_processor);

            assert(resize_processor->getOutputs().size() == 1);
            resize_outport = &resize_processor->getOutputs().front();
        }

        if (settings.check_query_single_value_result)
        {
            auto emitter_processor = std::make_shared<TableCheckResultEmitter>();
            auto * input_port = &emitter_processor->getInputPort();
            processors->emplace_back(emitter_processor);

            connect(*resize_outport, *input_port);
        }

        res.pipeline = QueryPipeline(Pipe(std::move(processors)));
        res.pipeline.setNumThreads(num_streams);
    }
    return res;
}

}
