#include <Interpreters/InterpreterCheckQuery.h>
#include <Interpreters/InterpreterFactory.h>

#include <algorithm>
#include <memory>

#include <Access/Common/AccessFlags.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>

#include <Common/FailPoint.h>
#include <Common/thread_local_rng.h>
#include <Common/typeid_cast.h>

#include <Core/Settings.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ProcessList.h>

#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTSetQuery.h>

#include <Processors/Chunk.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/IInflatingTransform.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

#include <Storages/IStorage.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool check_query_single_value_result;
    extern const SettingsMaxThreads max_threads;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
    extern const char check_table_query_delay_for_part[];
}

namespace
{

Block getSingleValueBlock(UInt8 value)
{
    return Block{{ColumnUInt8::create(1, value), std::make_shared<DataTypeUInt8>(), "result"}};
}

Block getHeaderForCheckResult(bool with_table_name)
{
    NamesAndTypes names_and_types;
    if (with_table_name)
    {
        names_and_types.emplace_back("database", std::make_shared<DataTypeString>());
        names_and_types.emplace_back("table", std::make_shared<DataTypeString>());
    }

    names_and_types.emplace_back("part_path", std::make_shared<DataTypeString>());
    names_and_types.emplace_back("is_passed", std::make_shared<DataTypeUInt8>());
    names_and_types.emplace_back("message", std::make_shared<DataTypeString>());

    ColumnsWithTypeAndName columns;
    for (const auto & [name, type] : names_and_types)
        columns.emplace_back(type->createColumn(), type, name);

    return Block(std::move(columns));
}

Chunk getChunkFromCheckResult(const CheckResult & check_result)
{
    MutableColumns columns = getHeaderForCheckResult(false).cloneEmptyColumns();
    columns[0]->insert(check_result.fs_path);
    columns[1]->insert(static_cast<UInt8>(check_result.success));
    columns[2]->insert(check_result.failure_message);
    return Chunk(std::move(columns), 1);
}

Chunk getChunkFromCheckResult(const String & database, const String & table, const CheckResult & check_result)
{
    MutableColumns columns = getHeaderForCheckResult(true).cloneEmptyColumns();
    columns[0]->insert(database);
    columns[1]->insert(table);
    columns[2]->insert(check_result.fs_path);
    columns[3]->insert(static_cast<UInt8>(check_result.success));
    columns[4]->insert(check_result.failure_message);
    return Chunk(std::move(columns), 1);
}

class TableCheckTask : public ChunkInfoCloneable<TableCheckTask>
{
public:
    TableCheckTask(StorageID table_id, const std::variant<std::monostate, ASTPtr, String> & partition_or_part, ContextPtr context)
        : table(DatabaseCatalog::instance().getTable(table_id, context))
        , check_data_tasks(table->getCheckTaskList(partition_or_part, context))
    {
        chassert(context);
        context->checkAccess(AccessType::SHOW_TABLES, table_id);
    }

    TableCheckTask(StoragePtr table_, ContextPtr context)
        : table(table_)
        , check_data_tasks(table->getCheckTaskList({}, context))
    {
        chassert(context);
        context->checkAccess(AccessType::SHOW_TABLES, table_->getStorageID());
    }

    TableCheckTask(const TableCheckTask & other)
        : table(other.table)
        , check_data_tasks(other.check_data_tasks)
        , is_finished(other.is_finished.load())
    {}

    std::optional<CheckResult> checkNext() const
    {
        if (isFinished())
            return {};

        fiu_do_on(FailPoints::check_table_query_delay_for_part,
        {
            std::chrono::milliseconds sleep_time{thread_local_rng() % 1000};
            std::this_thread::sleep_for(sleep_time);
        });

        IStorage::DataValidationTasksPtr tmp = check_data_tasks;
        auto result = table->checkDataNext(tmp);
        is_finished = !result.has_value();
        return result;
    }

    bool isFinished() const { return is_finished || !table || !check_data_tasks; }
    size_t size() const { return check_data_tasks ? check_data_tasks->size() : 0; }
    String getNameForLogs() const { return table ? table->getStorageID().getNameForLogs() : ""; }

    std::pair<String, String> getDatabaseAndTableName() const
    {
        if (!table)
            return {};
        const auto & storage_id = table->getStorageID();
        return {storage_id.getDatabaseName(), storage_id.getTableName()};
    }

private:
    StoragePtr table;
    IStorage::DataValidationTasksPtr check_data_tasks;

    mutable std::atomic_bool is_finished{false};
};

/// Sends TableCheckTask to workers
class TableCheckSource : public ISource
{
public:
    TableCheckSource(Strings databases_, ContextPtr context_, LoggerPtr log_)
        : ISource(getSingleValueBlock(0))
        , databases(databases_)
        , context(context_)
        , log(log_)
    {
    }

    TableCheckSource(std::shared_ptr<TableCheckTask> table_check_task_, LoggerPtr log_)
        : ISource(getSingleValueBlock(0))
        , table_check_task(table_check_task_)
        , log(log_)
    {
    }

    String getName() const override { return "TableCheckSource"; }

protected:
    std::optional<Chunk> tryGenerate() override
    {
        auto current_check_task = getTableCheckTask();
        if (!current_check_task)
            return {};

        progress(1, 0);

        Chunk result;
        /// source should return at least one row to start pipeline
        result.addColumn(ColumnUInt8::create(1, 1));
        /// actual data stored in chunk info
        result.getChunkInfos().add(std::move(current_check_task));
        return result;
    }

private:
    std::shared_ptr<TableCheckTask> getTableCheckTask()
    {
        if (table_check_task && !table_check_task->isFinished())
            return table_check_task;

        /// Advance iterator
        if (current_table_it && current_table_it->isValid())
            current_table_it->next();

        /// Find next table to check in current or next database
        while (true)
        {
            /// Try to get next table from current database
            auto table = getValidTable();
            if (table)
            {
                table_check_task = std::make_shared<TableCheckTask>(table, context);
                LOG_DEBUG(log, "Checking {} parts in table '{}'", table_check_task->size(), table_check_task->getNameForLogs());
                return table_check_task;
            }

            /// Move to next database
            auto database = getNextDatabase();
            if (!database)
                break;
            current_table_it = database->getTablesIterator(context);
        }

        return {};
    }

    StoragePtr getValidTable()
    {
        while (current_table_it && current_table_it->isValid())
        {
            /// Try to get next table from current database
            StoragePtr table = current_table_it->table();
            if (table && table->isMergeTree())
                return table;

            LOG_TRACE(log, "Skip checking table '{}.{}' because it is not {}",
                current_table_it->databaseName(), current_table_it->name(), table ? "MergeTree" : "found");

            current_table_it->next();
        }
        return {};
    }

    DatabasePtr getNextDatabase()
    {
        const auto & database_catalog = DatabaseCatalog::instance();

        while (!databases.empty())
        {
            auto database_name = std::move(databases.back());
            databases.pop_back();
            auto current_database = database_catalog.tryGetDatabase(database_name);
            if (current_database)
            {
                LOG_DEBUG(log, "Checking '{}' database", database_name);
                return current_database;
            }

            LOG_DEBUG(log, "Skipping database '{}' because it was dropped", database_name);
        }
        return {};
    }

    Strings databases;
    DatabaseTablesIteratorPtr current_table_it = nullptr;
    std::shared_ptr<TableCheckTask> table_check_task = nullptr;

    ContextPtr context;

    LoggerPtr log;
};

/// Receives TableCheckTask and returns CheckResult converted to sinle-row chunk
class TableCheckWorkerProcessor : public ISimpleTransform
{
public:
    TableCheckWorkerProcessor(bool with_table_name_, LoggerPtr log_)
        : ISimpleTransform(getSingleValueBlock(0), getHeaderForCheckResult(with_table_name_), true)
        , with_table_name(with_table_name_)
        , log(log_)
    {
    }

    String getName() const override { return "TableCheckWorkerProcessor"; }

protected:
    void transform(Chunk & chunk) override
    {
        auto table_check_task = chunk.getChunkInfos().get<TableCheckTask>();
        auto check_result = table_check_task->checkNext();
        if (!check_result)
        {
            chunk = {};
            return;
        }

        if (!check_result->success)
        {
            LOG_WARNING(log, "Check query for table {} failed, path {}, reason: {}",
                table_check_task->getNameForLogs(), check_result->fs_path, check_result->failure_message);
        }

        if (with_table_name)
        {
            const auto & [database, table] = table_check_task->getDatabaseAndTableName();
            chunk = getChunkFromCheckResult(database, table, *check_result);
        }
        else
        {
            chunk = getChunkFromCheckResult(*check_result);
        }
    }

private:
    /// If true, then output will contain columns with database and table names
    bool with_table_name;

    LoggerPtr log;
};

/// Accumulates all results and returns single value
/// Used when settings.check_query_single_value_result is true
class TableCheckResultEmitter : public IAccumulatingTransform
{
public:
    explicit TableCheckResultEmitter(Block input_header)
        : IAccumulatingTransform(input_header, getSingleValueBlock(1).cloneEmpty())
    {
        column_position_to_check = input_header.getPositionByName("is_passed");
    }

    String getName() const override { return "TableCheckResultEmitter"; }

    void consume(Chunk chunk) override
    {
        if (result_value == 0)
            return;

        const auto & columns = chunk.getColumns();
        if ((columns.size() != 3 && columns.size() != 5) || column_position_to_check >= columns.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of columns: {}, position {}", columns.size(), column_position_to_check);

        const auto & col = checkAndGetColumn<ColumnUInt8>(*columns[column_position_to_check]);
        for (size_t i = 0; i < col.size(); ++i)
        {
            if (col.getElement(i) == 0)
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
    size_t column_position_to_check;

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

    const auto & settings = context->getSettingsRef();
    auto processors = std::make_shared<Processors>();
    std::shared_ptr<TableCheckSource> worker_source;
    bool is_table_name_in_output = false;
    if (const auto * check_query = query_ptr->as<ASTCheckTableQuery>())
    {
        /// Check specific table
        auto table_id = context->resolveStorageID(*check_query, Context::ResolveOrdinary);
        auto table_check_task = std::make_shared<TableCheckTask>(table_id, check_query->getPartitionOrPartitionID(), context);
        worker_source = std::make_shared<TableCheckSource>(table_check_task, log);
        worker_source->addTotalRowsApprox(table_check_task->size());
    }
    else if (query_ptr->as<ASTCheckAllTablesQuery>())
    {
        is_table_name_in_output = true;
        /// Check all tables in all databases
        auto databases = getAllDatabases(context);
        LOG_DEBUG(log, "Checking {} databases", databases.size());
        worker_source = std::make_shared<TableCheckSource>(databases, context, log);
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected query {} in InterpreterCheckQuery", query_ptr->formatForErrorMessage());
    }

    size_t num_streams = std::max<size_t>(settings[Setting::max_threads], 1);

    processors->emplace_back(worker_source);
    std::vector<OutputPort *> worker_ports;
    {
        /// Create one source and N workers
        auto & worker_source_port = worker_source->getPort();

        auto resize_processor = std::make_shared<ResizeProcessor>(worker_source_port.getHeader(), 1, num_streams);
        processors->emplace_back(resize_processor);

        connect(worker_source_port, resize_processor->getInputs().front());

        auto & resize_outputs = resize_processor->getOutputs();
        for (auto & resize_output : resize_outputs)
        {
            auto worker_processor = std::make_shared<TableCheckWorkerProcessor>(is_table_name_in_output, log);
            worker_ports.emplace_back(&worker_processor->getOutputPort());
            connect(resize_output, worker_processor->getInputPort());
            processors->emplace_back(std::move(worker_processor));
        }
    }

    OutputPort * resize_outport;
    {
        chassert(!processors->empty() && !processors->back()->getOutputs().empty());
        Block header = processors->back()->getOutputs().front().getHeader();

        /// Merge output of all workers
        auto resize_processor = std::make_shared<ResizeProcessor>(header, worker_ports.size(), 1);

        auto & resize_inputs = resize_processor->getInputs();
        auto resize_inport_it = resize_inputs.begin();
        for (size_t i = 0; i < worker_ports.size(); ++i, ++resize_inport_it)
            connect(*worker_ports[i], *resize_inport_it);
        processors->emplace_back(resize_processor);

        assert(resize_processor->getOutputs().size() == 1);
        resize_outport = &resize_processor->getOutputs().front();
    }

    if (settings[Setting::check_query_single_value_result])
    {
        chassert(!processors->empty() && !processors->back()->getOutputs().empty());
        Block header = processors->back()->getOutputs().front().getHeader();

        /// Merge all results into single value
        auto emitter_processor = std::make_shared<TableCheckResultEmitter>(header);
        auto * input_port = &emitter_processor->getInputPort();
        processors->emplace_back(emitter_processor);

        connect(*resize_outport, *input_port);
    }

    BlockIO res;

    res.pipeline = QueryPipeline(Pipe(std::move(processors)));
    res.pipeline.setNumThreads(num_streams);

    return res;
}

void registerInterpreterCheckQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCheckQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCheckQuery", create_fn);
}

}
