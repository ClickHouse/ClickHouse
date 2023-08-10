#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCheckQuery.h>
#include <Access/Common/AccessFlags.h>
#include <Storages/IStorage.h>
#include <Parsers/ASTCheckQuery.h>
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

class TableCheckWorkerProcessor : public ISource
{

public:
    TableCheckWorkerProcessor(IStorage::DataValidationTasksPtr check_data_tasks_, StoragePtr table_)
        : ISource(getHeaderForCheckResult())
        , table(table_)
        , check_data_tasks(check_data_tasks_)
    {
    }

    String getName() const override { return "TableCheckWorkerProcessor"; }

protected:

    std::optional<Chunk> tryGenerate() override
    {
        bool has_nothing_to_do = false;
        auto check_result = table->checkDataNext(check_data_tasks, has_nothing_to_do);
        if (has_nothing_to_do)
            return {};

        /// We can omit manual `progess` call, ISource will may count it automatically by returned chunk
        /// However, we want to report only rows in progress
        progress(1, 0);

        if (!check_result.success)
        {
            LOG_WARNING(&Poco::Logger::get("InterpreterCheckQuery"),
                "Check query for table {} failed, path {}, reason: {}",
                table->getStorageID().getNameForLogs(),
                check_result.fs_path,
                check_result.failure_message);
        }

        return getChunkFromCheckResult(check_result);
    }

private:
    StoragePtr table;
    IStorage::DataValidationTasksPtr check_data_tasks;
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

BlockIO InterpreterCheckQuery::execute()
{
    const auto & check = query_ptr->as<ASTCheckQuery &>();
    const auto & context = getContext();
    auto table_id = context->resolveStorageID(check, Context::ResolveOrdinary);

    context->checkAccess(AccessType::SHOW_TABLES, table_id);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, context);

    auto check_data_tasks = table->getCheckTaskList(query_ptr, context);

    const auto & settings = context->getSettingsRef();

    BlockIO res;
    {
        auto processors = std::make_shared<Processors>();

        std::vector<OutputPort *> worker_ports;

        size_t num_streams = std::max<size_t>(1, settings.max_threads);

        for (size_t i = 0; i < num_streams; ++i)
        {
            auto worker_processor = std::make_shared<TableCheckWorkerProcessor>(check_data_tasks, table);
            if (i == 0)
                worker_processor->addTotalRowsApprox(check_data_tasks->size());
            worker_ports.emplace_back(&worker_processor->getPort());
            processors->emplace_back(worker_processor);
        }

        OutputPort * resize_outport;
        {
            auto resize_processor = std::make_shared<ResizeProcessor>(getHeaderForCheckResult(), worker_ports.size(), 1);

            auto & resize_inputs = resize_processor->getInputs();
            auto resize_inport_it = resize_inputs.begin();
            for (size_t i = 0; i < worker_ports.size(); ++i, ++resize_inport_it)
                connect(*worker_ports[i], *resize_inport_it);

            resize_outport = &resize_processor->getOutputs().front();
            processors->emplace_back(resize_processor);
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
