#include <Storages/Queue/StorageQueue.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Databases/IDatabase.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/ISimpleTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StreamingStorageRegistry.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <fmt/ranges.h>

#include <base/scope_guard.h>

#include <algorithm>
#include <unordered_set>


namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_queue_table_engine;
    extern const SettingsBool queue_commit_on_select;
    extern const SettingsString queue_consumer_offset;
    extern const SettingsString queue_consumer_group;
    extern const SettingsUInt64 queue_max_batch_size;
    extern const SettingsBool queue_reset_consumer_offset;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_NOT_ALLOWED;
    extern const int UNKNOWN_STORAGE;
}

namespace
{
    constexpr std::string_view ID_COLUMN = "_queue_id";
    constexpr std::string_view VERSION_COLUMN = "_queue_version";
    constexpr std::string_view IS_DELETED_COLUMN = "_queue_is_deleted";
    constexpr std::string_view CREATED_AT_COLUMN = "_queue_created_at";

    constexpr UInt64 DEFAULT_RETENTION_SECONDS = 7 * 24 * 60 * 60;
    constexpr UInt64 DEFAULT_MAX_BATCH_SIZE = 65536;
    constexpr UInt64 DEFAULT_POLLING_INTERVAL_MS = 100;

    bool startsAtLatest(const String & offset)
    {
        if (offset == "earliest")
            return false;
        if (offset == "latest")
            return true;
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Setting `queue_consumer_offset` must be `earliest` or `latest`, got '{}'",
            offset);
    }

    bool queryMayExcludeSourceRows(const ASTPtr & query)
    {
        if (!query)
            return false;

        if (const auto * select = query->as<ASTSelectQuery>();
            select
            && (select->distinct
                || select->limitLength()
                || select->limitBy()
                || select->join()
                || select->having()
                || select->qualify()))
            return true;

        for (const auto & child : query->children)
        {
            if (queryMayExcludeSourceRows(child))
                return true;
        }
        return false;
    }

    ASTPtr parseCreateQuery(const String & query)
    {
        ParserCreateQuery parser;
        return parseQuery(
            parser,
            query,
            "internal Queue table definition",
            DBMS_DEFAULT_MAX_QUERY_SIZE,
            DBMS_DEFAULT_MAX_PARSER_DEPTH,
            DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }

    String quoteTable(const StorageID & table_id)
    {
        return fmt::format(
            "{}.{}",
            backQuoteIfNeed(table_id.getDatabaseName()),
            backQuoteIfNeed(table_id.getTableName()));
    }

    ASTPtr makeInsertQuery(const StorageID & table_id, const Names & columns)
    {
        auto insert = make_intrusive<ASTInsertQuery>();
        insert->table_id = table_id;

        auto columns_ast = make_intrusive<ASTExpressionList>();
        for (const auto & name : columns)
            columns_ast->children.emplace_back(make_intrusive<ASTIdentifier>(name));
        insert->columns = columns_ast;
        return insert;
    }

    Block makeAcknowledgementBlock(const Block & block)
    {
        Block acknowledgement;
        acknowledgement.insert(block.getByName(String(ID_COLUMN)));
        acknowledgement.insert(ColumnWithTypeAndName{
            ColumnUInt64::create(block.rows(), 2),
            block.getByName(String(VERSION_COLUMN)).type,
            String(VERSION_COLUMN)});
        acknowledgement.insert(ColumnWithTypeAndName{
            ColumnUInt8::create(block.rows(), 1),
            block.getByName(String(IS_DELETED_COLUMN)).type,
            String(IS_DELETED_COLUMN)});
        acknowledgement.insert(block.getByName(String(CREATED_AT_COLUMN)));
        return acknowledgement;
    }

    struct QueueReadState
    {
        explicit QueueReadState(std::shared_mutex & consumer_groups_mutex)
            : consumer_groups_lock(consumer_groups_mutex)
        {
        }

        void addAcknowledgementBlock(Block block)
        {
            std::lock_guard lock(mutex);
            acknowledgement_blocks.emplace_back(std::move(block));
        }

        Blocks takeAcknowledgementBlocks()
        {
            std::lock_guard lock(mutex);
            Blocks result;
            result.swap(acknowledgement_blocks);
            return result;
        }

        std::shared_lock<std::shared_mutex> consumer_groups_lock;
        std::mutex mutex;
        Blocks acknowledgement_blocks;
    };

    Block removeAcknowledgementColumns(Block header)
    {
        for (const auto column_name : {ID_COLUMN, VERSION_COLUMN, IS_DELETED_COLUMN, CREATED_AT_COLUMN})
        {
            if (header.has(String(column_name)))
                header.erase(String(column_name));
        }
        return header;
    }

    class QueueAcknowledgementTransform final : public ISimpleTransform
    {
    public:
        QueueAcknowledgementTransform(SharedHeader input_header_, std::shared_ptr<QueueReadState> state_)
            : ISimpleTransform(input_header_, std::make_shared<const Block>(removeAcknowledgementColumns(*input_header_)), false)
            , input_header(std::move(input_header_))
            , state(std::move(state_))
        {
        }

        String getName() const override { return "QueueAcknowledgementTransform"; }

    private:
        void transform(Chunk & chunk) override
        {
            const size_t rows = chunk.getNumRows();
            Block block = input_header->cloneWithColumns(chunk.detachColumns());
            state->addAcknowledgementBlock(makeAcknowledgementBlock(block));

            Columns output_columns;
            const auto & output_header = getOutputPort().getHeader();
            output_columns.reserve(output_header.columns());
            for (const auto & column : output_header)
                output_columns.emplace_back(block.getByName(column.name).column);
            chunk.setColumns(std::move(output_columns), rows);
        }

        SharedHeader input_header;
        std::shared_ptr<QueueReadState> state;
    };

    class QueueAcknowledgementStep final : public ITransformingStep
    {
    public:
        QueueAcknowledgementStep(SharedHeader input_header_, std::shared_ptr<QueueReadState> state_)
            : ITransformingStep(
                input_header_,
                std::make_shared<const Block>(removeAcknowledgementColumns(*input_header_)),
                getTraits())
            , state(std::move(state_))
        {
        }

        String getName() const override { return "QueueAcknowledgement"; }

        void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
        {
            pipeline.addSimpleTransform(
                [state = state](const SharedHeader & header)
                {
                    return std::make_shared<QueueAcknowledgementTransform>(header, state);
                });
        }

    private:
        static Traits getTraits()
        {
            return Traits{
                {
                    .returns_single_stream = false,
                    .preserves_number_of_streams = true,
                    .preserves_sorting = true,
                },
                {
                    .preserves_number_of_rows = true,
                }};
        }

        void updateOutputHeader() override
        {
            output_header = std::make_shared<const Block>(removeAcknowledgementColumns(*input_headers.front()));
        }

        std::shared_ptr<QueueReadState> state;
    };

    class QueueSource final : public ISource, WithContext
    {
    public:
        QueueSource(
            SharedHeader header_,
            Names output_columns_,
            const StorageID & consumer_table_id_,
            ContextPtr context_,
            std::shared_ptr<QueueReadState> state_)
            : ISource(std::move(header_))
            , WithContext(context_)
            , output_columns(std::move(output_columns_))
            , consumer_table_id(consumer_table_id_)
            , state(std::move(state_))
        {
        }

        ~QueueSource() override
        {
            if (select_io && !select_finished)
            {
                if (reader)
                {
                    reader->cancel();
                    reader.reset();
                }
                select_io->onCancelOrConnectionLoss();
            }
        }

        String getName() const override { return "QueueSource"; }

    protected:
        Chunk generate() override
        {
            try
            {
                initialize();

                Block block;
                if (!reader->pull(block))
                {
                    reader.reset();
                    select_io->onFinish();
                    select_finished = true;
                    return {};
                }

                if (!block.rows())
                    return {};

                Columns output;
                output.reserve(output_columns.size());
                for (const auto & name : output_columns)
                    output.emplace_back(block.getByName(name).column);
                return Chunk(std::move(output), block.rows());
            }
            catch (...)
            {
                if (select_io && !select_finished)
                {
                    if (reader)
                    {
                        reader->cancel();
                        reader.reset();
                    }
                    select_io->onException();
                    select_finished = true;
                }
                throw;
            }
        }

    private:
        void initialize()
        {
            if (reader)
                return;

            Names select_columns = output_columns;

            Strings quoted_columns;
            quoted_columns.reserve(select_columns.size());
            for (const auto & name : select_columns)
                quoted_columns.emplace_back(backQuoteIfNeed(name));

            const String select_query = fmt::format(
                "SELECT {} FROM {} FINAL ORDER BY (`{}`, `{}`)",
                fmt::join(quoted_columns, ", "),
                quoteTable(consumer_table_id),
                CREATED_AT_COLUMN,
                ID_COLUMN);

            auto select_context = Context::createCopy(getContext());
            select_io.emplace(executeQuery(select_query, select_context, QueryFlags{.internal = true}).second);
            reader = std::make_unique<PullingPipelineExecutor>(select_io->pipeline);
        }

        const Names output_columns;
        const StorageID consumer_table_id;
        const std::shared_ptr<QueueReadState> state;

        std::optional<BlockIO> select_io;
        std::unique_ptr<PullingPipelineExecutor> reader;
        bool select_finished = false;
    };

    class ReadFromQueue final : public ISourceStep
    {
    public:
        ReadFromQueue(
            SharedHeader header_,
            Names output_columns_,
            const StorageID & consumer_table_id_,
            ContextPtr context_,
            std::shared_ptr<QueueReadState> state_)
            : ISourceStep(std::move(header_))
            , output_columns(std::move(output_columns_))
            , consumer_table_id(consumer_table_id_)
            , context(context_)
            , state(std::move(state_))
        {
        }

        String getName() const override { return "ReadFromQueue"; }

        void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
        {
            pipeline.init(Pipe(std::make_shared<QueueSource>(
                getOutputHeader(),
                output_columns,
                consumer_table_id,
                context,
                state)));
        }

    private:
        const Names output_columns;
        const StorageID consumer_table_id;
        const ContextPtr context;
        const std::shared_ptr<QueueReadState> state;
    };

    class QueueSink final : public SinkToStorage, WithContext
    {
    public:
        QueueSink(
            const StorageID & main_table_id_,
            const Block & header_,
            ContextPtr context_,
            bool async_insert_,
            std::shared_mutex & consumer_groups_mutex_)
            : SinkToStorage(std::make_shared<const Block>(header_))
            , WithContext(context_)
            , main_table_id(main_table_id_)
            , async_insert(async_insert_)
            , consumer_groups_mutex(consumer_groups_mutex_)
        {
        }

        ~QueueSink() override
        {
            if (executor)
            {
                try
                {
                    executor->cancel();
                }
                catch (...)
                {
                    tryLogCurrentException("QueueSink");
                }
            }
        }

        String getName() const override { return "QueueSink"; }

        void onStart() override
        {
            consumer_groups_lock.emplace(consumer_groups_mutex);

            auto insert_context = Context::createCopy(getContext());
            insert_context->makeQueryContext();

            InterpreterInsertQuery interpreter(
                makeInsertQuery(main_table_id, getHeader().getNames()),
                insert_context,
                /* allow_materialized */ true,
                /* no_squash */ false,
                /* no_destination */ false,
                async_insert);

            block_io = interpreter.execute();
            executor = std::make_unique<PushingPipelineExecutor>(block_io.pipeline);
            executor->start();
        }

        void consume(Chunk & chunk) override
        {
            if (!chunk.getNumRows())
                return;
            executor->push(getHeader().cloneWithColumns(chunk.detachColumns()));
        }

        void onFinish() override
        {
            executor->finish();
            executor.reset();
            block_io.onFinish();
            block_io = {};
            consumer_groups_lock.reset();
        }

        void onException(std::exception_ptr) override
        {
            if (executor)
                executor->cancel();
            executor.reset();
            block_io.onException();
            block_io = {};
            consumer_groups_lock.reset();
        }

    private:
        const StorageID main_table_id;
        const bool async_insert;
        std::shared_mutex & consumer_groups_mutex;
        std::optional<std::shared_lock<std::shared_mutex>> consumer_groups_lock;
        BlockIO block_io;
        std::unique_ptr<PushingPipelineExecutor> executor;
    };
}


String StorageQueue::getMainTableName(const StorageID & queue_table_id)
{
    if (queue_table_id.hasUUID())
        return ".inner_id.queue." + toString(queue_table_id.uuid);
    return ".inner.queue." + queue_table_id.getTableName();
}


String StorageQueue::getConsumerTableName(const StorageID & queue_table_id, const String & consumer_group)
{
    return getMainTableName(queue_table_id) + ".group." + sipHash128String(consumer_group);
}


String StorageQueue::getConsumerViewName(const StorageID & queue_table_id, const String & consumer_group)
{
    return getMainTableName(queue_table_id) + ".group_view." + sipHash128String(consumer_group);
}


StorageID StorageQueue::createDataTable(
    const ASTCreateQuery & outer_query,
    const StorageID & table_id,
    ContextPtr local_context,
    LoadingStrictnessLevel mode,
    UInt64 retention_seconds,
    bool consumer_table)
{
    if (mode > LoadingStrictnessLevel::SECONDARY_CREATE)
        return table_id;

    const auto internal_definition = fmt::format(
        "CREATE TABLE queue_inner ("
        "`{}` UUID DEFAULT generateUUIDv7(), "
        "`{}` UInt64 DEFAULT 1, "
        "`{}` UInt8 DEFAULT 0, "
        "`{}` DateTime64(6) DEFAULT now64(6)) "
        "ENGINE = {} "
        "PARTITION BY toYYYYMM(`{}`) "
        "ORDER BY (`{}`, `{}`) "
        "TTL `{}` + toIntervalSecond({}) DELETE{}",
        ID_COLUMN,
        VERSION_COLUMN,
        IS_DELETED_COLUMN,
        CREATED_AT_COLUMN,
        consumer_table
            ? String(fmt::format("ReplacingMergeTree(`{}`, `{}`)", VERSION_COLUMN, IS_DELETED_COLUMN))
            : String("MergeTree"),
        CREATED_AT_COLUMN,
        CREATED_AT_COLUMN,
        ID_COLUMN,
        CREATED_AT_COLUMN,
        retention_seconds,
        consumer_table
            ? String(" SETTINGS clean_deleted_rows = 'Always', allow_experimental_replacing_merge_with_cleanup = 1")
            : String());

    auto inner_query = parseCreateQuery(internal_definition);
    auto & inner_create = inner_query->as<ASTCreateQuery &>();
    inner_create.setDatabase(table_id.getDatabaseName());
    inner_create.setTable(table_id.getTableName());

    const auto * outer_columns = outer_query.columns_list ? outer_query.columns_list->columns : nullptr;
    if (!outer_columns)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The `Queue` table engine requires an explicit column list");

    auto * inner_columns = inner_create.columns_list->columns;
    ASTs user_columns;
    user_columns.reserve(outer_columns->children.size());
    for (const auto & column : outer_columns->children)
        user_columns.emplace_back(column->clone());
    inner_columns->children.insert(inner_columns->children.begin(), user_columns.begin(), user_columns.end());

    auto create_context = Context::createCopy(local_context);
    InterpreterCreateQuery interpreter(inner_query, create_context);
    interpreter.setInternal(true);
    interpreter.execute();
    return table_id;
}


StorageQueue::StorageQueue(
    const StorageID & table_id_,
    ContextPtr context_,
    LoadingStrictnessLevel mode_,
    const ASTCreateQuery & query_,
    const ColumnsDescription & columns_,
    const String & comment_,
    UInt64 retention_seconds_,
    UInt64 max_batch_size_,
    UInt64 polling_interval_ms_)
    : IStreamingStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , outer_create_query(query_.clone())
    , main_table_id(createDataTable(
        query_,
        StorageID{table_id_.getDatabaseName(), getMainTableName(table_id_)},
        context_,
        mode_,
        retention_seconds_,
        /* consumer_table */ false))
    , retention_seconds(retention_seconds_)
    , max_batch_size(max_batch_size_)
    , polling_interval_ms(polling_interval_ms_)
    , log(getLogger("StorageQueue (" + table_id_.getFullTableName() + ")"))
{
    for (const auto internal_name : {ID_COLUMN, VERSION_COLUMN, IS_DELETED_COLUMN, CREATED_AT_COLUMN})
    {
        if (columns_.has(String(internal_name)))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column name `{}` is reserved by the `Queue` table engine", internal_name);
    }

    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns_);
    metadata.setComment(comment_);

    VirtualColumnsDescription virtuals;
    virtuals.addEphemeral(String(ID_COLUMN), std::make_shared<DataTypeUUID>(), "Queue message identifier", VirtualsMaterializationPlace::Reader);
    virtuals.addEphemeral(String(VERSION_COLUMN), std::make_shared<DataTypeUInt64>(), "Queue message state version", VirtualsMaterializationPlace::Reader);
    virtuals.addEphemeral(String(IS_DELETED_COLUMN), std::make_shared<DataTypeUInt8>(), "Queue message deletion marker", VirtualsMaterializationPlace::Reader);
    virtuals.addEphemeral(String(CREATED_AT_COLUMN), std::make_shared<DataTypeDateTime64>(6), "Queue enqueue timestamp", VirtualsMaterializationPlace::Reader);
    metadata.setVirtuals(std::move(virtuals));
    setInMemoryMetadata(metadata);

    streaming_task = getContext()->getSchedulePool().createTask(
        getStorageID(),
        log->name(),
        [this] { threadFunc(); });
    streaming_task->deactivate();
}


StorageQueue::~StorageQueue()
{
    if (!shutdown_called)
        shutdown(false);
}


StoragePtr StorageQueue::getInnerTable(ContextPtr local_context) const
{
    return DatabaseCatalog::instance().getTable(main_table_id, local_context);
}


std::pair<String, bool> StorageQueue::getConsumerSettingsForView(
    const StorageID & view_id,
    ContextPtr query_context) const
{
    auto view = DatabaseCatalog::instance().getTable(view_id, query_context);
    if (const auto * materialized_view = dynamic_cast<const StorageMaterializedView *>(view.get());
        materialized_view && materialized_view->isRefreshable())
        return {};

    auto metadata = view->getInMemoryMetadataPtr(query_context, false);
    auto view_context = Context::createCopy(query_context);
    view_context->setSetting("queue_consumer_group", String());
    view_context->setSetting("queue_consumer_offset", String("earliest"));
    InterpreterSetQuery::applySettingsFromQuery(metadata->getSelectQuery().select_query, view_context);

    String consumer_group = view_context->getSettingsRef()[Setting::queue_consumer_group];
    if (consumer_group.empty())
        consumer_group = view_id.getFullTableName();

    if (queryMayExcludeSourceRows(metadata->getSelectQuery().select_query))
    {
        throw Exception(
            ErrorCodes::QUERY_NOT_ALLOWED,
            "Materialized view '{}' cannot consume from `Queue` with joins, `DISTINCT`, or `LIMIT` "
            "until post-query message identity tracking is available; excluded messages must remain pending",
            view_id.getFullTableName());
    }

    return {
        std::move(consumer_group),
        startsAtLatest(view_context->getSettingsRef()[Setting::queue_consumer_offset])};
}


std::unordered_map<String, bool> StorageQueue::getConsumerGroups(
    const std::vector<StorageID> & view_ids,
    ContextPtr query_context) const
{
    std::unordered_map<String, bool> consumer_groups;
    for (const auto & view_id : view_ids)
    {
        auto [consumer_group, start_at_latest] = getConsumerSettingsForView(view_id, query_context);
        if (!consumer_group.empty())
        {
            const auto [it, inserted] = consumer_groups.emplace(std::move(consumer_group), start_at_latest);
            if (!inserted && it->second != start_at_latest)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Materialized views in consumer group '{}' specify different `queue_consumer_offset` values",
                    it->first);
            }
        }
    }
    return consumer_groups;
}


bool StorageQueue::shouldPushToMaterializedView(const StorageID & view_id, ContextPtr query_context) const
{
    const String requested_group = query_context->getSettingsRef()[Setting::queue_consumer_group];
    const String view_group = getConsumerSettingsForView(view_id, query_context).first;
    return !requested_group.empty() && !view_group.empty() && requested_group == view_group;
}


void StorageQueue::createConsumerView(
    const StorageID & consumer_table_id,
    const StorageID & consumer_view_id,
    ContextPtr query_context) const
{
    const String query = fmt::format(
        "CREATE MATERIALIZED VIEW {} TO {} AS SELECT * FROM {}",
        quoteTable(consumer_view_id),
        quoteTable(consumer_table_id),
        quoteTable(main_table_id));

    auto create_query = parseCreateQuery(query);
    auto create_context = Context::createCopy(query_context);
    InterpreterCreateQuery interpreter(create_query, create_context);
    interpreter.setInternal(true);
    interpreter.execute();
}


StorageID StorageQueue::ensureConsumerGroup(
    const String & consumer_group,
    bool start_at_latest)
{
    std::unique_lock lock(consumer_groups_mutex);
    auto internal_context = Context::createCopy(getContext());
    internal_context->makeQueryContext();

    const StorageID consumer_table_id{
        getStorageID().getDatabaseName(),
        getConsumerTableName(getStorageID(), consumer_group)};
    const StorageID consumer_view_id{
        getStorageID().getDatabaseName(),
        getConsumerViewName(getStorageID(), consumer_group)};

    auto & catalog = DatabaseCatalog::instance();
    const bool has_consumer_table = catalog.tryGetTable(consumer_table_id, internal_context) != nullptr;
    const bool has_consumer_view = catalog.tryGetTable(consumer_view_id, internal_context) != nullptr;

    if (!has_consumer_table)
    {
        createDataTable(
            outer_create_query->as<const ASTCreateQuery &>(),
            consumer_table_id,
            internal_context,
            LoadingStrictnessLevel::CREATE,
            retention_seconds,
            /* consumer_table */ true);
    }

    if (!has_consumer_view)
    {
        if (!start_at_latest)
        {
            const String populate_query = fmt::format(
                "INSERT INTO {} SELECT * FROM {}",
                quoteTable(consumer_table_id),
                quoteTable(main_table_id));
            auto populate_io = executeQuery(populate_query, internal_context, QueryFlags{.internal = true}).second;
            CompletedPipelineExecutor populate_executor(populate_io.pipeline);
            populate_executor.execute();
            populate_io.onFinish();
        }

        createConsumerView(consumer_table_id, consumer_view_id, internal_context);
    }

    return consumer_table_id;
}


StorageID StorageQueue::resetConsumerGroup(
    const String & consumer_group,
    bool start_at_latest)
{
    std::unique_lock lock(consumer_groups_mutex);
    auto internal_context = Context::createCopy(getContext());
    internal_context->makeQueryContext();

    const StorageID consumer_table_id{
        getStorageID().getDatabaseName(),
        getConsumerTableName(getStorageID(), consumer_group)};
    const StorageID consumer_view_id{
        getStorageID().getDatabaseName(),
        getConsumerViewName(getStorageID(), consumer_group)};

    dropInternalTableIfAny(consumer_view_id, /* sync */ true, internal_context);
    dropInternalTableIfAny(consumer_table_id, /* sync */ true, internal_context);
    createDataTable(
        outer_create_query->as<const ASTCreateQuery &>(),
        consumer_table_id,
        internal_context,
        LoadingStrictnessLevel::CREATE,
        retention_seconds,
        /* consumer_table */ true);

    if (!start_at_latest)
    {
        const String populate_query = fmt::format(
            "INSERT INTO {} SELECT * FROM {}",
            quoteTable(consumer_table_id),
            quoteTable(main_table_id));
        auto populate_io = executeQuery(populate_query, internal_context, QueryFlags{.internal = true}).second;
        CompletedPipelineExecutor populate_executor(populate_io.pipeline);
        populate_executor.execute();
        populate_io.onFinish();
    }

    createConsumerView(consumer_table_id, consumer_view_id, internal_context);
    return consumer_table_id;
}


void StorageQueue::startup()
{
    streaming_task->activateAndSchedule();
    StreamingStorageRegistry::instance().registerTable(getStorageID());
}


void StorageQueue::shutdown(bool)
{
    if (shutdown_called.exchange(true))
        return;

    streaming_task->deactivate();
    StreamingStorageRegistry::instance().unregisterTable(getStorageID(), /* if_exists */ true);
}


void StorageQueue::scheduleStreamingTasksImpl()
{
    streaming_task->schedule();
}


void StorageQueue::threadFunc()
{
    try
    {
        const UInt64 cycle_epoch = stream_control.currentCancelEpoch();
        const auto ready_views = DatabaseCatalog::instance().getReadyDependentViews(getStorageID(), getContext());

        if (!shutdown_called
            && !ready_views.empty()
            && stream_control.claimCycle(last_seen_refresh_epoch))
        {
            for (const auto & [consumer_group, start_at_latest] : getConsumerGroups(ready_views, getContext()))
            {
                try
                {
                    const auto consumer_table_id = ensureConsumerGroup(
                        consumer_group,
                        start_at_latest);
                    while (!shutdown_called
                        && !stream_control.isCancelRequested(cycle_epoch)
                        && streamToViews(consumer_group, consumer_table_id, cycle_epoch))
                    {
                        if (stream_control.isBlocked())
                            break;
                    }
                }
                catch (...)
                {
                    tryLogCurrentException(
                        log,
                        fmt::format("Failed to consume from `Queue` consumer group '{}'", consumer_group));
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to consume from `Queue`");
    }

    if (!shutdown_called)
        streaming_task->scheduleAfter(polling_interval_ms);
}


struct StorageQueue::ViewAcknowledgementState
{
    explicit ViewAcknowledgementState(size_t max_batch_size_)
        : max_batch_size(max_batch_size_)
    {
    }

    static Block filterBlock(const Block & block, const IColumn::Filter & filter, size_t result_size)
    {
        Block result;
        for (const auto & column : block)
        {
            result.insert(ColumnWithTypeAndName{
                column.column->filter(filter, result_size),
                column.type,
                column.name});
        }
        return result;
    }

    void filterAndAdd(Block & block)
    {
        std::lock_guard lock(mutex);

        const size_t rows = block.rows();
        if (!rows)
            return;

        const auto & ids = assert_cast<const ColumnUUID &>(
            *block.getByName(String(ID_COLUMN)).column).getData();

        IColumn::Filter output_filter(rows, 0);
        IColumn::Filter acknowledgement_filter(rows, 0);
        size_t output_rows = 0;
        size_t acknowledgement_rows = 0;

        for (size_t row = 0; row < rows; ++row)
        {
            const auto & id = ids[row];
            const bool already_selected = selected_message_ids.contains(id);
            if (!already_selected && selected_message_ids.size() >= max_batch_size)
                continue;

            output_filter[row] = 1;
            ++output_rows;

            if (!already_selected)
            {
                selected_message_ids.emplace(id);
                acknowledgement_filter[row] = 1;
                ++acknowledgement_rows;
            }
        }

        if (acknowledgement_rows)
        {
            acknowledgement_blocks.emplace_back(makeAcknowledgementBlock(
                filterBlock(block, acknowledgement_filter, acknowledgement_rows)));
        }

        if (output_rows != rows)
            block = filterBlock(block, output_filter, output_rows);
    }

    bool isFull()
    {
        std::lock_guard lock(mutex);
        return selected_message_ids.size() >= max_batch_size;
    }

    Blocks take()
    {
        std::lock_guard lock(mutex);
        Blocks result;
        result.swap(acknowledgement_blocks);
        return result;
    }

    const size_t max_batch_size;
    std::mutex mutex;
    std::unordered_set<UUID> selected_message_ids;
    Blocks acknowledgement_blocks;
};


Names StorageQueue::getMaterializedViewSourceTrackingColumns(ContextPtr query_context) const
{
    if (query_context->getSettingsRef()[Setting::queue_consumer_group].value.empty())
        return {};

    std::lock_guard lock(view_acknowledgement_mutex);
    if (!view_acknowledgement_state)
        return {};

    return {
        String(ID_COLUMN),
        String(VERSION_COLUMN),
        String(IS_DELETED_COLUMN),
        String(CREATED_AT_COLUMN)};
}


void StorageQueue::trackMaterializedViewSourceRows(
    const StorageID &,
    Block & block,
    ContextPtr query_context)
{
    if (query_context->getSettingsRef()[Setting::queue_consumer_group].value.empty() || !block.rows())
        return;

    std::shared_ptr<ViewAcknowledgementState> state;
    {
        std::lock_guard lock(view_acknowledgement_mutex);
        state = view_acknowledgement_state;
    }

    if (state)
        state->filterAndAdd(block);
}


bool StorageQueue::streamToViews(
    const String & consumer_group,
    const StorageID & consumer_table_id,
    UInt64 cycle_epoch)
{
    std::lock_guard lock(consume_mutex);
    std::shared_lock consumer_groups_lock(consumer_groups_mutex);

    auto queue_context = Context::createCopy(getContext());
    queue_context->makeQueryContext();
    queue_context->setSetting("queue_consumer_group", consumer_group);

    auto view_state = std::make_shared<ViewAcknowledgementState>(max_batch_size);
    {
        std::lock_guard state_lock(view_acknowledgement_mutex);
        view_acknowledgement_state = view_state;
    }
    SCOPE_EXIT({
        std::lock_guard state_lock(view_acknowledgement_mutex);
        if (view_acknowledgement_state == view_state)
            view_acknowledgement_state.reset();
    });

    Names view_input_columns = getInMemoryMetadataPtr(queue_context, false)->getSampleBlock().getNames();
    view_input_columns.emplace_back(ID_COLUMN);
    view_input_columns.emplace_back(VERSION_COLUMN);
    view_input_columns.emplace_back(IS_DELETED_COLUMN);
    view_input_columns.emplace_back(CREATED_AT_COLUMN);

    auto insert = makeInsertQuery(getStorageID(), view_input_columns);
    InterpreterInsertQuery view_interpreter(
        insert,
        queue_context,
        /* allow_materialized */ false,
        /* no_squash */ true,
        /* no_destination */ true,
        /* async_insert */ false);
    auto view_io = view_interpreter.execute();
    const Names delivery_columns = view_io.pipeline.getHeader().getNames();

    Names select_columns = delivery_columns;

    Strings quoted_columns;
    quoted_columns.reserve(select_columns.size());
    for (const auto & name : select_columns)
        quoted_columns.emplace_back(backQuoteIfNeed(name));

    const String select_query = fmt::format(
        "SELECT {} FROM {} FINAL ORDER BY (`{}`, `{}`)",
        fmt::join(quoted_columns, ", "),
        quoteTable(consumer_table_id),
        CREATED_AT_COLUMN,
        ID_COLUMN);

    auto select_io = executeQuery(select_query, queue_context, QueryFlags{.internal = true}).second;
    PullingPipelineExecutor reader(select_io.pipeline);
    PushingPipelineExecutor writer(view_io.pipeline);
    writer.start();

    size_t rows = 0;
    bool source_exhausted = true;

    bool view_query_finished = false;
    bool select_query_finished = false;
    try
    {
        Block block;
        while (reader.pull(block))
        {
            if (!block.rows())
                continue;

            Block payload;
            for (const auto & name : delivery_columns)
                payload.insert(block.getByName(name));
            writer.push(std::move(payload));

            rows += block.rows();
            if (view_state->isFull())
            {
                source_exhausted = false;
                break;
            }
        }

        writer.finish();
        view_io.onFinish();
        view_query_finished = true;

        if (source_exhausted)
        {
            select_io.onFinish();
        }
        else
        {
            reader.cancel();
            select_io.onCancelOrConnectionLoss();
        }
        select_query_finished = true;

        if (stream_control.isCancelRequested(cycle_epoch))
            return false;

        auto acknowledgement_blocks = view_state->take();
        if (acknowledgement_blocks.empty())
            return false;

        acknowledge(consumer_table_id, acknowledgement_blocks, queue_context);
    }
    catch (...)
    {
        writer.cancel();
        if (!view_query_finished)
            view_io.onException();
        if (!select_query_finished)
            select_io.onException();
        throw;
    }

    LOG_DEBUG(log, "Processed {} queued rows for consumer group '{}' and acknowledged only rows selected by its views", rows, consumer_group);
    return true;
}


void StorageQueue::acknowledge(
    const StorageID & consumer_table_id,
    const Blocks & blocks,
    ContextMutablePtr queue_context)
{
    const Names columns{
        String(ID_COLUMN),
        String(VERSION_COLUMN),
        String(IS_DELETED_COLUMN),
        String(CREATED_AT_COLUMN)};

    InterpreterInsertQuery interpreter(
        makeInsertQuery(consumer_table_id, columns),
        queue_context,
        /* allow_materialized */ true,
        /* no_squash */ true,
        /* no_destination */ false,
        /* async_insert */ false);
    auto block_io = interpreter.execute();
    PushingPipelineExecutor executor(block_io.pipeline);
    executor.start();

    try
    {
        for (const auto & block : blocks)
            executor.push(block);
        executor.finish();
        block_io.onFinish();
    }
    catch (...)
    {
        executor.cancel();
        block_io.onException();
        throw;
    }
}


void StorageQueue::addPostFilterStep(QueryPlan & query_plan, ContextPtr query_context)
{
    std::function<void(QueryPlan &)> callback;
    {
        std::lock_guard lock(post_filter_steps_mutex);
        auto it = post_filter_steps.find(query_context->getCurrentQueryId());
        if (it == post_filter_steps.end())
            return;
        callback = std::move(it->second);
        post_filter_steps.erase(it);
    }
    callback(query_plan);
}


void StorageQueue::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    const auto & settings = local_context->getSettingsRef();
    const String consumer_group = settings[Setting::queue_consumer_group];
    const bool commit_on_select = settings[Setting::queue_commit_on_select];
    const bool reset_consumer_offset = settings[Setting::queue_reset_consumer_offset];

    if (reset_consumer_offset && consumer_group.empty())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Setting `queue_consumer_group` must be specified when `queue_reset_consumer_offset` is enabled");
    }

    if (reset_consumer_offset && commit_on_select)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Settings `queue_reset_consumer_offset` and `queue_commit_on_select` cannot be enabled together");
    }

    if (commit_on_select)
    {
        if (consumer_group.empty())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Setting `queue_consumer_group` must be specified when `queue_commit_on_select` is enabled");
        }

        if (query_info.need_aggregate || query_info.has_aggregates)
        {
            throw Exception(
                ErrorCodes::QUERY_NOT_ALLOWED,
                "Aggregation is not allowed for a committing `SELECT` from `Queue`; "
                "set `queue_commit_on_select = 0` to run a noncommitting aggregation");
        }

        if (queryMayExcludeSourceRows(query_info.query))
        {
            throw Exception(
                ErrorCodes::QUERY_NOT_ALLOWED,
                "A committing `SELECT` from `Queue` cannot use joins, `DISTINCT`, or `LIMIT` "
                "until post-query message identity tracking is available; "
                "set `queue_commit_on_select = 0` to run the query without acknowledging messages");
        }
    }

    std::optional<StorageID> consumer_table_id;
    if (!consumer_group.empty())
    {
        const bool start_at_latest = startsAtLatest(settings[Setting::queue_consumer_offset]);
        consumer_table_id = reset_consumer_offset
            ? resetConsumerGroup(consumer_group, start_at_latest)
            : ensureConsumerGroup(consumer_group, start_at_latest);
    }

    if (commit_on_select)
    {
        const UInt64 requested_batch_size = settings[Setting::queue_max_batch_size];
        const UInt64 batch_size = requested_batch_size ? requested_batch_size : max_batch_size;
        auto state = std::make_shared<QueueReadState>(consumer_groups_mutex);
        auto storage = std::static_pointer_cast<StorageQueue>(shared_from_this());

        Names read_columns = column_names;
        for (const auto internal_name : {ID_COLUMN, VERSION_COLUMN, IS_DELETED_COLUMN, CREATED_AT_COLUMN})
        {
            if (std::find(read_columns.begin(), read_columns.end(), internal_name) == read_columns.end())
                read_columns.emplace_back(internal_name);
        }

        {
            std::lock_guard lock(post_filter_steps_mutex);
            const auto [_, inserted] = post_filter_steps.emplace(
                local_context->getCurrentQueryId(),
                [state, batch_size](QueryPlan & plan)
                {
                    plan.addStep(std::make_unique<LimitStep>(plan.getCurrentHeader(), batch_size, 0));
                    plan.addStep(std::make_unique<QueueAcknowledgementStep>(plan.getCurrentHeader(), state));
                });
            if (!inserted)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "A post-filter Queue step is already registered for this query");
        }

        local_context->addSuccessfulQueryCallback(
            [storage, state, consumer_table_id = *consumer_table_id]
            {
                auto acknowledgement_blocks = state->takeAcknowledgementBlocks();
                if (acknowledgement_blocks.empty())
                    return;

                auto acknowledge_context = Context::createCopy(storage->getContext());
                acknowledge_context->makeQueryContext();
                storage->acknowledge(consumer_table_id, acknowledgement_blocks, acknowledge_context);
            });

        query_info.optimize_trivial_count = false;
        query_plan.addStep(std::make_unique<ReadFromQueue>(
            std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(read_columns)),
            read_columns,
            *consumer_table_id,
            local_context,
            std::move(state)));
        return;
    }

    const StorageID source_table_id = consumer_table_id.value_or(main_table_id);
    auto source_table = DatabaseCatalog::instance().getTable(source_table_id, local_context);
    auto source_lock = source_table->lockForShare(
        local_context->getCurrentQueryId(),
        local_context->getSettingsRef()[Setting::lock_acquire_timeout]);
    auto source_metadata = source_table->getInMemoryMetadataPtr(local_context, false);
    auto source_snapshot = source_table->getStorageSnapshot(source_metadata, local_context);
    auto source_query_info = query_info;
    source_query_info.initial_storage_snapshot = storage_snapshot;

    if (!consumer_group.empty())
    {
        source_query_info.query = query_info.query->clone();
        source_query_info.query->as<ASTSelectQuery &>().setFinal();
        if (source_query_info.query_tree)
        {
            if (!source_query_info.table_expression_modifiers)
                source_query_info.table_expression_modifiers.emplace();
            source_query_info.table_expression_modifiers->setHasFinal(true);
        }
    }

    source_table->read(
        query_plan,
        column_names,
        source_snapshot,
        source_query_info,
        local_context,
        processed_stage,
        max_block_size,
        num_streams);
    query_plan.addStorageHolder(source_table);
    query_plan.addTableLock(std::move(source_lock));
}


SinkToStoragePtr StorageQueue::write(
    const ASTPtr &,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context,
    bool async_insert)
{
    const Block header = metadata_snapshot->getSampleBlockNonMaterialized();
    return std::make_shared<QueueSink>(
        main_table_id,
        header,
        local_context,
        async_insert,
        consumer_groups_mutex);
}


void StorageQueue::drop()
{
    dropInnerTableIfAny(/* sync */ false, getContext());
}


void StorageQueue::dropInnerTableIfAny(bool sync, ContextPtr local_context)
{
    std::unique_lock lock(consumer_groups_mutex);

    for (const auto & table_id : getInternalTables(local_context, getMainTableName(getStorageID()) + ".group_view."))
        dropInternalTableIfAny(table_id, sync, local_context);
    for (const auto & table_id : getInternalTables(local_context, getMainTableName(getStorageID()) + ".group."))
        dropInternalTableIfAny(table_id, sync, local_context);
    dropInternalTableIfAny(main_table_id, sync, local_context);
}


std::vector<StorageID> StorageQueue::getInternalTables(
    ContextPtr query_context,
    std::string_view name_prefix) const
{
    std::vector<StorageID> result;
    auto database = DatabaseCatalog::instance().getDatabase(getStorageID().getDatabaseName());
    for (auto iterator = database->getTablesIterator(query_context); iterator->isValid(); iterator->next())
    {
        if (iterator->name().starts_with(name_prefix))
            result.emplace_back(getStorageID().getDatabaseName(), iterator->name());
    }
    return result;
}


void StorageQueue::dropInternalTableIfAny(
    const StorageID & table_id,
    bool sync,
    ContextPtr query_context) const
{
    if (!DatabaseCatalog::instance().tryGetTable(table_id, query_context))
        return;

    const bool may_lock_ddl_guard = getStorageID().getQualifiedName() < table_id.getQualifiedName();
    InterpreterDropQuery::executeDropQuery(
        ASTDropQuery::Kind::Drop,
        getContext(),
        query_context,
        table_id,
        sync,
        /* ignore_sync_setting */ true,
        may_lock_ddl_guard);
}


void StorageQueue::checkTableSizeBelowDropLimit(ContextPtr query_context) const
{
    if (auto main = DatabaseCatalog::instance().tryGetTable(main_table_id, query_context))
        main->checkTableSizeBelowDropLimit(query_context);

    for (const auto & table_id : getInternalTables(query_context, getMainTableName(getStorageID()) + ".group."))
    {
        if (auto table = DatabaseCatalog::instance().tryGetTable(table_id, query_context))
            table->checkTableSizeBelowDropLimit(query_context);
    }
}


void StorageQueue::truncate(
    const ASTPtr &,
    const StorageMetadataPtr &,
    ContextPtr local_context,
    TableExclusiveLockHolder &)
{
    std::unique_lock lock(consumer_groups_mutex);

    InterpreterDropQuery::executeDropQuery(
        ASTDropQuery::Kind::Truncate,
        getContext(),
        local_context,
        main_table_id,
        /* sync */ true);

    for (const auto & table_id : getInternalTables(local_context, getMainTableName(getStorageID()) + ".group."))
    {
        InterpreterDropQuery::executeDropQuery(
            ASTDropQuery::Kind::Truncate,
            getContext(),
            local_context,
            table_id,
            /* sync */ true);
    }
}


std::optional<UInt64> StorageQueue::totalRows(ContextPtr query_context) const
{
    return getInnerTable(query_context)->totalRows(query_context);
}


std::optional<UInt64> StorageQueue::totalBytes(ContextPtr query_context) const
{
    return getInnerTable(query_context)->totalBytes(query_context);
}


Strings StorageQueue::getDataPaths() const
{
    return getInnerTable(getContext())->getDataPaths();
}


void registerStorageQueue(StorageFactory & factory)
{
    factory.registerStorage(
        "Queue",
        [](const StorageFactory::Arguments & args)
        {
            if (args.mode <= LoadingStrictnessLevel::CREATE
                && !args.getLocalContext()->getSettingsRef()[Setting::allow_experimental_queue_table_engine])
            {
                throw Exception(
                    ErrorCodes::UNKNOWN_STORAGE,
                    "Table engine `Queue` is experimental. "
                    "Set `allow_experimental_queue_table_engine` to enable it");
            }

            if (args.engine_args.size() > 3)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The `Queue` table engine accepts up to three arguments: "
                    "retention_seconds, max_batch_size, polling_interval_ms");

            for (const auto internal_name : {ID_COLUMN, VERSION_COLUMN, IS_DELETED_COLUMN, CREATED_AT_COLUMN})
            {
                if (args.columns.has(String(internal_name)))
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Column name `{}` is reserved by the `Queue` table engine",
                        internal_name);
                }
            }

            UInt64 retention_seconds = DEFAULT_RETENTION_SECONDS;
            UInt64 max_batch_size = DEFAULT_MAX_BATCH_SIZE;
            UInt64 polling_interval_ms = DEFAULT_POLLING_INTERVAL_MS;

            if (!args.engine_args.empty())
                retention_seconds = checkAndGetLiteralArgument<UInt64>(args.engine_args[0], "retention_seconds");
            if (args.engine_args.size() >= 2)
                max_batch_size = checkAndGetLiteralArgument<UInt64>(args.engine_args[1], "max_batch_size");
            if (args.engine_args.size() >= 3)
                polling_interval_ms = checkAndGetLiteralArgument<UInt64>(args.engine_args[2], "polling_interval_ms");

            if (!retention_seconds || !max_batch_size || !polling_interval_ms)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "All `Queue` engine arguments must be greater than zero");

            return std::make_shared<StorageQueue>(
                args.table_id,
                args.getContext(),
                args.mode,
                args.query,
                args.columns,
                args.comment,
                retention_seconds,
                max_batch_size,
                polling_interval_ms);
        },
        {},
        Documentation{
            .description = "Experimental native persistent queue backed by an immutable `MergeTree` log and "
                "a `ReplacingMergeTree` pending-message table for each consumer group. Rows are acknowledged "
                "only in the consumer group's table after delivery succeeds.",
            .syntax = "ENGINE = Queue([retention_seconds[, max_batch_size[, polling_interval_ms]]])"});
}

}
