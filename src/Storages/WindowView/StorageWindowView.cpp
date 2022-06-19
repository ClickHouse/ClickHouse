#include <numeric>
#include <regex>

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsTimeWindow.h>
#include <Interpreters/addMissingDefaults.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/QueryNormalizer.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTWatchQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Sources/BlocksSource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Transforms/WatermarkTransform.h>
#include <Processors/Transforms/SquashingChunksTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Sinks/EmptySink.h>
#include <Storages/AlterCommands.h>
#include <Storages/StorageFactory.h>
#include <Common/typeid_cast.h>
#include <Common/ProfileEvents.h>
#include <base/sleep.h>
#include <Common/logger_useful.h>

#include <Storages/LiveView/StorageBlocks.h>

#include <Storages/WindowView/StorageWindowView.h>
#include <Storages/WindowView/WindowViewSource.h>

#include <QueryPipeline/printPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
    extern const int SYNTAX_ERROR;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TABLE_WAS_NOT_DROPPED;
    extern const int NOT_IMPLEMENTED;
}

namespace
{
    /// Fetch all window info and replace tumble or hop node names with windowID
    struct WindowFunctionMatcher
    {
        using Visitor = InDepthNodeVisitor<WindowFunctionMatcher, true>;
        using TypeToVisit = ASTFunction;

        struct Data
        {
            ASTPtr window_function;
            String serialized_window_function;
            bool is_tumble = false;
            bool is_hop = false;
            bool check_duplicate_window = false;
        };

        static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

        static void visit(ASTPtr & ast, Data & data)
        {
            if (auto * t = ast->as<ASTFunction>())
            {
                if (t->name == "tumble" || t->name == "hop")
                {
                    data.is_tumble = t->name == "tumble";
                    data.is_hop = t->name == "hop";
                    auto temp_node = t->clone();
                    temp_node->setAlias("");
                    if (!data.window_function)
                    {
                        if (data.check_duplicate_window)
                            data.serialized_window_function = serializeAST(*temp_node);
                        t->name = "windowID";
                        data.window_function = t->clone();
                        data.window_function->setAlias("");
                    }
                    else
                    {
                        if (data.check_duplicate_window && serializeAST(*temp_node) != data.serialized_window_function)
                            throw Exception(
                                "WINDOW VIEW only support ONE TIME WINDOW FUNCTION", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW);
                        t->name = "windowID";
                    }
                }
            }
        }
    };

    /// Replace windowID node name with either tumble or hop
    struct ReplaceWindowIdMatcher
    {
    public:
        using Visitor = InDepthNodeVisitor<ReplaceWindowIdMatcher, true>;
        struct Data
        {
            String window_name;
        };

        static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

        static void visit(ASTPtr & ast, Data & data)
        {
            if (auto * t = ast->as<ASTFunction>())
            {
                if (t->name == "windowID")
                    t->name = data.window_name;
            }
        }
    };

    /// GROUP BY tumble(now(), INTERVAL '5' SECOND)
    /// will become
    /// GROUP BY tumble(____timestamp, INTERVAL '5' SECOND)
    struct ReplaceFunctionNowData
    {
        using TypeToVisit = ASTFunction;

        bool is_time_column_func_now = false;
        String window_id_name;
        String now_timezone;

        void visit(ASTFunction & node, ASTPtr & node_ptr)
        {
            if (node.name == "windowID" || node.name == "tumble" || node.name == "hop")
            {
                if (const auto * t = node.arguments->children[0]->as<ASTFunction>();
                    t && t->name == "now")
                {
                    if (!t->children.empty())
                    {
                        const auto & children = t->children[0]->as<ASTExpressionList>()->children;
                        if (!children.empty())
                        {
                            const auto * timezone_ast = children[0]->as<ASTLiteral>();
                            if (timezone_ast)
                                now_timezone = timezone_ast->value.safeGet<String>();
                        }
                    }
                    is_time_column_func_now = true;
                    node_ptr->children[0]->children[0] = std::make_shared<ASTIdentifier>("____timestamp");
                    window_id_name = node.getColumnName();
                }
            }
        }
    };

    using ReplaceFunctionNowVisitor = InDepthNodeVisitor<OneTypeMatcher<ReplaceFunctionNowData>, true>;

    class ToIdentifierMatcher
    {
    public:
        using Visitor = InDepthNodeVisitor<ToIdentifierMatcher, true>;

        struct Data
        {
            String window_id_name;
            String window_id_alias;
        };

        static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

        static void visit(ASTPtr & ast, Data & data)
        {
            if (const auto * t = ast->as<ASTFunction>())
                visit(*t, ast, data);
            if (const auto * t = ast->as<ASTIdentifier>())
                visit(*t, ast, data);
        }

    private:
        static void visit(const ASTFunction & node, ASTPtr & node_ptr, Data &)
        {
            if (node.name == "tuple")
            {
                /// tuple(windowID(timestamp, toIntervalSecond('5')))
                return;
            }
            else
            {
                /// windowID(timestamp, toIntervalSecond('5')) -> identifier.
                /// and other...
                node_ptr = std::make_shared<ASTIdentifier>(node.getColumnName());
            }
        }

        static void visit(const ASTIdentifier & node, ASTPtr & node_ptr, Data & data)
        {
            if (node.getColumnName() == data.window_id_alias)
            {
                if (auto identifier = std::dynamic_pointer_cast<ASTIdentifier>(node_ptr))
                    identifier->setShortName(data.window_id_name);
            }
        }
    };

    struct DropTableIdentifierMatcher
    {
        using Visitor = InDepthNodeVisitor<DropTableIdentifierMatcher, true>;

        struct Data{};

        static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

        static void visit(ASTPtr & ast, Data &)
        {
            if (auto * t = ast->as<ASTIdentifier>())
            {
                t->setShortName(t->shortName());
            }
        }
    };

    void extractWindowArgument(const ASTPtr & ast, IntervalKind::Kind & kind, Int64 & num_units, String err_msg)
    {
        const auto * arg = ast->as<ASTFunction>();
        if (!arg || !startsWith(arg->name, "toInterval")
        || !IntervalKind::tryParseString(Poco::toLower(arg->name.substr(10)), kind))
            throw Exception(err_msg, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const auto * interval_unit = arg->children.front()->children.front()->as<ASTLiteral>();
        if (!interval_unit
            || (interval_unit->value.getType() != Field::Types::String
                && interval_unit->value.getType() != Field::Types::UInt64))
            throw Exception("Interval argument must be integer", ErrorCodes::BAD_ARGUMENTS);

        if (interval_unit->value.getType() == Field::Types::String)
            num_units = parse<Int64>(interval_unit->value.safeGet<String>());
        else
            num_units = interval_unit->value.safeGet<UInt64>();

        if (num_units <= 0)
            throw Exception("Value for Interval argument must be positive.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

    UInt32 addTime(UInt32 time_sec, IntervalKind::Kind kind, Int64 num_units, const DateLUTImpl & time_zone)
    {
        switch (kind)
        {
            case IntervalKind::Nanosecond:
            case IntervalKind::Microsecond:
            case IntervalKind::Millisecond:
                throw Exception("Fractional seconds are not supported by windows yet", ErrorCodes::SYNTAX_ERROR);
#define CASE_WINDOW_KIND(KIND) \
    case IntervalKind::KIND: { \
        return AddTime<IntervalKind::KIND>::execute(time_sec, num_units, time_zone); \
    }
            CASE_WINDOW_KIND(Second)
            CASE_WINDOW_KIND(Minute)
            CASE_WINDOW_KIND(Hour)
            CASE_WINDOW_KIND(Day)
            CASE_WINDOW_KIND(Week)
            CASE_WINDOW_KIND(Month)
            CASE_WINDOW_KIND(Quarter)
            CASE_WINDOW_KIND(Year)
#undef CASE_WINDOW_KIND
        }
        __builtin_unreachable();
    }

    class AddingAggregatedChunkInfoTransform : public ISimpleTransform
    {
    public:
        explicit AddingAggregatedChunkInfoTransform(Block header) : ISimpleTransform(header, header, false) { }

        void transform(Chunk & chunk) override { chunk.setChunkInfo(std::make_shared<AggregatedChunkInfo>()); }

        String getName() const override { return "AddingAggregatedChunkInfoTransform"; }
    };

    String generateInnerTableName(const StorageID & storage_id)
    {
        if (storage_id.hasUUID())
            return ".inner." + toString(storage_id.uuid);
        return ".inner." + storage_id.getTableName();
    }

    String generateTargetTableName(const StorageID & storage_id)
    {
        if (storage_id.hasUUID())
            return ".inner.target." + toString(storage_id.uuid);
        return ".inner.target." + storage_id.table_name;
    }

    ASTPtr generateInnerFetchQuery(StorageID inner_table_id)
    {
        auto fetch_query = std::make_shared<ASTSelectQuery>();
        auto select = std::make_shared<ASTExpressionList>();
        select->children.push_back(std::make_shared<ASTAsterisk>());
        fetch_query->setExpression(ASTSelectQuery::Expression::SELECT, select);
        fetch_query->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
        auto tables_elem = std::make_shared<ASTTablesInSelectQueryElement>();
        auto table_expr = std::make_shared<ASTTableExpression>();
        fetch_query->tables()->children.push_back(tables_elem);
        tables_elem->table_expression = table_expr;
        tables_elem->children.push_back(table_expr);
        table_expr->database_and_table_name = std::make_shared<ASTTableIdentifier>(inner_table_id);
        table_expr->children.push_back(table_expr->database_and_table_name);
        return fetch_query;
    }
}

static void extractDependentTable(ContextPtr context, ASTPtr & query, String & select_database_name, String & select_table_name)
{
    ASTSelectQuery & select_query = typeid_cast<ASTSelectQuery &>(*query);

    auto db_and_table = getDatabaseAndTable(select_query, 0);
    ASTPtr subquery = extractTableExpression(select_query, 0);

    if (!db_and_table && !subquery)
        return;

    if (db_and_table)
    {
        select_table_name = db_and_table->table;

        if (db_and_table->database.empty())
        {
            db_and_table->database = select_database_name;
            AddDefaultDatabaseVisitor visitor(context, select_database_name);
            visitor.visit(select_query);
        }
        else
            select_database_name = db_and_table->database;
    }
    else if (auto * ast_select = subquery->as<ASTSelectWithUnionQuery>())
    {
        if (ast_select->list_of_selects->children.size() != 1)
            throw Exception("UNION is not supported for WINDOW VIEW", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW);

        auto & inner_select_query = ast_select->list_of_selects->children.at(0);

        extractDependentTable(context, inner_select_query, select_database_name, select_table_name);
    }
    else
        throw Exception(
            "Logical error while creating StorageWindowView."
            " Could not retrieve table name from select query.",
            DB::ErrorCodes::LOGICAL_ERROR);
}

UInt32 StorageWindowView::getCleanupBound()
{
    if (max_fired_watermark == 0)
        return 0;
    if (is_proctime)
        return max_fired_watermark;
    else
    {
        auto w_bound = max_fired_watermark;
        if (allowed_lateness)
            w_bound = addTime(w_bound, lateness_kind, -lateness_num_units, *time_zone);
        return getWindowLowerBound(w_bound);
    }
}

ASTPtr StorageWindowView::getCleanupQuery()
{
    ASTPtr function_less;
    function_less= makeASTFunction(
        "less",
        std::make_shared<ASTIdentifier>(window_id_name),
        std::make_shared<ASTLiteral>(getCleanupBound()));

    auto alter_query = std::make_shared<ASTAlterQuery>();
    alter_query->setDatabase(inner_table_id.database_name);
    alter_query->setTable(inner_table_id.table_name);
    alter_query->uuid = inner_table_id.uuid;
    alter_query->set(alter_query->command_list, std::make_shared<ASTExpressionList>());
    alter_query->alter_object = ASTAlterQuery::AlterObjectType::TABLE;

    auto alter_command = std::make_shared<ASTAlterCommand>();
    alter_command->type = ASTAlterCommand::DELETE;
    alter_command->predicate = function_less;
    alter_command->children.push_back(alter_command->predicate);
    alter_query->command_list->children.push_back(alter_command);
    return alter_query;
}

void StorageWindowView::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr local_context, TableExclusiveLockHolder &)
{
    InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Truncate, getContext(), local_context, inner_table_id, true);
}

bool StorageWindowView::optimize(
    const ASTPtr & query,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    ContextPtr local_context)
{
    auto storage_ptr = getInnerTable();
    auto metadata_snapshot = storage_ptr->getInMemoryMetadataPtr();
    return getInnerTable()->optimize(query, metadata_snapshot, partition, final, deduplicate, deduplicate_by_columns, local_context);
}

void StorageWindowView::alter(
    const AlterCommands & params,
    ContextPtr local_context,
    AlterLockHolder &)
{
    auto table_id = getStorageID();
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    StorageInMemoryMetadata old_metadata = getInMemoryMetadata();
    params.apply(new_metadata, local_context);

    const auto & new_select = new_metadata.select;
    const auto & new_select_query = new_metadata.select.inner_query;

    modifying_query = true;
    SCOPE_EXIT({
        modifying_query = false;
    });

    shutdown();

    auto inner_query = initInnerQuery(new_select_query->as<ASTSelectQuery &>(), local_context);

    input_header.clear();
    output_header.clear();

    InterpreterDropQuery::executeDropQuery(
    ASTDropQuery::Kind::Drop, getContext(), local_context, inner_table_id, true);

    /// create inner table
    auto create_context = Context::createCopy(local_context);
    auto inner_create_query = getInnerTableCreateQuery(inner_query, inner_table_id);
    InterpreterCreateQuery create_interpreter(inner_create_query, create_context);
    create_interpreter.setInternal(true);
    create_interpreter.execute();

    DatabaseCatalog::instance().addDependency(select_table_id, table_id);

    shutdown_called = false;

    clean_cache_task = getContext()->getSchedulePool().createTask(getStorageID().getFullTableName(), [this] { threadFuncCleanup(); });
    fire_task = getContext()->getSchedulePool().createTask(
        getStorageID().getFullTableName(), [this] { is_proctime ? threadFuncFireProc() : threadFuncFireEvent(); });
    clean_cache_task->deactivate();
    fire_task->deactivate();

    new_metadata.setSelectQuery(new_select);

    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(local_context, table_id, new_metadata);
    setInMemoryMetadata(new_metadata);

    startup();
}

void StorageWindowView::checkAlterIsPossible(const AlterCommands & commands, ContextPtr /*local_context*/) const
{
    for (const auto & command : commands)
    {
        if (!command.isCommentAlter() && command.type != AlterCommand::MODIFY_QUERY)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}", command.type, getName());
    }
}

std::pair<BlocksPtr, Block> StorageWindowView::getNewBlocks(UInt32 watermark)
{
    UInt32 w_start = addTime(watermark, window_kind, -window_num_units, *time_zone);

    auto inner_table = getInnerTable();
    InterpreterSelectQuery fetch(
        inner_fetch_query,
        getContext(),
        inner_table,
        inner_table->getInMemoryMetadataPtr(),
        SelectQueryOptions(QueryProcessingStage::FetchColumns));

    auto builder = fetch.buildQueryPipeline();

    ASTPtr filter_function;
    if (is_tumble)
    {
        /// SELECT * FROM inner_table WHERE window_id_name == w_end
        /// (because we fire at the end of windows)
        filter_function = makeASTFunction("equals", std::make_shared<ASTIdentifier>(window_id_name), std::make_shared<ASTLiteral>(watermark));
    }
    else
    {
        auto func_array = makeASTFunction("array");
        auto w_end = watermark;
        while (w_start < w_end)
        {
            /// slice_num_units = std::gcd(hop_num_units, window_num_units);
            /// We use std::gcd(hop_num_units, window_num_units) as the new window size
            /// to split the overlapped windows into non-overlapped.
            /// For a hopping window with window_size=3 slice=1, the windows might be
            /// [1,3],[2,4],[3,5], which will cause recomputation.
            /// In this case, the slice_num_units will be `gcd(1,3)=1' and the non-overlapped
            /// windows will split into [1], [2], [3]... We compute each split window into
            /// mergeable state and merge them when the window is triggering.
            func_array ->arguments->children.push_back(std::make_shared<ASTLiteral>(w_end));
            w_end = addTime(w_end, window_kind, -slice_num_units, *time_zone);
        }
        filter_function = makeASTFunction("has", func_array, std::make_shared<ASTIdentifier>(window_id_name));
    }

    auto syntax_result = TreeRewriter(getContext()).analyze(filter_function, builder.getHeader().getNamesAndTypesList());
    auto filter_expression = ExpressionAnalyzer(filter_function, syntax_result, getContext()).getActionsDAG(false);

    builder.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<FilterTransform>(
            header, std::make_shared<ExpressionActions>(filter_expression), filter_function->getColumnName(), true);
    });

    /// Adding window column
    DataTypes window_column_type{std::make_shared<DataTypeDateTime>(), std::make_shared<DataTypeDateTime>()};
    ColumnWithTypeAndName column;
    column.name = window_column_name;
    column.type = std::make_shared<DataTypeTuple>(std::move(window_column_type));
    column.column = column.type->createColumnConst(0, Tuple{w_start, watermark});
    auto adding_column_dag = ActionsDAG::makeAddingColumnActions(std::move(column));
    auto adding_column_actions
        = std::make_shared<ExpressionActions>(std::move(adding_column_dag), ExpressionActionsSettings::fromContext(getContext()));
    builder.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ExpressionTransform>(header, adding_column_actions);
    });

    /// Removing window id column
    auto new_header = builder.getHeader();
    new_header.erase(window_id_name);
    auto convert_actions_dag = ActionsDAG::makeConvertingActions(
        builder.getHeader().getColumnsWithTypeAndName(),
        new_header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name);
    auto actions = std::make_shared<ExpressionActions>(
        convert_actions_dag, ExpressionActionsSettings::fromContext(getContext(), CompileExpressions::yes));
    builder.addSimpleTransform([&](const Block & stream_header)
    {
        return std::make_shared<ExpressionTransform>(stream_header, actions);
    });

    builder.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<AddingAggregatedChunkInfoTransform>(header);
    });

    Pipes pipes;
    QueryPlanResourceHolder resources;
    auto pipe = QueryPipelineBuilder::getPipe(std::move(builder), resources);
    pipes.emplace_back(std::move(pipe));

    auto creator = [&](const StorageID & blocks_id_global)
    {
        auto source_table_metadata = getSourceTable()->getInMemoryMetadataPtr();
        auto required_columns = source_table_metadata->getColumns();
        required_columns.add(ColumnDescription("____timestamp", std::make_shared<DataTypeDateTime>()));
        return StorageBlocks::createStorage(blocks_id_global, required_columns, std::move(pipes), QueryProcessingStage::WithMergeableState);
    };

    TemporaryTableHolder blocks_storage(getContext(), creator);

    InterpreterSelectQuery select(
        getFinalQuery(),
        getContext(),
        blocks_storage.getTable(),
        blocks_storage.getTable()->getInMemoryMetadataPtr(),
        SelectQueryOptions(QueryProcessingStage::Complete));

    builder = select.buildQueryPipeline();

    builder.addSimpleTransform([&](const Block & current_header)
    {
        return std::make_shared<MaterializingTransform>(current_header);
    });
    builder.addSimpleTransform([&](const Block & current_header)
    {
        return std::make_shared<SquashingChunksTransform>(
            current_header,
            getContext()->getSettingsRef().min_insert_block_size_rows,
            getContext()->getSettingsRef().min_insert_block_size_bytes);
    });

    auto header = builder.getHeader();
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));

    PullingAsyncPipelineExecutor executor(pipeline);
    Block block;
    BlocksPtr new_blocks = std::make_shared<Blocks>();

    while (executor.pull(block))
    {
        if (block.rows() == 0)
            continue;
        new_blocks->push_back(std::move(block));
    }
    return std::make_pair(new_blocks, header);
}

inline void StorageWindowView::fire(UInt32 watermark)
{
    LOG_TRACE(log, "Watch streams number: {}, target table: {}",
              watch_streams.size(),
              target_table_id.empty() ? "None" : target_table_id.getNameForLogs());

    if (target_table_id.empty() && watch_streams.empty())
        return;

    BlocksPtr blocks;
    Block header;

    std::lock_guard lock(mutex);
    std::tie(blocks, header) = getNewBlocks(watermark);

    if (!blocks || blocks->empty())
        return;

    for (const auto & block : *blocks)
    {
        for (auto & watch_stream : watch_streams)
        {
            if (auto watch_stream_ptr = watch_stream.lock())
                watch_stream_ptr->addBlock(block, watermark);
        }
        fire_condition.notify_all();
    }

    if (!target_table_id.empty())
    {
        StoragePtr target_table = getTargetTable();
        auto insert = std::make_shared<ASTInsertQuery>();
        insert->table_id = target_table->getStorageID();
        InterpreterInsertQuery interpreter(insert, getContext());
        auto block_io = interpreter.execute();

        auto pipe = Pipe(std::make_shared<BlocksSource>(blocks, header));

        auto adding_missing_defaults_dag = addMissingDefaults(
            pipe.getHeader(),
            block_io.pipeline.getHeader().getNamesAndTypesList(),
            getTargetTable()->getInMemoryMetadataPtr()->getColumns(),
            getContext(),
            getContext()->getSettingsRef().insert_null_as_default);
        auto adding_missing_defaults_actions = std::make_shared<ExpressionActions>(adding_missing_defaults_dag);
        pipe.addSimpleTransform([&](const Block & stream_header)
        {
            return std::make_shared<ExpressionTransform>(stream_header, adding_missing_defaults_actions);
        });

        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            pipe.getHeader().getColumnsWithTypeAndName(),
            block_io.pipeline.getHeader().getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position);
        auto actions = std::make_shared<ExpressionActions>(
            convert_actions_dag,
            ExpressionActionsSettings::fromContext(getContext(), CompileExpressions::yes));
        pipe.addSimpleTransform([&](const Block & stream_header)
        {
            return std::make_shared<ExpressionTransform>(stream_header, actions);
        });

        block_io.pipeline.complete(std::move(pipe));
        CompletedPipelineExecutor executor(block_io.pipeline);
        executor.execute();
    }
}

ASTPtr StorageWindowView::getSourceTableSelectQuery()
{
    auto query = select_query->clone();
    auto & modified_select = query->as<ASTSelectQuery &>();

    if (hasJoin(modified_select))
    {
        auto analyzer_res = TreeRewriterResult({});
        removeJoin(modified_select, analyzer_res, getContext());
    }
    else
    {
        modified_select.setExpression(ASTSelectQuery::Expression::HAVING, {});
        modified_select.setExpression(ASTSelectQuery::Expression::GROUP_BY, {});
    }

    auto select_list = std::make_shared<ASTExpressionList>();
    for (const auto & column_name : getInputHeader().getNames())
        select_list->children.emplace_back(std::make_shared<ASTIdentifier>(column_name));
    modified_select.setExpression(ASTSelectQuery::Expression::SELECT, select_list);

    if (!is_time_column_func_now)
    {
        auto query = select_query->clone();
        DropTableIdentifierMatcher::Data drop_table_identifier_data;
        DropTableIdentifierMatcher::Visitor(drop_table_identifier_data).visit(query);

        WindowFunctionMatcher::Data query_info_data;
        WindowFunctionMatcher::Visitor(query_info_data).visit(query);

        auto order_by = std::make_shared<ASTExpressionList>();
        auto order_by_elem = std::make_shared<ASTOrderByElement>();
        order_by_elem->children.push_back(std::make_shared<ASTIdentifier>(timestamp_column_name));
        order_by_elem->direction = 1;
        order_by->children.push_back(order_by_elem);
        modified_select.setExpression(ASTSelectQuery::Expression::ORDER_BY, std::move(order_by));
    }
    else
        modified_select.setExpression(ASTSelectQuery::Expression::ORDER_BY, {});

    const auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();
    select_with_union_query->list_of_selects->children.push_back(query);

    return select_with_union_query;
}

ASTPtr StorageWindowView::getInnerTableCreateQuery(const ASTPtr & inner_query, const StorageID & inner_table_id)
{
    /// We will create a query to create an internal table.
    auto inner_create_query = std::make_shared<ASTCreateQuery>();
    inner_create_query->setDatabase(inner_table_id.getDatabaseName());
    inner_create_query->setTable(inner_table_id.getTableName());

    Aliases aliases;
    QueryAliasesVisitor(aliases).visit(inner_query);
    auto inner_query_normalized = inner_query->clone();
    QueryNormalizer::Data normalizer_data(aliases, {}, false, getContext()->getSettingsRef(), false);
    QueryNormalizer(normalizer_data).visit(inner_query_normalized);

    auto inner_select_query = std::static_pointer_cast<ASTSelectQuery>(inner_query_normalized);

    auto t_sample_block
        = InterpreterSelectQuery(inner_select_query, getContext(), SelectQueryOptions(QueryProcessingStage::WithMergeableState))
              .getSampleBlock();

    ASTPtr columns_list = InterpreterCreateQuery::formatColumns(t_sample_block.getNamesAndTypesList());

    if (is_time_column_func_now)
    {
        auto column_window = std::make_shared<ASTColumnDeclaration>();
        column_window->name = window_id_name;
        column_window->type = std::make_shared<ASTIdentifier>("UInt32");
        columns_list->children.push_back(column_window);
    }

    ToIdentifierMatcher::Data query_data;
    query_data.window_id_name = window_id_name;
    query_data.window_id_alias = window_id_alias;
    ToIdentifierMatcher::Visitor to_identifier_visitor(query_data);

    ReplaceFunctionNowData time_now_data;
    ReplaceFunctionNowVisitor time_now_visitor(time_now_data);
    WindowFunctionMatcher::Data window_data;
    WindowFunctionMatcher::Visitor window_visitor(window_data);

    DropTableIdentifierMatcher::Data drop_table_identifier_data;
    DropTableIdentifierMatcher::Visitor drop_table_identifier_visitor(drop_table_identifier_data);

    auto visit = [&](const IAST * ast)
    {
        auto node = ast->clone();
        QueryNormalizer(normalizer_data).visit(node);
        /// now() -> ____timestamp
        if (is_time_column_func_now)
        {
            time_now_visitor.visit(node);
            function_now_timezone = time_now_data.now_timezone;
        }
        drop_table_identifier_visitor.visit(node);
        /// tumble/hop -> windowID
        window_visitor.visit(node);
        to_identifier_visitor.visit(node);
        node->setAlias("");
        return node;
    };

    auto new_storage = std::make_shared<ASTStorage>();
    /// inner_storage_engine != nullptr in case create window view with ENGINE syntax
    if (inner_table_engine)
    {
        auto storage = inner_table_engine->as<ASTStorage &>();

        if (storage.ttl_table)
            throw Exception(
                ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW,
                "TTL is not supported for inner table in Window View");

        new_storage->set(new_storage->engine, storage.engine->clone());

        if (endsWith(storage.engine->name, "MergeTree"))
        {
            if (storage.partition_by)
                new_storage->set(new_storage->partition_by, visit(storage.partition_by));
            if (storage.primary_key)
                new_storage->set(new_storage->primary_key, visit(storage.primary_key));
            if (storage.order_by)
                new_storage->set(new_storage->order_by, visit(storage.order_by));
            if (storage.sample_by)
                new_storage->set(new_storage->sample_by, visit(storage.sample_by));

            if (storage.settings)
                new_storage->set(new_storage->settings, storage.settings->clone());
        }
    }
    else
    {
        new_storage->set(new_storage->engine, makeASTFunction("AggregatingMergeTree"));

        if (inner_select_query->groupBy()->children.size() == 1) //GROUP BY windowID
        {
            auto node = visit(inner_select_query->groupBy()->children[0].get());
            new_storage->set(new_storage->order_by, std::make_shared<ASTIdentifier>(node->getColumnName()));
        }
        else
        {
            auto group_by_function = makeASTFunction("tuple");
            for (auto & child : inner_select_query->groupBy()->children)
            {
                auto node = visit(child.get());
                group_by_function->arguments->children.push_back(std::make_shared<ASTIdentifier>(node->getColumnName()));
            }
            new_storage->set(new_storage->order_by, group_by_function);
        }
    }

    auto new_columns = std::make_shared<ASTColumns>();
    new_columns->set(new_columns->columns, columns_list);
    inner_create_query->set(inner_create_query->columns_list, new_columns);
    inner_create_query->set(inner_create_query->storage, new_storage);

    return inner_create_query;
}

UInt32 StorageWindowView::getWindowLowerBound(UInt32 time_sec)
{
    switch (slide_kind)
    {
        case IntervalKind::Nanosecond:
        case IntervalKind::Microsecond:
        case IntervalKind::Millisecond:
            throw Exception("Fractional seconds are not supported by windows yet", ErrorCodes::SYNTAX_ERROR);
#define CASE_WINDOW_KIND(KIND) \
    case IntervalKind::KIND: \
    { \
        if (is_tumble) \
            return ToStartOfTransform<IntervalKind::KIND>::execute(time_sec, window_num_units, *time_zone); \
        else \
        {\
            UInt32 w_start = ToStartOfTransform<IntervalKind::KIND>::execute(time_sec, hop_num_units, *time_zone); \
            UInt32 w_end = AddTime<IntervalKind::KIND>::execute(w_start, hop_num_units, *time_zone);\
            return AddTime<IntervalKind::KIND>::execute(w_end, -window_num_units, *time_zone);\
        }\
    }
        CASE_WINDOW_KIND(Second)
        CASE_WINDOW_KIND(Minute)
        CASE_WINDOW_KIND(Hour)
        CASE_WINDOW_KIND(Day)
        CASE_WINDOW_KIND(Week)
        CASE_WINDOW_KIND(Month)
        CASE_WINDOW_KIND(Quarter)
        CASE_WINDOW_KIND(Year)
#undef CASE_WINDOW_KIND
    }
    __builtin_unreachable();
}

UInt32 StorageWindowView::getWindowUpperBound(UInt32 time_sec)
{
    switch (slide_kind)
    {
        case IntervalKind::Nanosecond:
        case IntervalKind::Microsecond:
        case IntervalKind::Millisecond:
            throw Exception("Fractional seconds are not supported by window view yet", ErrorCodes::SYNTAX_ERROR);

#define CASE_WINDOW_KIND(KIND) \
    case IntervalKind::KIND: \
    { \
        UInt32 w_start = ToStartOfTransform<IntervalKind::KIND>::execute(time_sec, slide_num_units, *time_zone); \
        return AddTime<IntervalKind::KIND>::execute(w_start, slide_num_units, *time_zone); \
    }
        CASE_WINDOW_KIND(Second)
        CASE_WINDOW_KIND(Minute)
        CASE_WINDOW_KIND(Hour)
        CASE_WINDOW_KIND(Day)
        CASE_WINDOW_KIND(Week)
        CASE_WINDOW_KIND(Month)
        CASE_WINDOW_KIND(Quarter)
        CASE_WINDOW_KIND(Year)
#undef CASE_WINDOW_KIND
    }
    __builtin_unreachable();
}

void StorageWindowView::addFireSignal(std::set<UInt32> & signals)
{
    std::lock_guard lock(fire_signal_mutex);
    for (const auto & signal : signals)
        fire_signal.push_back(signal);
    fire_task->schedule();
}

void StorageWindowView::updateMaxTimestamp(UInt32 timestamp)
{
    std::lock_guard lock(fire_signal_mutex);
    if (timestamp > max_timestamp)
        max_timestamp = timestamp;
}

void StorageWindowView::updateMaxWatermark(UInt32 watermark)
{
    if (is_proctime)
    {
        max_watermark = watermark;
        return;
    }

    std::lock_guard lock(fire_signal_mutex);

    bool updated;
    if (is_watermark_strictly_ascending)
    {
        updated = max_watermark < watermark;
        while (max_watermark < watermark)
        {
            fire_signal.push_back(max_watermark);
            max_watermark = addTime(max_watermark, slide_kind, slide_num_units, *time_zone);
        }
    }
    else // strictly || bounded
    {
        UInt32 max_watermark_bias = addTime(max_watermark, watermark_kind, watermark_num_units, *time_zone);
        updated = max_watermark_bias <= watermark;
        while (max_watermark_bias <= max_timestamp)
        {
            fire_signal.push_back(max_watermark);
            max_watermark = addTime(max_watermark, slide_kind, slide_num_units, *time_zone);
            max_watermark_bias = addTime(max_watermark, slide_kind, slide_num_units, *time_zone);
        }
    }

    if (updated)
        fire_task->schedule();
}

void StorageWindowView::cleanup()
{
    std::lock_guard fire_signal_lock(fire_signal_mutex);
    std::lock_guard mutex_lock(mutex);

    auto alter_query = getCleanupQuery();
    auto cleanup_context = Context::createCopy(getContext());
    cleanup_context->makeQueryContext();
    cleanup_context->setCurrentQueryId("");
    cleanup_context->getClientInfo().is_replicated_database_internal = true;
    InterpreterAlterQuery interpreter_alter(alter_query, cleanup_context);
    interpreter_alter.execute();

    watch_streams.remove_if([](std::weak_ptr<WindowViewSource> & ptr) { return ptr.expired(); });
}

void StorageWindowView::threadFuncCleanup()
{
    if (shutdown_called)
        return;

    if ((Poco::Timestamp().epochMicroseconds() - last_clean_timestamp_usec) > clean_interval_usec)
    {
        try
        {
            cleanup();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        last_clean_timestamp_usec = Poco::Timestamp().epochMicroseconds();
    }
}

void StorageWindowView::threadFuncFireProc()
{
    if (shutdown_called)
        return;

    std::unique_lock lock(fire_signal_mutex);
    UInt32 timestamp_now = std::time(nullptr);

    while (next_fire_signal <= timestamp_now)
    {
        try
        {
            if (max_watermark >= timestamp_now)
                fire(next_fire_signal);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        max_fired_watermark = next_fire_signal;
        auto slide_interval = addTime(0, slide_kind, slide_num_units, *time_zone);
        /// Convert DayNum into seconds when the slide interval is larger than Day
        if (slide_kind > IntervalKind::Day)
            slide_interval *= 86400;
        next_fire_signal += slide_interval;
    }

    if (max_watermark >= timestamp_now)
        clean_cache_task->schedule();

    UInt64 timestamp_ms = static_cast<UInt64>(Poco::Timestamp().epochMicroseconds()) / 1000;
    if (!shutdown_called)
        fire_task->scheduleAfter(std::max(UInt64(0), static_cast<UInt64>(next_fire_signal) * 1000 - timestamp_ms));
}

void StorageWindowView::threadFuncFireEvent()
{
    std::unique_lock lock(fire_signal_mutex);

    LOG_TRACE(log, "Fire events: {}", fire_signal.size());

    while (!shutdown_called && !fire_signal.empty())
    {
        try
        {
            fire(fire_signal.front());
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        max_fired_watermark = fire_signal.front();
        fire_signal.pop_front();
    }

    clean_cache_task->schedule();
}

void StorageWindowView::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    if (target_table_id.empty())
        return;

    auto storage = getTargetTable();
    auto lock = storage->lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);
    auto target_metadata_snapshot = storage->getInMemoryMetadataPtr();
    auto target_storage_snapshot = storage->getStorageSnapshot(target_metadata_snapshot, local_context);

    if (query_info.order_optimizer)
        query_info.input_order_info = query_info.order_optimizer->getInputOrder(target_metadata_snapshot, local_context);

    storage->read(query_plan, column_names, target_storage_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);

    if (query_plan.isInitialized())
    {
        auto wv_header = getHeaderForProcessingStage(column_names, storage_snapshot, query_info, local_context, processed_stage);
        auto target_header = query_plan.getCurrentDataStream().header;

        if (!blocksHaveEqualStructure(wv_header, target_header))
        {
            auto converting_actions = ActionsDAG::makeConvertingActions(
                target_header.getColumnsWithTypeAndName(), wv_header.getColumnsWithTypeAndName(), ActionsDAG::MatchColumnsMode::Name);
            auto converting_step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), converting_actions);
            converting_step->setStepDescription("Convert Target table structure to WindowView structure");
            query_plan.addStep(std::move(converting_step));
        }

        query_plan.addStorageHolder(storage);
        query_plan.addTableLock(std::move(lock));
    }
}

Pipe StorageWindowView::watch(
    const Names & /*column_names*/,
    const SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum & processed_stage,
    size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    ASTWatchQuery & query = typeid_cast<ASTWatchQuery &>(*query_info.query);

    bool has_limit = false;
    UInt64 limit = 0;
    if (query.limit_length)
    {
        has_limit = true;
        limit = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*query.limit_length).value);
    }

    auto reader = std::make_shared<WindowViewSource>(
        std::static_pointer_cast<StorageWindowView>(shared_from_this()),
        query.is_watch_events,
        window_view_timezone,
        has_limit,
        limit,
        local_context->getSettingsRef().window_view_heartbeat_interval.totalSeconds());

    std::lock_guard lock(fire_signal_mutex);
    watch_streams.push_back(reader);
    processed_stage = QueryProcessingStage::Complete;

    return Pipe(reader);
}

StorageWindowView::StorageWindowView(
    const StorageID & table_id_,
    ContextPtr context_,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_,
    bool attach_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , log(&Poco::Logger::get(fmt::format("StorageWindowView({}.{})", table_id_.database_name, table_id_.table_name)))
    , fire_signal_timeout_s(context_->getSettingsRef().wait_for_window_view_fire_signal_timeout.totalSeconds())
    , clean_interval_usec(context_->getSettingsRef().window_view_clean_interval.totalMicroseconds())
{
    if (!query.select)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "SELECT query is not specified for {}", getName());

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);

    /// If the target table is not set, use inner target table
    has_inner_target_table = query.to_table_id.empty();
    if (has_inner_target_table && !query.storage)
        throw Exception(
            "You must specify where to save results of a WindowView query: either ENGINE or an existing table in a TO clause",
            ErrorCodes::INCORRECT_QUERY);

    if (query.select->list_of_selects->children.size() != 1)
        throw Exception(
            ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW,
            "UNION is not supported for {}", getName());

    /// Extract information about watermark, lateness.
    eventTimeParser(query);

    auto inner_query = initInnerQuery(query.select->list_of_selects->children.at(0)->as<ASTSelectQuery &>(), context_);

    if (query.inner_storage)
        inner_table_engine = query.inner_storage->clone();
    inner_table_id = StorageID(getStorageID().database_name, generateInnerTableName(getStorageID()));
    inner_fetch_query = generateInnerFetchQuery(inner_table_id);

    target_table_id = has_inner_target_table ? StorageID(table_id_.database_name, generateTargetTableName(table_id_)) : query.to_table_id;

    if (is_proctime)
        next_fire_signal = getWindowUpperBound(std::time(nullptr));

    std::exchange(has_inner_table, true);
    if (!attach_)
    {
        auto inner_create_query = getInnerTableCreateQuery(inner_query, inner_table_id);
        auto create_context = Context::createCopy(context_);
        InterpreterCreateQuery create_interpreter(inner_create_query, create_context);
        create_interpreter.setInternal(true);
        create_interpreter.execute();
        if (has_inner_target_table)
        {
            /// create inner target table
            auto create_context = Context::createCopy(context_);
            auto target_create_query = std::make_shared<ASTCreateQuery>();
            target_create_query->setDatabase(table_id_.database_name);
            target_create_query->setTable(generateTargetTableName(table_id_));

            auto new_columns_list = std::make_shared<ASTColumns>();
            new_columns_list->set(new_columns_list->columns, query.columns_list->columns->ptr());

            target_create_query->set(target_create_query->columns_list, new_columns_list);
            target_create_query->set(target_create_query->storage, query.storage->ptr());

            InterpreterCreateQuery create_interpreter(target_create_query, create_context);
            create_interpreter.setInternal(true);
            create_interpreter.execute();
        }
    }

    clean_cache_task = getContext()->getSchedulePool().createTask(getStorageID().getFullTableName(), [this] { threadFuncCleanup(); });
    fire_task = getContext()->getSchedulePool().createTask(
        getStorageID().getFullTableName(), [this] { is_proctime ? threadFuncFireProc() : threadFuncFireEvent(); });
    clean_cache_task->deactivate();
    fire_task->deactivate();
}

ASTPtr StorageWindowView::initInnerQuery(ASTSelectQuery query, ContextPtr context_)
{
    select_query = query.clone();
    input_header.clear();
    output_header.clear();

    String select_database_name = getContext()->getCurrentDatabase();
    String select_table_name;
    auto select_query_tmp = query.clone();
    extractDependentTable(context_, select_query_tmp, select_database_name, select_table_name);

    /// If the table is not specified - use the table `system.one`
    if (select_table_name.empty())
    {
        select_database_name = "system";
        select_table_name = "one";
    }
    select_table_id = StorageID(select_database_name, select_table_name);

    /// Extract all info from query; substitute Function_tumble and Function_hop with Function_windowID.
    auto inner_query = innerQueryParser(query);

    /// Parse mergeable query
    mergeable_query = inner_query->clone();
    ReplaceFunctionNowData func_now_data;
    ReplaceFunctionNowVisitor(func_now_data).visit(mergeable_query);
    is_time_column_func_now = func_now_data.is_time_column_func_now;
    if (!is_proctime && is_time_column_func_now)
        throw Exception("now() is not supported for Event time processing.", ErrorCodes::INCORRECT_QUERY);
    if (is_time_column_func_now)
        window_id_name = func_now_data.window_id_name;

    window_column_name = std::regex_replace(window_id_name, std::regex("windowID"), is_tumble ? "tumble" : "hop");

    /// Parse final query (same as mergeable query but has tumble/hop instead of windowID)
    final_query = mergeable_query->clone();
    ReplaceWindowIdMatcher::Data final_query_data;
    final_query_data.window_name = is_tumble ? "tumble" : "hop";
    ReplaceWindowIdMatcher::Visitor(final_query_data).visit(final_query);

    return inner_query;
}

ASTPtr StorageWindowView::innerQueryParser(const ASTSelectQuery & query)
{
    if (!query.groupBy())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "GROUP BY query is required for {}", getName());

    // Parse stage mergeable
    ASTPtr result = query.clone();

    WindowFunctionMatcher::Data query_info_data;
    query_info_data.check_duplicate_window = true;
    WindowFunctionMatcher::Visitor(query_info_data).visit(result);

    if (!query_info_data.is_tumble && !query_info_data.is_hop)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
                        "TIME WINDOW FUNCTION is not specified for {}", getName());

    is_tumble = query_info_data.is_tumble;

    // Parse time window function
    ASTFunction & window_function = typeid_cast<ASTFunction &>(*query_info_data.window_function);
    const auto & arguments = window_function.arguments->children;
    extractWindowArgument(
        arguments.at(1), window_kind, window_num_units,
        "Illegal type of second argument of function " + window_function.name + " should be Interval");

    window_id_alias = window_function.alias;
    if (auto * node = arguments[0]->as<ASTIdentifier>())
        timestamp_column_name = node->shortName();

    DropTableIdentifierMatcher::Data drop_identifier_data;
    DropTableIdentifierMatcher::Visitor(drop_identifier_data).visit(query_info_data.window_function);
    window_id_name = window_function.getColumnName();

    slide_kind = window_kind;
    slide_num_units = window_num_units;

    if (!is_tumble)
    {
        hop_kind = window_kind;
        hop_num_units = window_num_units;
        extractWindowArgument(
            arguments.at(2), window_kind, window_num_units,
            "Illegal type of third argument of function " + window_function.name + " should be Interval");
        slice_num_units = std::gcd(hop_num_units, window_num_units);
    }

    // Parse time zone
    size_t time_zone_arg_num = is_tumble ? 2 : 3;
    if (arguments.size() > time_zone_arg_num)
    {
        const auto & ast = arguments.at(time_zone_arg_num);
        const auto * time_zone_ast = ast->as<ASTLiteral>();
        if (!time_zone_ast || time_zone_ast->value.getType() != Field::Types::String)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column #{} of time zone argument of function, must be constant string",
                time_zone_arg_num);
        window_view_timezone = time_zone_ast->value.safeGet<String>();
        time_zone = &DateLUT::instance(window_view_timezone);
    }
    else
        time_zone = &DateLUT::instance();

    return result;
}

void StorageWindowView::eventTimeParser(const ASTCreateQuery & query)
{
    watermark_num_units = 0;
    lateness_num_units = 0;
    is_watermark_strictly_ascending = query.is_watermark_strictly_ascending;
    is_watermark_ascending = query.is_watermark_ascending;
    is_watermark_bounded = query.is_watermark_bounded;

    if (query.is_watermark_strictly_ascending || query.is_watermark_ascending || query.is_watermark_bounded)
    {
        is_proctime = false;

        if (query.is_watermark_ascending)
        {
            is_watermark_bounded = true;
            watermark_kind = IntervalKind::Second;
            watermark_num_units = 1;
        }
        else if (query.is_watermark_bounded)
        {
            extractWindowArgument(
                query.watermark_function, watermark_kind, watermark_num_units,
                "Illegal type WATERMARK function should be Interval");
        }
    }
    else
        is_proctime = true;

    if (query.allowed_lateness)
    {
        allowed_lateness = true;
        extractWindowArgument(
            query.lateness_function, lateness_kind, lateness_num_units,
            "Illegal type ALLOWED_LATENESS function should be Interval");
    }
    else
        allowed_lateness = false;
}

void StorageWindowView::writeIntoWindowView(
    StorageWindowView & window_view, const Block & block, ContextPtr local_context)
{
    while (window_view.modifying_query)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    if (!window_view.is_proctime && window_view.max_watermark == 0 && block.rows() > 0)
    {
        std::lock_guard lock(window_view.fire_signal_mutex);
        const auto & window_column = block.getByName(window_view.timestamp_column_name);
        const ColumnUInt32::Container & window_end_data = static_cast<const ColumnUInt32 &>(*window_column.column).getData();
        UInt32 first_record_timestamp = window_end_data[0];
        window_view.max_watermark = window_view.getWindowUpperBound(first_record_timestamp);
    }

    Pipe pipe(std::make_shared<SourceFromSingleChunk>(block.cloneEmpty(), Chunk(block.getColumns(), block.rows())));

    UInt32 lateness_bound = 0;
    UInt32 t_max_watermark = 0;
    UInt32 t_max_timestamp = 0;
    UInt32 t_max_fired_watermark = 0;
    {
        std::lock_guard lock(window_view.fire_signal_mutex);
        t_max_fired_watermark = window_view.max_fired_watermark;
        t_max_watermark = window_view.max_watermark;
        t_max_timestamp = window_view.max_timestamp;
    }

    // Filter outdated data
    if (window_view.allowed_lateness && t_max_timestamp != 0)
    {
        lateness_bound = addTime(t_max_timestamp, window_view.lateness_kind, -window_view.lateness_num_units, *window_view.time_zone);

        if (window_view.is_watermark_bounded)
        {
            UInt32 watermark_lower_bound
                = addTime(t_max_watermark, window_view.slide_kind, -window_view.slide_num_units, *window_view.time_zone);

            if (watermark_lower_bound < lateness_bound)
                lateness_bound = watermark_lower_bound;
        }
    }
    else if (!window_view.is_time_column_func_now)
    {
        lateness_bound = t_max_fired_watermark;
    }

    if (lateness_bound > 0) /// Add filter, which leaves rows with timestamp >= lateness_bound
    {
        auto filter_function = makeASTFunction(
            "greaterOrEquals",
            std::make_shared<ASTIdentifier>(window_view.timestamp_column_name),
            std::make_shared<ASTLiteral>(lateness_bound));

        ASTPtr query = filter_function;
        NamesAndTypesList columns;
        columns.emplace_back(window_view.timestamp_column_name, std::make_shared<DataTypeDateTime>());

        auto syntax_result = TreeRewriter(local_context).analyze(query, columns);
        auto filter_expression = ExpressionAnalyzer(filter_function, syntax_result, local_context).getActionsDAG(false);

        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<FilterTransform>(
                header, std::make_shared<ExpressionActions>(filter_expression),
                filter_function->getColumnName(), true);
        });
    }

    std::shared_lock<std::shared_mutex> fire_signal_lock;
    QueryPipelineBuilder builder;
    if (window_view.is_proctime)
    {
        fire_signal_lock = std::shared_lock<std::shared_mutex>(window_view.fire_signal_mutex);

        /// Fill ____timestamp column with current time in case of now() time column.
        if (window_view.is_time_column_func_now)
        {
            ColumnWithTypeAndName column;
            column.name = "____timestamp";
            const auto & timezone = window_view.function_now_timezone;
            if (timezone.empty())
                column.type = std::make_shared<DataTypeDateTime>();
            else
                column.type = std::make_shared<DataTypeDateTime>(timezone);
            column.column = column.type->createColumnConst(0, Field(std::time(nullptr)));

            auto adding_column_dag = ActionsDAG::makeAddingColumnActions(std::move(column));
            auto adding_column_actions = std::make_shared<ExpressionActions>(
                std::move(adding_column_dag),
                ExpressionActionsSettings::fromContext(local_context));

            pipe.addSimpleTransform([&](const Block & stream_header)
            {
                return std::make_shared<ExpressionTransform>(stream_header, adding_column_actions);
            });
        }
    }

    Pipes pipes;
    pipes.emplace_back(std::move(pipe));

    auto creator = [&](const StorageID & blocks_id_global)
    {
        auto source_metadata = window_view.getSourceTable()->getInMemoryMetadataPtr();
        auto required_columns = source_metadata->getColumns();
        required_columns.add(ColumnDescription("____timestamp", std::make_shared<DataTypeDateTime>()));
        return StorageBlocks::createStorage(blocks_id_global, required_columns, std::move(pipes), QueryProcessingStage::FetchColumns);
    };
    TemporaryTableHolder blocks_storage(local_context, creator);

    InterpreterSelectQuery select_block(
        window_view.getMergeableQuery(),
        local_context,
        blocks_storage.getTable(),
        blocks_storage.getTable()->getInMemoryMetadataPtr(),
        QueryProcessingStage::WithMergeableState);

    builder = select_block.buildQueryPipeline();
    builder.addSimpleTransform([&](const Block & current_header)
    {
        return std::make_shared<SquashingChunksTransform>(
            current_header,
            local_context->getSettingsRef().min_insert_block_size_rows,
            local_context->getSettingsRef().min_insert_block_size_bytes);
    });

    if (!window_view.is_proctime)
    {
        UInt32 block_max_timestamp = 0;
        if (window_view.is_watermark_bounded || window_view.allowed_lateness)
        {
            const auto & timestamp_column = *block.getByName(window_view.timestamp_column_name).column;
            const auto & timestamp_data = typeid_cast<const ColumnUInt32 &>(timestamp_column).getData();
            for (const auto & timestamp : timestamp_data)
            {
                if (timestamp > block_max_timestamp)
                    block_max_timestamp = timestamp;
            }
        }

        if (block_max_timestamp)
            window_view.updateMaxTimestamp(block_max_timestamp);
    }

    UInt32 lateness_upper_bound = 0;
    if (!window_view.is_proctime && window_view.allowed_lateness && t_max_fired_watermark)
        lateness_upper_bound = t_max_fired_watermark;

    /// On each chunk check window end for each row in a window column, calculating max.
    /// Update max watermark (latest seen window end) if needed.
    /// If lateness is allowed, add lateness signals.
    builder.addSimpleTransform([&](const Block & current_header)
    {
        return std::make_shared<WatermarkTransform>(
            current_header,
            window_view,
            window_view.window_id_name,
            lateness_upper_bound);
    });

    auto inner_table = window_view.getInnerTable();
    auto lock = inner_table->lockForShare(
        local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);
    auto metadata_snapshot = inner_table->getInMemoryMetadataPtr();
    auto output = inner_table->write(window_view.getMergeableQuery(), metadata_snapshot, local_context);
    output->addTableLock(lock);

    if (!blocksHaveEqualStructure(builder.getHeader(), output->getHeader()))
    {
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            builder.getHeader().getColumnsWithTypeAndName(),
            output->getHeader().getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);
        auto convert_actions = std::make_shared<ExpressionActions>(
            convert_actions_dag, ExpressionActionsSettings::fromContext(local_context, CompileExpressions::yes));

        builder.addSimpleTransform([&](const Block & header) { return std::make_shared<ExpressionTransform>(header, convert_actions); });
    }

    builder.addChain(Chain(std::move(output)));
    builder.setSinks([&](const Block & cur_header, Pipe::StreamType)
    {
        return std::make_shared<EmptySink>(cur_header);
    });

    auto executor = builder.execute();
    executor->execute(builder.getNumThreads());
}

void StorageWindowView::startup()
{
    DatabaseCatalog::instance().addDependency(select_table_id, getStorageID());

    fire_task->activate();
    clean_cache_task->activate();

    /// Start the working thread
    if (is_proctime)
        fire_task->schedule();
}

void StorageWindowView::shutdown()
{
    shutdown_called = true;

    fire_condition.notify_all();

    clean_cache_task->deactivate();
    fire_task->deactivate();

    auto table_id = getStorageID();
    DatabaseCatalog::instance().removeDependency(select_table_id, table_id);
}

void StorageWindowView::checkTableCanBeDropped() const
{
    auto table_id = getStorageID();
    Dependencies dependencies = DatabaseCatalog::instance().getDependencies(table_id);
    if (!dependencies.empty())
    {
        StorageID dependent_table_id = dependencies.front();
        throw Exception("Table has dependency " + dependent_table_id.getNameForLogs(), ErrorCodes::TABLE_WAS_NOT_DROPPED);
    }
}

void StorageWindowView::drop()
{
    /// Must be guaranteed at this point for database engine Atomic that has_inner_table == false,
    /// because otherwise will be a deadlock.
    dropInnerTableIfAny(true, getContext());
}

void StorageWindowView::dropInnerTableIfAny(bool no_delay, ContextPtr local_context)
{
    if (!std::exchange(has_inner_table, false))
        return;

    try
    {
        InterpreterDropQuery::executeDropQuery(
            ASTDropQuery::Kind::Drop, getContext(), local_context, inner_table_id, no_delay);

        if (has_inner_target_table)
            InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, getContext(), local_context, target_table_id, no_delay);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

const Block & StorageWindowView::getInputHeader() const
{
    std::lock_guard lock(sample_block_lock);
    if (!input_header)
    {
        auto metadata = getSourceTable()->getInMemoryMetadataPtr();
        input_header = metadata->getSampleBlockNonMaterialized();
    }
    return input_header;
}

const Block & StorageWindowView::getOutputHeader() const
{
    std::lock_guard lock(sample_block_lock);
    if (!output_header)
    {
        output_header = InterpreterSelectQuery(select_query->clone(), getContext(), SelectQueryOptions(QueryProcessingStage::Complete))
                           .getSampleBlock();
    }
    return output_header;
}

StoragePtr StorageWindowView::getSourceTable() const
{
    return DatabaseCatalog::instance().getTable(select_table_id, getContext());
}

StoragePtr StorageWindowView::getInnerTable() const
{
    return DatabaseCatalog::instance().getTable(inner_table_id, getContext());
}

StoragePtr StorageWindowView::getTargetTable() const
{
    return DatabaseCatalog::instance().getTable(target_table_id, getContext());
}

void registerStorageWindowView(StorageFactory & factory)
{
    factory.registerStorage("WindowView", [](const StorageFactory::Arguments & args)
    {
        if (!args.attach && !args.getLocalContext()->getSettingsRef().allow_experimental_window_view)
            throw Exception(
                "Experimental WINDOW VIEW feature is not enabled (the setting 'allow_experimental_window_view')",
                ErrorCodes::SUPPORT_IS_DISABLED);

        return std::make_shared<StorageWindowView>(args.table_id, args.getLocalContext(), args.query, args.columns, args.attach);
    });
}

}
