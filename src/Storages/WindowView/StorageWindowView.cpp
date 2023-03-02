#include <numeric>
#include <regex>

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsTimeWindow.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/QueryNormalizer.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
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
#include <Processors/Transforms/ReplacingWindowColumnTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Sinks/EmptySink.h>
#include <Storages/StorageFactory.h>
#include <Common/typeid_cast.h>
#include <base/sleep.h>
#include <base/logger_useful.h>

#include <Storages/LiveView/StorageBlocks.h>

#include <Storages/WindowView/StorageWindowView.h>
#include <Storages/WindowView/WindowViewSource.h>

#include <QueryPipeline/printPipeline.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TABLE_WAS_NOT_DROPPED;
}

namespace
{
    /// Fetch all window info and replace tumble or hop node names with windowID
    struct FetchQueryInfoMatcher
    {
        using Visitor = InDepthNodeVisitor<FetchQueryInfoMatcher, true>;
        using TypeToVisit = ASTFunction;

        struct Data
        {
            ASTPtr window_function;
            String window_id_name;
            String window_id_alias;
            String serialized_window_function;
            String timestamp_column_name;
            bool is_tumble = false;
            bool is_hop = false;
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
                        data.serialized_window_function = serializeAST(*temp_node);
                        t->name = "windowID";
                        data.window_id_name = t->getColumnName();
                        data.window_id_alias = t->alias;
                        data.window_function = t->clone();
                        data.window_function->setAlias("");
                        data.timestamp_column_name = t->arguments->children[0]->getColumnName();
                    }
                    else
                    {
                        if (serializeAST(*temp_node) != data.serialized_window_function)
                            throw Exception("WINDOW VIEW only support ONE TIME WINDOW FUNCTION", ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW);
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

    struct ReplaceFunctionWindowMatcher
    {
        using Visitor = InDepthNodeVisitor<ReplaceFunctionWindowMatcher, true>;

        struct Data{};

        static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

        static void visit(ASTPtr & ast, Data &)
        {
            if (auto * t = ast->as<ASTFunction>())
            {
                if (t->name == "hop" || t->name == "tumble")
                    t->name = "windowID";
            }
        }
    };

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
                ast = std::make_shared<ASTIdentifier>(t->shortName());
            }
        }
    };

    IntervalKind strToIntervalKind(const String& interval_str)
    {
        if (interval_str == "Second")
            return IntervalKind::Second;
        else if (interval_str == "Minute")
            return IntervalKind::Minute;
        else if (interval_str == "Hour")
            return IntervalKind::Hour;
        else if (interval_str == "Day")
            return IntervalKind::Day;
        else if (interval_str == "Week")
            return IntervalKind::Week;
        else if (interval_str == "Month")
            return IntervalKind::Month;
        else if (interval_str == "Quarter")
            return IntervalKind::Quarter;
        else if (interval_str == "Year")
            return IntervalKind::Year;
        __builtin_unreachable();
    }

    void extractWindowArgument(const ASTPtr & ast, IntervalKind::Kind & kind, Int64 & num_units, String err_msg)
    {
        const auto * arg = ast->as<ASTFunction>();
        if (!arg || !startsWith(arg->name, "toInterval"))
            throw Exception(err_msg, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        kind = strToIntervalKind(arg->name.substr(10));
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

static size_t getWindowIDColumnPosition(const Block & header)
{
    auto position = -1;
    for (const auto & column : header.getColumnsWithTypeAndName())
    {
        if (startsWith(column.name, "windowID"))
        {
            position = header.getPositionByName(column.name);
            break;
        }
    }
    if (position < 0)
        throw Exception("Not found column windowID", ErrorCodes::LOGICAL_ERROR);
    return position;
}

UInt32 StorageWindowView::getCleanupBound()
{
    UInt32 w_bound;
    {
        std::lock_guard lock(fire_signal_mutex);
        w_bound = max_fired_watermark;
        if (w_bound == 0)
            return 0;

        if (!is_proctime)
        {
            if (max_watermark == 0)
                return 0;
            if (allowed_lateness)
            {
                UInt32 lateness_bound = addTime(
                    max_timestamp, lateness_kind, -1 * lateness_num_units, *time_zone);
                lateness_bound = getWindowLowerBound(lateness_bound);
                if (lateness_bound < w_bound)
                    w_bound = lateness_bound;
            }
        }
    }
    return w_bound;
}

ASTPtr StorageWindowView::getCleanupQuery()
{
    ASTPtr function_equal;
    function_equal = makeASTFunction(
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
    alter_command->predicate = function_equal;
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
    auto storage_ptr = getInnerStorage();
    auto metadata_snapshot = storage_ptr->getInMemoryMetadataPtr();
    return getInnerStorage()->optimize(query, metadata_snapshot, partition, final, deduplicate, deduplicate_by_columns, local_context);
}

std::pair<BlocksPtr, Block> StorageWindowView::getNewBlocks(UInt32 watermark)
{
    UInt32 w_start = addTime(watermark, window_kind, -1 * window_num_units, *time_zone);

    InterpreterSelectQuery fetch(
        getFetchColumnQuery(w_start, watermark),
        window_view_context,
        getInnerStorage(),
        nullptr,
        SelectQueryOptions(QueryProcessingStage::FetchColumns));

    auto builder = fetch.buildQueryPipeline();

    DataTypes types{std::make_shared<DataTypeDateTime>(), std::make_shared<DataTypeDateTime>()};
    auto window_column_type = std::make_shared<DataTypeTuple>(types);

    {
        ColumnWithTypeAndName window_column{window_column_type->createColumn(), window_column_type, window_column_name};
        auto adding_column_dag = ActionsDAG::makeAddingColumnActions(std::move(window_column));
        auto adding_column_actions = std::make_shared<ExpressionActions>(
            std::move(adding_column_dag),
            ExpressionActionsSettings::fromContext(getContext()));
        builder.addSimpleTransform([&](const Block & stream_header)
        {
            return std::make_shared<ExpressionTransform>(stream_header, adding_column_actions);
        });
    }

    builder.addSimpleTransform([&](const Block & current_header)
    {
        Tuple window_value{w_start, watermark};
        auto window_column_name_and_type = NameAndTypePair(window_column_name, window_column_type);
        return std::make_shared<ReplacingWindowColumnTransform>(
            current_header, getWindowIDColumnPosition(current_header), window_column_name_and_type, window_value);
    });

    Pipes pipes;
    auto pipe = QueryPipelineBuilder::getPipe(std::move(builder));
    pipes.emplace_back(std::move(pipe));

    auto creator = [&](const StorageID & blocks_id_global)
    {
        auto parent_table_metadata = getParentStorage()->getInMemoryMetadataPtr();
        auto required_columns = parent_table_metadata->getColumns();
        required_columns.add(ColumnDescription("____timestamp", std::make_shared<DataTypeDateTime>()));
        return StorageBlocks::createStorage(blocks_id_global, required_columns, std::move(pipes), QueryProcessingStage::WithMergeableState);
    };

    TemporaryTableHolder blocks_storage(window_view_context, creator);

    InterpreterSelectQuery select(
        getFinalQuery(),
        window_view_context,
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
    {
        std::lock_guard lock(mutex);
        std::tie(blocks, header) = getNewBlocks(watermark);
    }

    for (const auto & block : *blocks)
    {
        for (auto & watch_stream : watch_streams)
        {
            if (auto watch_stream_ptr = watch_stream.lock())
                watch_stream_ptr->addBlock(block, watermark);
        }
    }
    if (!target_table_id.empty())
    {
        StoragePtr target_table = getTargetStorage();
        auto insert = std::make_shared<ASTInsertQuery>();
        insert->table_id = target_table->getStorageID();
        InterpreterInsertQuery interpreter(insert, getContext());
        auto block_io = interpreter.execute();

        auto pipe = Pipe(std::make_shared<BlocksSource>(blocks, header));
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

std::shared_ptr<ASTCreateQuery> StorageWindowView::getInnerTableCreateQuery(
    const ASTPtr & inner_query, ASTStorage * storage, const String & database_name, const String & table_name)
{
    /// We will create a query to create an internal table.
    auto inner_create_query = std::make_shared<ASTCreateQuery>();
    inner_create_query->setDatabase(database_name);
    inner_create_query->setTable(table_name);

    Aliases aliases;
    QueryAliasesVisitor(aliases).visit(inner_query);
    auto inner_query_normalized = inner_query->clone();
    QueryNormalizer::Data normalizer_data(aliases, {}, false, getContext()->getSettingsRef(), false);
    QueryNormalizer(normalizer_data).visit(inner_query_normalized);

    auto inner_select_query = std::static_pointer_cast<ASTSelectQuery>(inner_query_normalized);

    auto t_sample_block
        = InterpreterSelectQuery(
            inner_select_query, window_view_context, getParentStorage(), nullptr,
            SelectQueryOptions(QueryProcessingStage::WithMergeableState)) .getSampleBlock();

    auto columns_list = std::make_shared<ASTExpressionList>();

    String window_id_column_name;
    if (is_time_column_func_now)
    {
        auto column_window = std::make_shared<ASTColumnDeclaration>();
        column_window->name = window_id_name;
        column_window->type = std::make_shared<ASTIdentifier>("UInt32");
        columns_list->children.push_back(column_window);
        window_id_column_name = window_id_name;
    }

    for (const auto & column : t_sample_block.getColumnsWithTypeAndName())
    {
        ParserIdentifierWithOptionalParameters parser;
        String sql = column.type->getName();
        ASTPtr ast = parseQuery(parser, sql.data(), sql.data() + sql.size(), "data type", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        auto column_dec = std::make_shared<ASTColumnDeclaration>();
        column_dec->name = column.name;
        column_dec->type = ast;
        columns_list->children.push_back(column_dec);
        if (!is_time_column_func_now && window_id_column_name.empty() && startsWith(column.name, "windowID"))
        {
            window_id_column_name = column.name;
        }
    }

    if (window_id_column_name.empty())
        throw Exception(
            "The first argument of time window function should not be a constant value.",
            ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW);

    ToIdentifierMatcher::Data query_data;
    query_data.window_id_name = window_id_name;
    query_data.window_id_alias = window_id_alias;
    ToIdentifierMatcher::Visitor to_identifier_visitor(query_data);

    ReplaceFunctionNowData time_now_data;
    ReplaceFunctionNowVisitor time_now_visitor(time_now_data);
    ReplaceFunctionWindowMatcher::Data func_hop_data;
    ReplaceFunctionWindowMatcher::Visitor func_window_visitor(func_hop_data);

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
        func_window_visitor.visit(node);
        to_identifier_visitor.visit(node);
        node->setAlias("");
        return node;
    };

    auto new_storage = std::make_shared<ASTStorage>();
    /// storage != nullptr in case create window view with ENGINE syntax
    if (storage)
    {
        new_storage->set(new_storage->engine, storage->engine->clone());

        if (storage->ttl_table)
            throw Exception(
                ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW,
                "TTL is not supported for inner table in Window View");

        if (!endsWith(storage->engine->name, "MergeTree"))
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "The ENGINE of WindowView must be MergeTree family of table engines "
                "including the engines with replication support");

        if (storage->partition_by)
            new_storage->set(new_storage->partition_by, visit(storage->partition_by));
        if (storage->primary_key)
            new_storage->set(new_storage->primary_key, visit(storage->primary_key));
        if (storage->order_by)
            new_storage->set(new_storage->order_by, visit(storage->order_by));
        if (storage->sample_by)
            new_storage->set(new_storage->sample_by, visit(storage->sample_by));

        if (storage->settings)
            new_storage->set(new_storage->settings, storage->settings->clone());
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
    IntervalKind window_interval_kind;
    if (is_tumble)
        window_interval_kind = window_kind;
    else
        window_interval_kind = hop_kind;

    switch (window_interval_kind)
    {
#define CASE_WINDOW_KIND(KIND) \
    case IntervalKind::KIND: \
    { \
        if (is_tumble) \
            return ToStartOfTransform<IntervalKind::KIND>::execute(time_sec, window_num_units, *time_zone); \
        else \
        {\
            UInt32 w_start = ToStartOfTransform<IntervalKind::KIND>::execute(time_sec, hop_num_units, *time_zone); \
            UInt32 w_end = AddTime<IntervalKind::KIND>::execute(w_start, hop_num_units, *time_zone);\
            return AddTime<IntervalKind::KIND>::execute(w_end, -1 * window_num_units, *time_zone);\
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
    IntervalKind window_interval_kind;
    if (is_tumble)
        window_interval_kind = window_kind;
    else
        window_interval_kind = hop_kind;

    switch (window_interval_kind)
    {
#define CASE_WINDOW_KIND(KIND) \
    case IntervalKind::KIND: \
    { \
        if (is_tumble) \
        {\
            UInt32 w_start = ToStartOfTransform<IntervalKind::KIND>::execute(time_sec, window_num_units, *time_zone); \
            return AddTime<IntervalKind::KIND>::execute(w_start, window_num_units, *time_zone); \
        }\
        else \
        {\
            UInt32 w_start = ToStartOfTransform<IntervalKind::KIND>::execute(time_sec, hop_num_units, *time_zone); \
            return AddTime<IntervalKind::KIND>::execute(w_start, hop_num_units, *time_zone); \
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

void StorageWindowView::addFireSignal(std::set<UInt32> & signals)
{
    std::lock_guard lock(fire_signal_mutex);
    for (const auto & signal : signals)
        fire_signal.push_back(signal);
    fire_signal_condition.notify_all();
}

void StorageWindowView::updateMaxTimestamp(UInt32 timestamp)
{
    std::lock_guard lock(fire_signal_mutex);
    if (timestamp > max_timestamp)
        max_timestamp = timestamp;
}

void StorageWindowView::updateMaxWatermark(UInt32 watermark)
{
    std::lock_guard lock(fire_signal_mutex);
    if (max_watermark == 0)
    {
        max_watermark = getWindowUpperBound(watermark - 1);
        return;
    }

    bool updated;
    if (is_watermark_strictly_ascending)
    {
        updated = max_watermark < watermark;
        while (max_watermark < watermark)
        {
            fire_signal.push_back(max_watermark);
            max_fired_watermark = max_watermark;
            max_watermark
                = is_tumble
                ? addTime(max_watermark, window_kind, window_num_units, *time_zone)
                : addTime(max_watermark, hop_kind, hop_num_units, *time_zone);
        }
    }
    else // strictly || bounded
    {
        UInt32 max_watermark_bias = addTime(max_watermark, watermark_kind, watermark_num_units, *time_zone);
        updated = max_watermark_bias <= watermark;
        while (max_watermark_bias <= max_timestamp)
        {
            fire_signal.push_back(max_watermark);
            max_fired_watermark = max_watermark;
            if (is_tumble)
            {
                max_watermark = addTime(max_watermark, window_kind, window_num_units, *time_zone);
                max_watermark_bias = addTime(max_watermark, window_kind, window_num_units, *time_zone);
            }
            else
            {
                max_watermark = addTime(max_watermark, hop_kind, hop_num_units, *time_zone);
                max_watermark_bias = addTime(max_watermark, hop_kind, hop_num_units, *time_zone);
            }
        }
    }

    if (updated)
        fire_signal_condition.notify_all();
}

inline void StorageWindowView::cleanup()
{
    InterpreterAlterQuery alter_query(getCleanupQuery(), window_view_context);
    alter_query.execute();

    std::lock_guard lock(fire_signal_mutex);
    watch_streams.remove_if([](std::weak_ptr<WindowViewSource> & ptr) { return ptr.expired(); });
}

void StorageWindowView::threadFuncCleanup()
{
    try
    {
        cleanup();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    if (!shutdown_called)
        clean_cache_task->scheduleAfter(clean_interval_ms);
}

void StorageWindowView::threadFuncFireProc()
{
    std::unique_lock lock(fire_signal_mutex);
    UInt32 timestamp_now = std::time(nullptr);

    while (next_fire_signal <= timestamp_now)
    {
        try
        {
            fire(next_fire_signal);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        max_fired_watermark = next_fire_signal;
        next_fire_signal = addTime(next_fire_signal, window_kind, window_num_units, *time_zone);
    }

    UInt64 timestamp_ms = static_cast<UInt64>(Poco::Timestamp().epochMicroseconds()) / 1000;
    if (!shutdown_called)
        fire_task->scheduleAfter(std::max(UInt64(0), static_cast<UInt64>(next_fire_signal) * 1000 - timestamp_ms));
}

void StorageWindowView::threadFuncFireEvent()
{
    std::unique_lock lock(fire_signal_mutex);
    while (!shutdown_called)
    {
        bool signaled = std::cv_status::no_timeout == fire_signal_condition.wait_for(lock, std::chrono::seconds(5));
        if (!signaled)
            continue;

        LOG_TRACE(log, "Fire events: {}", fire_signal.size());

        while (!fire_signal.empty())
        {
            fire(fire_signal.front());
            fire_signal.pop_front();
        }
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
{
    window_view_context = Context::createCopy(getContext());
    window_view_context->makeQueryContext();

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);

    if (!query.select)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "SELECT query is not specified for {}", getName());

    if (query.select->list_of_selects->children.size() != 1)
        throw Exception(
            ErrorCodes::QUERY_IS_NOT_SUPPORTED_IN_WINDOW_VIEW,
            "UNION is not supported for {}", getName());

    select_query = query.select->list_of_selects->children.at(0)->clone();
    String select_database_name = getContext()->getCurrentDatabase();
    String select_table_name;
    auto select_query_tmp = select_query->clone();
    extractDependentTable(getContext(), select_query_tmp, select_database_name, select_table_name);

    /// If the table is not specified - use the table `system.one`
    if (select_table_name.empty())
    {
        select_database_name = "system";
        select_table_name = "one";
    }
    select_table_id = StorageID(select_database_name, select_table_name);
    DatabaseCatalog::instance().addDependency(select_table_id, table_id_);

    /// Extract all info from query; substitute Function_tumble and Function_hop with Function_windowID.
    auto inner_query = innerQueryParser(select_query->as<ASTSelectQuery &>());

    // Parse mergeable query
    mergeable_query = inner_query->clone();
    ReplaceFunctionNowData func_now_data;
    ReplaceFunctionNowVisitor(func_now_data).visit(mergeable_query);
    is_time_column_func_now = func_now_data.is_time_column_func_now;
    if (is_time_column_func_now)
        window_id_name = func_now_data.window_id_name;

    // Parse final query (same as mergeable query but has tumble/hop instead of windowID)
    final_query = mergeable_query->clone();
    ReplaceWindowIdMatcher::Data final_query_data;
    if (is_tumble)
        final_query_data.window_name = "tumble";
    else
        final_query_data.window_name = "hop";
    ReplaceWindowIdMatcher::Visitor(final_query_data).visit(final_query);

    is_watermark_strictly_ascending = query.is_watermark_strictly_ascending;
    is_watermark_ascending = query.is_watermark_ascending;
    is_watermark_bounded = query.is_watermark_bounded;
    target_table_id = query.to_table_id;

    /// Extract information about watermark, lateness.
    eventTimeParser(query);

    if (is_tumble)
        window_column_name = std::regex_replace(window_id_name, std::regex("windowID"), "tumble");
    else
        window_column_name = std::regex_replace(window_id_name, std::regex("windowID"), "hop");

    auto generate_inner_table_name = [](const StorageID & storage_id)
    {
        if (storage_id.hasUUID())
            return ".inner." + toString(storage_id.uuid);
        return ".inner." + storage_id.table_name;
    };

    if (attach_)
    {
        inner_table_id = StorageID(table_id_.database_name, generate_inner_table_name(table_id_));
    }
    else
    {
        auto inner_create_query
            = getInnerTableCreateQuery(inner_query, query.storage, table_id_.database_name, generate_inner_table_name(table_id_));

        auto create_context = Context::createCopy(context_);
        InterpreterCreateQuery create_interpreter(inner_create_query, create_context);
        create_interpreter.setInternal(true);
        create_interpreter.execute();
        inner_storage = DatabaseCatalog::instance().getTable(StorageID(inner_create_query->getDatabase(), inner_create_query->getTable()), getContext());
        inner_table_id = inner_storage->getStorageID();
    }

    clean_interval_ms = getContext()->getSettingsRef().window_view_clean_interval.totalMilliseconds();
    next_fire_signal = getWindowUpperBound(std::time(nullptr));

    clean_cache_task = window_view_context->getSchedulePool().createTask(getStorageID().getFullTableName(), [this] { threadFuncCleanup(); });
    if (is_proctime)
        fire_task = window_view_context->getSchedulePool().createTask(getStorageID().getFullTableName(), [this] { threadFuncFireProc(); });
    else
        fire_task = window_view_context->getSchedulePool().createTask(getStorageID().getFullTableName(), [this] { threadFuncFireEvent(); });
    clean_cache_task->deactivate();
    fire_task->deactivate();
}


ASTPtr StorageWindowView::innerQueryParser(const ASTSelectQuery & query)
{
    if (!query.groupBy())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "GROUP BY query is required for {}", getName());

    // Parse stage mergeable
    ASTPtr result = query.clone();
    FetchQueryInfoMatcher::Data query_info_data;
    FetchQueryInfoMatcher::Visitor(query_info_data).visit(result);

    if (!query_info_data.is_tumble && !query_info_data.is_hop)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
                        "TIME WINDOW FUNCTION is not specified for {}", getName());

    window_id_name = query_info_data.window_id_name;
    window_id_alias = query_info_data.window_id_alias;
    timestamp_column_name = query_info_data.timestamp_column_name;
    is_tumble = query_info_data.is_tumble;

    // Parse time window function
    ASTFunction & window_function = typeid_cast<ASTFunction &>(*query_info_data.window_function);
    const auto & arguments = window_function.arguments->children;
    extractWindowArgument(
        arguments.at(1), window_kind, window_num_units,
        "Illegal type of second argument of function " + window_function.name + " should be Interval");

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
    if (query.is_watermark_strictly_ascending || query.is_watermark_ascending || query.is_watermark_bounded)
    {
        is_proctime = false;

        if (is_time_column_func_now)
            throw Exception("now() is not supported for Event time processing.", ErrorCodes::INCORRECT_QUERY);

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

    if (query.allowed_lateness)
    {
        allowed_lateness = true;
        extractWindowArgument(
            query.lateness_function, lateness_kind, lateness_num_units,
            "Illegal type ALLOWED_LATENESS function should be Interval");
    }
}

void StorageWindowView::writeIntoWindowView(
    StorageWindowView & window_view, const Block & block, ContextPtr local_context)
{
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
        lateness_bound = addTime(
            t_max_timestamp, window_view.lateness_kind,
            -1 * window_view.lateness_num_units, *window_view.time_zone);

        if (window_view.is_watermark_bounded)
        {
            UInt32 watermark_lower_bound = window_view.is_tumble
                ? addTime(t_max_watermark, window_view.window_kind,
                          -1 * window_view.window_num_units, *window_view.time_zone)
                : addTime(t_max_watermark, window_view.hop_kind,
                          -1 * window_view.hop_num_units, *window_view.time_zone);

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
        ASTPtr args = std::make_shared<ASTExpressionList>();
        args->children.push_back(std::make_shared<ASTIdentifier>(window_view.timestamp_column_name));
        args->children.push_back(std::make_shared<ASTLiteral>(lateness_bound));

        auto filter_function = std::make_shared<ASTFunction>();
        filter_function->name = "greaterOrEquals";
        filter_function->arguments = args;
        filter_function->children.push_back(filter_function->arguments);

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
        auto parent_metadata = window_view.getParentStorage()->getInMemoryMetadataPtr();
        auto required_columns = parent_metadata->getColumns();
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

        UInt32 lateness_upper_bound = 0;
        if (window_view.allowed_lateness && t_max_fired_watermark)
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
    }

    auto inner_storage = window_view.getInnerStorage();
    auto lock = inner_storage->lockForShare(
        local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);
    auto metadata_snapshot = inner_storage->getInMemoryMetadataPtr();
    auto output = inner_storage->write(window_view.getMergeableQuery(), metadata_snapshot, local_context);

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
    // Start the working thread
    clean_cache_task->activateAndSchedule();
    fire_task->activateAndSchedule();
}

void StorageWindowView::shutdown()
{
    shutdown_called = true;

    {
        std::lock_guard lock(mutex);
        fire_condition.notify_all();
    }

    clean_cache_task->deactivate();
    fire_task->deactivate();

    auto table_id = getStorageID();
    DatabaseCatalog::instance().removeDependency(select_table_id, table_id);
    inner_storage.reset();
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
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

Block & StorageWindowView::getHeader() const
{
    std::lock_guard lock(sample_block_lock);
    if (!sample_block)
    {
        sample_block = InterpreterSelectQuery(
            select_query->clone(), window_view_context, getParentStorage(), nullptr,
            SelectQueryOptions(QueryProcessingStage::Complete)).getSampleBlock();
        /// convert all columns to full columns
        /// in case some of them are constant
        for (size_t i = 0; i < sample_block.columns(); ++i)
        {
            sample_block.safeGetByPosition(i).column = sample_block.safeGetByPosition(i).column->convertToFullColumnIfConst();
        }
    }
    return sample_block;
}

StoragePtr StorageWindowView::getParentStorage() const
{
    if (!parent_storage)
        parent_storage = DatabaseCatalog::instance().getTable(select_table_id, getContext());
    return parent_storage;
}

StoragePtr StorageWindowView::getInnerStorage() const
{
    if (!inner_storage)
        inner_storage = DatabaseCatalog::instance().getTable(inner_table_id, getContext());
    return inner_storage;
}

ASTPtr StorageWindowView::getFetchColumnQuery(UInt32 w_start, UInt32 w_end) const
{
    auto res_query = std::make_shared<ASTSelectQuery>();
    auto select = std::make_shared<ASTExpressionList>();
    select->children.push_back(std::make_shared<ASTAsterisk>());
    res_query->setExpression(ASTSelectQuery::Expression::SELECT, select);
    res_query->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    auto tables_elem = std::make_shared<ASTTablesInSelectQueryElement>();
    auto table_expr = std::make_shared<ASTTableExpression>();
    res_query->tables()->children.push_back(tables_elem);
    tables_elem->table_expression = table_expr;
    tables_elem->children.push_back(table_expr);
    table_expr->database_and_table_name = std::make_shared<ASTTableIdentifier>(inner_table_id);
    table_expr->children.push_back(table_expr->database_and_table_name);

    if (is_tumble)
    {
        /// SELECT * FROM inner_table PREWHERE window_id_name == w_end
        /// (because we fire at the end of windows)
        auto func_equals = makeASTFunction("equals", std::make_shared<ASTIdentifier>(window_id_name), std::make_shared<ASTLiteral>(w_end));
        res_query->setExpression(ASTSelectQuery::Expression::PREWHERE, func_equals);
    }
    else
    {
        auto func_array = makeASTFunction("array");
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
            w_end = addTime(w_end, window_kind, -1 * slice_num_units, *time_zone);
        }
        auto func_has = makeASTFunction("has", func_array, std::make_shared<ASTIdentifier>(window_id_name));
        res_query->setExpression(ASTSelectQuery::Expression::PREWHERE, func_has);
    }

    return res_query;
}

StoragePtr StorageWindowView::getTargetStorage() const
{
    if (!target_storage && !target_table_id.empty())
        target_storage = DatabaseCatalog::instance().getTable(target_table_id, getContext());
    return target_storage;
}

void registerStorageWindowView(StorageFactory & factory)
{
    factory.registerStorage("WindowView", [](const StorageFactory::Arguments & args)
    {
        if (!args.attach && !args.getLocalContext()->getSettingsRef().allow_experimental_window_view)
            throw Exception(
                "Experimental WINDOW VIEW feature is not enabled (the setting 'allow_experimental_window_view')",
                ErrorCodes::SUPPORT_IS_DISABLED);

        return StorageWindowView::create(args.table_id, args.getLocalContext(), args.query, args.columns, args.attach);
    });
}

}
